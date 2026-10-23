package grpcserver

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	pkgauth "github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Public Session RPC + PostgreSQL lifecycle + real compute. Harness protocol is
// deliberately a separate qualification client; this is NOT public kagent A2A.
func TestPublicSessionNativeCompute(t *testing.T) {
	root := os.Getenv("EXECUTION_KAGENT_NATIVE_ROOT")
	if root == "" {
		t.Skip("explicit isolated native resources required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	databaseCtx, databaseCancel := context.WithCancel(context.WithoutCancel(ctx))
	t.Cleanup(databaseCancel)
	dsn := dbtest.StartT(databaseCtx, t)
	dbtest.MigrateT(t, dsn, false)
	pool, err := database.Connect(ctx, &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
	require.NoError(t, err)
	defer pool.Close()
	store := database.NewClient(pool, "public")
	tlsCred, err := credentials.NewClientTLSFromFile(filepath.Join(root, "trust.pem"), "127.0.0.1")
	require.NoError(t, err)
	ctlConn, err := grpc.NewClient("unix://"+filepath.Join(root, "control-rpc.sock"), grpc.WithTransportCredentials(tlsCred))
	require.NoError(t, err)
	defer ctlConn.Close()
	ctl := atepb.NewControlClient(ctlConn)
	template, err := ctl.GetActorTemplate(ctx, &atepb.GetActorTemplateRequest{ActorTemplate: &atepb.ObjectRef{Atespace: "native", Name: "toolchat"}})
	require.NoError(t, err)
	pair := database.AgentDefinition{Namespace: "native", AgentName: "toolchat", AgentUID: "native-fixture-agent", DesiredRevision: "native-fixture-revision"}
	require.NoError(t, store.UpsertAgentDefinition(ctx, pair))
	require.NoError(t, store.RecordRuntimeRevision(ctx, database.RuntimeRevision{Revision: pair.DesiredRevision, Namespace: pair.Namespace, AgentName: pair.AgentName, AgentUID: pair.AgentUID, SourceSnapshot: []byte("{}"), AgentCard: &a2apb.AgentCard{}, EgressDestinations: []string{}, ActorTemplateAtespace: "native", ActorTemplateName: "toolchat", ActorTemplateUID: template.Metadata.Uid}, database.RecordRevisionOptions{Ready: true}))
	actors, err := substrate.Dial(ctx, substrate.Config{AteAPIEndpoint: "unix://" + filepath.Join(root, "control-rpc.sock"), CAFile: filepath.Join(root, "trust.pem"), EgressPolicies: store, CallTimeout: 30 * time.Second})
	require.NoError(t, err)
	defer actors.Close()
	workflow := sessionsvc.NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second)
	listener, err := net.Listen("unix", filepath.Join(root, "kagent-session.sock"))
	require.NoError(t, err)
	server, err := New(Config{Listener: listener, Authenticator: &authimpl.InsecureAuthenticator{}, SystemService: testSystemService(), SessionService: sessionsvc.NewService(store, &pkgauth.NoopAuthorizer{}, workflow)})
	require.NoError(t, err)
	serverCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Start(serverCtx) }()
	defer func() { stop(); require.NoError(t, <-done) }()
	conn, err := grpc.NewClient("unix://"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := apiv1.NewSessionServiceClient(conn)
	owner := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-user-id", "native-owner"))
	raw, err := os.ReadFile(filepath.Join(root, "requirement.json"))
	require.NoError(t, err)
	created, err := client.CreateSession(owner, &apiv1.CreateSessionRequest{Agent: &apiv1.ResourceReference{Namespace: pair.Namespace, Name: pair.AgentName}, RequestId: "native-compute-session", ExecutionRequirement: raw})
	require.NoError(t, err)
	session := created.Session
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, session.State)
	defer func() {
		clean, cc := context.WithTimeout(context.Background(), 15*time.Second)
		defer cc()
		clean = metadata.NewOutgoingContext(clean, metadata.Pairs("x-user-id", "native-owner"))
		_, e := client.DeleteSession(clean, &apiv1.DeleteSessionRequest{SessionId: session.Id})
		if status.Code(e) != codes.NotFound {
			require.NoError(t, e)
		}
	}()
	resumed, err := client.ResumeSession(owner, &apiv1.ResumeSessionRequest{SessionId: session.Id})
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, resumed.Session.State)
	stored, err := database.NewClient(pool, "public").GetSession(ctx, session.Id, "native-owner")
	require.NoError(t, err)
	require.True(t, bytes.Equal(raw, stored.ExecutionRequirement))
	actor, err := actors.GetActor(ctx, "native", substrate.ActorName(session.Id))
	require.NoError(t, err)
	require.Equal(t, raw, actor.ExecutionRequirement)
	// Governed compute must be running before pinned ingress can accept it.
	require.Equal(t, atepb.ActorState_ACTOR_STATE_RUNNING, actor.Status.State)
	require.NoError(t, os.WriteFile(filepath.Join(root, "kagent-native-actor-uid"), []byte(actor.Metadata.Uid), 0600))
	command := exec.CommandContext(ctx, os.Getenv("EXECUTION_SESSIONS_CLIENT"))
	command.Env = append(os.Environ(), "SESSIONS_NATIVE_ACTOR_UID="+actor.Metadata.Uid, "SESSIONS_NATIVE_ACTOR_NAME=native/"+actor.Metadata.Name)
	out, err := command.CombinedOutput()
	require.NoError(t, os.WriteFile(filepath.Join(root, "kagent-native-harness.log"), out, 0600))
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "NATIVE_SESSIONS_TOOL_PASS")
	deleted, err := client.DeleteSession(owner, &apiv1.DeleteSessionRequest{SessionId: session.Id})
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.Session.State)
	t.Log("PUBLIC_KAGENT_SESSION_NATIVE_COMPUTE_PASS real stored immutable requirement; real native allowed/deny/replay; public exact deletion")
}
