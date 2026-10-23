package grpcserver

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// These actors deliberately fake only compute. The generated transport,
// authentication/interceptors, PostgreSQL and normal ActorWorkflow are real.
// This does NOT qualify guest enforcement or Kaimahi end-to-end execution.
type publicRequirementActors struct {
	mu          sync.Mutex
	actor       *ateapipb.Actor
	creates     int
	terminated  string
	resumes     int
	operationID string
	retired     bool
}

func (a *publicRequirementActors) GetActor(context.Context, string, string) (*ateapipb.Actor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.actor == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	return proto.CloneOf(a.actor), nil
}
func (a *publicRequirementActors) CreateActor(context.Context, string, string, string, string) (*ateapipb.Actor, error) {
	return nil, fmt.Errorf("governed execution must not use legacy creation")
}
func (a *publicRequirementActors) CreateActorFromTag(context.Context, string, string, string, string, string, string) (*ateapipb.Actor, error) {
	return nil, fmt.Errorf("unexpected tag")
}
func (a *publicRequirementActors) CreateActorWithRequirement(_ context.Context, space, name, ts, tn string, raw []byte) (*ateapipb.Actor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.creates++
	a.actor = &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: space, Name: name, Uid: "fixed-exact-test-actor"}, ActorTemplate: &ateapipb.ObjectRef{Atespace: ts, Name: tn}, ExecutionRequirement: append([]byte(nil), raw...), Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}}
	return proto.CloneOf(a.actor), nil
}
func (a *publicRequirementActors) EnsureActorEgressPolicy(context.Context, string, string, string) error {
	return nil
}
func (a *publicRequirementActors) ResumeActor(context.Context, string, string) (*ateapipb.Actor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resumes++
	a.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
	return proto.CloneOf(a.actor), nil
}
func (a *publicRequirementActors) SuspendActor(context.Context, string, string) (*ateapipb.Actor, error) {
	return nil, fmt.Errorf("unexpected suspend")
}
func (a *publicRequirementActors) DeleteActor(context.Context, string, string) error {
	return fmt.Errorf("unexpected delete")
}

func (a *publicRequirementActors) TerminateActor(_ context.Context, _, _, uid string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.actor == nil || a.actor.GetMetadata().GetUid() != uid {
		return fmt.Errorf("exact UID changed")
	}
	a.terminated = uid
	a.actor = nil
	return nil
}

func TestPublicSessionRequirementTransportAndPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	dsn := dbtest.StartT(t.Context(), t)
	dbtest.MigrateT(t, dsn, false)
	pool, err := database.Connect(t.Context(), &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store := database.NewClient(pool, "public")
	pair := database.AgentDefinition{Namespace: "team", AgentName: "report", AgentUID: "template-uid", DesiredRevision: "governed-revision"}
	require.NoError(t, store.UpsertAgentDefinition(t.Context(), pair))
	require.NoError(t, store.RecordRuntimeRevision(t.Context(), database.RuntimeRevision{Revision: pair.DesiredRevision, Namespace: pair.Namespace, AgentName: pair.AgentName, AgentUID: pair.AgentUID, SourceSnapshot: []byte("{}"), AgentCard: &a2apb.AgentCard{}, EgressDestinations: []string{}, ActorTemplateAtespace: "team", ActorTemplateName: "runtime", ActorTemplateUID: "runtime-uid"}, database.RecordRevisionOptions{Ready: true}))
	actors := &publicRequirementActors{}
	workflow := sessionsvc.NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second)
	listener := bufconn.Listen(DefaultMaxMessageSize)
	server, err := New(Config{Listener: listener, Authenticator: &authimpl.InsecureAuthenticator{}, SystemService: testSystemService(), SessionService: sessionsvc.NewService(store, &pkgauth.NoopAuthorizer{}, workflow)})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	client := apiv1.NewSessionServiceClient(connection)
	owner := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "alice"))
	raw := []byte(`{"format":"non-suite-test/v1","opaque":{"integer":9007199254740993}}`)
	req := &apiv1.CreateSessionRequest{Agent: &apiv1.ResourceReference{Namespace: "team", Name: "report"}, RequestId: uuid.NewString(), ExecutionRequirement: append([]byte(nil), raw...)}
	created, err := client.CreateSession(owner, req)
	require.NoError(t, err)
	require.Equal(t, "alice", created.Session.Creator)
	require.Equal(t, raw, created.Session.ExecutionRequirement)
	actors.mu.Lock()
	require.Equal(t, 1, actors.creates)
	require.Equal(t, raw, actors.actor.ExecutionRequirement)
	actors.mu.Unlock()
	// A new client/store view must not depend on mutable request buffers.
	req.ExecutionRequirement[0] = 'X'
	read, err := client.GetSession(owner, &apiv1.GetSessionRequest{SessionId: created.Session.Id})
	require.NoError(t, err)
	require.True(t, bytes.Equal(raw, read.Session.ExecutionRequirement))
	retry := &apiv1.CreateSessionRequest{Agent: req.Agent, RequestId: req.RequestId}
	_, err = client.CreateSession(owner, retry)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	retry.ExecutionRequirement = append([]byte(nil), raw...)
	replay, err := client.CreateSession(owner, retry)
	require.NoError(t, err)
	require.Equal(t, created.Session.Id, replay.Session.Id)
	stored, err := database.NewClient(pool, "public").GetSession(t.Context(), created.Session.Id, "alice")
	require.NoError(t, err)
	require.Equal(t, raw, stored.ExecutionRequirement)
	actors.mu.Lock()
	require.Equal(t, 1, actors.resumes, "governed READY requires owning lifecycle warm admission")
	require.Equal(t, ateapipb.ActorState_ACTOR_STATE_RUNNING, actors.actor.Status.State)
	actors.mu.Unlock()
	visitor := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "bob"))
	_, err = client.GetSession(visitor, &apiv1.GetSessionRequest{SessionId: created.Session.Id})
	require.Equal(t, codes.NotFound, status.Code(err))
	// Normal public DELETE invokes native destructive deletion, not suspend.
	deleted, err := client.DeleteSession(owner, &apiv1.DeleteSessionRequest{SessionId: created.Session.Id})
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, deleted.Session.State)
	actors.mu.Lock()
	defer actors.mu.Unlock()
	require.Equal(t, 1, actors.creates)
	require.Equal(t, "fixed-exact-test-actor", actors.terminated)
}

// This TEST peer models the owning Control's durable operation and exact UID
// preconditions, not a permissive legacy fallback. No kernel enforcement claim.
func (a *publicRequirementActors) CreateActorOperation(ctx context.Context, b substrate.ActorBinding, id string, raw []byte, retire bool) (*ateapipb.Actor, error) {
	a.mu.Lock()
	if a.operationID != "" && a.operationID != id {
		a.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "operation changed")
	}
	a.operationID = id
	if retire {
		a.retired = true
		actor := proto.CloneOf(a.actor)
		a.mu.Unlock()
		if actor == nil {
			return &ateapipb.Actor{}, nil
		}
		return actor, nil
	}
	if a.retired {
		a.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "retired operation")
	}
	if a.actor != nil {
		actor := proto.CloneOf(a.actor)
		a.mu.Unlock()
		return actor, nil
	}
	a.mu.Unlock()
	return a.CreateActorWithRequirement(ctx, b.Atespace, b.Name, b.TemplateAtespace, b.TemplateName, raw)
}
func (a *publicRequirementActors) ResumeActorExact(_ context.Context, space, name, uid string) (*ateapipb.Actor, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.actor == nil || a.actor.GetMetadata().GetAtespace() != space || a.actor.GetMetadata().GetName() != name || a.actor.GetMetadata().GetUid() != uid {
		return nil, status.Error(codes.Aborted, "exact actor changed")
	}
	a.resumes++
	a.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
	return proto.CloneOf(a.actor), nil
}
