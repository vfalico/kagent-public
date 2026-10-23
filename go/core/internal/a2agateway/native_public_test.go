package a2agateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	"github.com/kagent-dev/kagent/go/core/internal/grpcserver"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
	"github.com/kagent-dev/kagent/go/core/internal/service/system"
	"github.com/kagent-dev/kagent/go/core/internal/service/taskstore"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type nativeAgentReader struct{ agent *v1alpha3.Agent }

func (r nativeAgentReader) Get(context.Context, types.NamespacedName) (*v1alpha3.Agent, error) {
	return r.agent.DeepCopy(), nil
}

// Explicit native qualification: real public gateway, PostgreSQL Session and
// Task storage, owning compute and Codex; only Agent discovery is test-owned.
func TestPublicKaimahiNativeA2A(t *testing.T) {
	root := os.Getenv("EXECUTION_KAGENT_NATIVE_ROOT")
	if root == "" || os.Getenv("EXECUTION_NATIVE_KAGENT_A2A") != "1" {
		t.Skip("explicit native resources")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	dbctx, stopdb := context.WithCancel(context.WithoutCancel(ctx))
	t.Cleanup(stopdb)
	dsn := dbtest.StartT(dbctx, t)
	dbtest.MigrateT(t, dsn, false)
	pool, err := database.Connect(ctx, &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
	require.NoError(t, err)
	defer pool.Close()
	store := database.NewClient(pool, "public")
	tlscred, err := credentials.NewClientTLSFromFile(filepath.Join(root, "trust.pem"), "127.0.0.1")
	require.NoError(t, err)
	conn, err := grpc.NewClient("unix://"+filepath.Join(root, "control-rpc.sock"), grpc.WithTransportCredentials(tlscred))
	require.NoError(t, err)
	defer conn.Close()
	template, err := atepb.NewControlClient(conn).GetActorTemplate(ctx, &atepb.GetActorTemplateRequest{ActorTemplate: &atepb.ObjectRef{Atespace: "native", Name: "toolchat"}})
	require.NoError(t, err)
	pair := database.AgentDefinition{Namespace: "agents", AgentName: "sample", AgentUID: "native-agent", DesiredRevision: "native-revision"}
	require.NoError(t, store.UpsertAgentDefinition(ctx, pair))
	require.NoError(t, store.RecordRuntimeRevision(ctx, database.RuntimeRevision{Revision: pair.DesiredRevision, Namespace: pair.Namespace, AgentName: pair.AgentName, AgentUID: pair.AgentUID, SourceSnapshot: []byte("{}"), AgentCard: &a2apb.AgentCard{}, EgressDestinations: []string{}, ActorTemplateAtespace: "native", ActorTemplateName: "toolchat", ActorTemplateUID: template.Metadata.Uid}, database.RecordRevisionOptions{Ready: true}))
	actors, err := substrate.Dial(ctx, substrate.Config{AteAPIEndpoint: "unix://" + filepath.Join(root, "control-rpc.sock"), CAFile: filepath.Join(root, "trust.pem"), EgressPolicies: store, CallTimeout: 30 * time.Second})
	require.NoError(t, err)
	defer actors.Close()
	workflow := sessionsvc.NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second)
	sessions := sessionsvc.NewService(store, &auth.NoopAuthorizer{}, workflow)
	authenticator := &authimpl.InsecureAuthenticator{}
	interactions := sessionsvc.NewInteractionService(store, nativeAgentReader{&v1alpha3.Agent{ObjectMeta: metav1.ObjectMeta{Name: pair.AgentName, Namespace: pair.Namespace}, Status: v1alpha3.AgentStatus{LatestSuccessfulRevision: pair.DesiredRevision}}}, sessions, false)
	dialer, err := NewRuntimeDialer("http://"+os.Getenv("SESSIONS_NATIVE_ROUTER"), authenticator, store.SessionActorUID)
	require.NoError(t, err)
	socketRoot, err := os.MkdirTemp("/tmp", "poc-a2a-")
	require.NoError(t, err)
	defer os.RemoveAll(socketRoot)
	socketPath := filepath.Join(socketRoot, "control.sock")
	require.NoError(t, os.WriteFile(filepath.Join(root, "kagent-control-path"), []byte(socketPath), 0600))
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	server, err := grpcserver.New(grpcserver.Config{Listener: listener, Authenticator: authenticator, RuntimeAuthenticator: &taskstore.Authenticator{}, SystemService: system.NewService(nil, nil, nil, nil), SessionService: sessions, TaskStoreService: taskstore.NewService(store, nil, nil), HTTPHandler: NewHTTPHandler(New(interactions, dialer, "http://native.invalid"), authenticator, store)})
	require.NoError(t, err)
	sc, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Start(sc) }()
	defer func() { stop(); require.NoError(t, <-done) }()
	// Kaimahi owns resolution and JSON-RPC carrier. Its kubectl fixture only
	// models discovery/placement; the send uses the actual gateway above.
	cmd := exec.CommandContext(ctx, os.Getenv("EXECUTION_KAIMAHI_KAGENT_TEST"), "-test.run=^TestSuiteKagentRealNativeA2ACarrier$", "-test.v", "-test.timeout=100s")
	cmd.Env = append(os.Environ(), "KMX_KAGENT_NATIVE_SOCKET="+listener.Addr().String())
	out, err := cmd.CombinedOutput()
	require.NoError(t, os.WriteFile(filepath.Join(root, "kaimahi-public-a2a.log"), out, 0600))
	require.NoError(t, err, string(out))
	rows, err := store.ListSessions(ctx, database.SessionQuery{AllUsers: true, Limit: 10})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	raw, err := os.ReadFile(filepath.Join(root, "requirement.json"))
	require.NoError(t, err)
	require.JSONEq(t, string(raw), string(rows[0].ExecutionRequirement))
	require.NoError(t, os.WriteFile(filepath.Join(root, "public-session.json"), mustNativeJSON(t, rows[0]), 0600))

	if os.Getenv("EXECUTION_KAGENT_NATIVE_SCENARIO") == "deny" {
		owner := auth.AuthSessionTo(ctx, &authimpl.SimpleSession{P: auth.Principal{User: auth.User{ID: rows[0].Creator}}})
		_, err = sessions.Delete(owner, rows[0].Id)
		require.NoError(t, err)
		t.Log("PUBLIC_KAIMAHI_KAGENT_A2A_NATIVE_PASS actual forbidden native tool; no passing receipt; exact deletion")
		return
	}
	// Real-gateway continuation refusal must precede native model/tool dispatch.
	// Observe persisted Task cardinality, not merely an error string.
	var beforeTasks int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM session_task").Scan(&beforeTasks))
	transport := &http.Transport{DialContext: func(c context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(c, "unix", socketPath)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	for _, mode := range []string{"omitted", "changed", "malformed", "nested"} {
		msg := map[string]any{"role": "ROLE_USER", "messageId": "native-refusal-" + mode, "contextId": rows[0].ContextId, "parts": []any{map[string]any{"text": "must not execute"}}}
		params := map[string]any{"message": msg, "configuration": map[string]any{"returnImmediately": false}}
		switch mode {
		case "changed":
			params["metadata"] = map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString([]byte(`{"opaque":"changed"}`))}
		case "malformed":
			params["metadata"] = map[string]any{"kagent.dev/execution-requirement/v1": "!"}
		case "nested":
			msg["metadata"] = map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(raw)}
		}
		payload := mustNativeJSON(t, map[string]any{"jsonrpc": "2.0", "id": mode, "method": "SendMessage", "params": params})
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, "http://native/agents/"+pair.Namespace+"/"+pair.AgentName, bytes.NewReader(payload))
		require.NoError(t, e)
		req.Header.Set("Content-Type", "application/json")
		response, e := client.Do(req)
		require.NoError(t, e)
		body, e := io.ReadAll(response.Body)
		response.Body.Close()
		require.NoError(t, e)
		var result struct {
			ID    string `json:"id"`
			Error *struct {
				Code int `json:"code"`
			} `json:"error"`
			Result json.RawMessage `json:"result"`
		}
		require.NoError(t, json.Unmarshal(body, &result))
		require.Equal(t, mode, result.ID)
		require.NotNil(t, result.Error, string(body))
		require.Empty(t, result.Result, string(body))
		require.NoError(t, os.WriteFile(filepath.Join(root, "continuation-refusal-"+mode+".json"), body, 0600))
		var afterTasks int
		require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM session_task").Scan(&afterTasks))
		require.Equal(t, beforeTasks, afterTasks, "refused continuation created Task")
	}
	t.Log("REAL_GATEWAY_PERSISTED_AUTHORITY_REFUSALS_PASS omitted changed malformed nested; no extra Task or native traffic")

	require.NoError(t, os.WriteFile(filepath.Join(root, "restart-request"), nil, 0600))
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, "restart-ready")); err == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "exact Guardd restart deadline")
		time.Sleep(50 * time.Millisecond)
	}
	beforeUID, err := store.SessionActorUID(ctx, rows[0].Id)
	require.NoError(t, err)
	continuation := map[string]any{"jsonrpc": "2.0", "id": "native-recovered", "method": "SendMessage", "params": map[string]any{
		"message":       map[string]any{"messageId": "native-recovered-message", "role": "ROLE_USER", "contextId": rows[0].ContextId, "parts": []any{map[string]any{"text": "repeat native network probe"}}},
		"configuration": map[string]any{"returnImmediately": false}, "metadata": map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(rows[0].ExecutionRequirement)}}}
	payload := mustNativeJSON(t, continuation)
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, "http://native/agents/"+pair.Namespace+"/"+pair.AgentName, bytes.NewReader(payload))
	require.NoError(t, e)
	req.Header.Set("Content-Type", "application/json")
	response, e := client.Do(req)
	require.NoError(t, e)
	body, e := io.ReadAll(response.Body)
	response.Body.Close()
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "continuation-after-restart.json"), body, 0600))
	var recovered struct {
		Error  json.RawMessage
		Result struct {
			Task struct {
				ID        string
				ContextID string `json:"contextId"`
				Status    struct{ State string }
				Artifacts []json.RawMessage
			}
		}
	}
	require.NoError(t, json.Unmarshal(body, &recovered))
	require.Empty(t, recovered.Error, string(body))
	require.Equal(t, "TASK_STATE_COMPLETED", recovered.Result.Task.Status.State, string(body))
	require.Equal(t, rows[0].ContextId, recovered.Result.Task.ContextID)
	require.NotEmpty(t, recovered.Result.Task.ID)
	require.NotEmpty(t, recovered.Result.Task.Artifacts)
	afterUID, err := store.SessionActorUID(ctx, rows[0].Id)
	require.NoError(t, err)
	require.Equal(t, beforeUID, afterUID, "restart continuation replaced native Actor")
	var recoveredTasks int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM session_task").Scan(&recoveredTasks))
	require.Equal(t, beforeTasks+1, recoveredTasks)
	t.Log("REAL_GATEWAY_NATIVE_CONTINUATION_AFTER_GUARDD_SIGKILL_PASS identical immutable policy, same retained Actor, new completed Task")

	// Model a lost initial reply: replay Kaimahi's exact initial request, never create another
	// Task or dispatch another native model/tool execution.
	initialPayload, e := os.ReadFile(filepath.Join(root, "gateway-request.json"))
	require.NoError(t, e)
	initialResponseRaw, e := os.ReadFile(filepath.Join(root, "gateway-response.json"))
	require.NoError(t, e)
	var initialResponse struct {
		Result struct{ Task struct{ ID string } }
	}
	require.NoError(t, json.Unmarshal(initialResponseRaw, &initialResponse))
	retryReq, e := http.NewRequestWithContext(ctx, http.MethodPost, "http://native/agents/"+pair.Namespace+"/"+pair.AgentName, bytes.NewReader(initialPayload))
	require.NoError(t, e)
	retryReq.Header.Set("Content-Type", "application/json")
	retryResponse, e := client.Do(retryReq)
	require.NoError(t, e)
	retryBody, e := io.ReadAll(retryResponse.Body)
	retryResponse.Body.Close()
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "continuation-exact-retry.json"), retryBody, 0600))
	var retryResult struct {
		Error  json.RawMessage
		Result struct {
			Task struct {
				ID        string
				ContextID string `json:"contextId"`
				Status    struct{ State string }
			}
		}
	}
	require.NoError(t, json.Unmarshal(retryBody, &retryResult))
	require.Empty(t, retryResult.Error, string(retryBody))
	require.Equal(t, initialResponse.Result.Task.ID, retryResult.Result.Task.ID)
	require.Equal(t, recovered.Result.Task.ContextID, retryResult.Result.Task.ContextID)
	require.Equal(t, "TASK_STATE_COMPLETED", retryResult.Result.Task.Status.State)
	var retryTasks int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM session_task").Scan(&retryTasks))
	require.Equal(t, recoveredTasks, retryTasks, "exact accepted-message retry created another Task")
	t.Log("REAL_GATEWAY_EXACT_ACCEPTED_INITIAL_RETRY_PASS identical persisted Task and no repeated model/tool dispatch")

	// Lost continuation reply: replay the identical accepted context message.
	// Persisted acceptance, not a new runtime dispatch, must supply the result.
	continuationRetry, e := http.NewRequestWithContext(ctx, http.MethodPost, "http://native/agents/"+pair.Namespace+"/"+pair.AgentName, bytes.NewReader(payload))
	require.NoError(t, e)
	continuationRetry.Header.Set("Content-Type", "application/json")
	continuationResponse, e := client.Do(continuationRetry)
	require.NoError(t, e)
	continuationBody, e := io.ReadAll(continuationResponse.Body)
	continuationResponse.Body.Close()
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "accepted-continuation-retry.json"), continuationBody, 0600))
	var continuationResult struct {
		Error  json.RawMessage
		Result struct {
			Task struct {
				ID        string
				ContextID string `json:"contextId"`
				Status    struct{ State string }
			}
		}
	}
	require.NoError(t, json.Unmarshal(continuationBody, &continuationResult))
	require.Empty(t, continuationResult.Error, string(continuationBody))
	require.Equal(t, recovered.Result.Task.ID, continuationResult.Result.Task.ID)
	require.Equal(t, recovered.Result.Task.ContextID, continuationResult.Result.Task.ContextID)
	require.Equal(t, "TASK_STATE_COMPLETED", continuationResult.Result.Task.Status.State)
	var afterContinuationRetry int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM session_task").Scan(&afterContinuationRetry))
	require.Equal(t, recoveredTasks, afterContinuationRetry)
	t.Log("REAL_GATEWAY_EXACT_ACCEPTED_CONTINUATION_RETRY_PASS identical persisted Task and no repeated model/tool dispatch")
	owner := auth.AuthSessionTo(ctx, &authimpl.SimpleSession{P: auth.Principal{User: auth.User{ID: rows[0].Creator}}})
	_, err = sessions.Delete(owner, rows[0].Id)
	require.NoError(t, err)
	t.Log("PUBLIC_KAIMAHI_KAGENT_A2A_NATIVE_PASS real A2A, persistent Task, owning native model/tool execution; fixture discovery boundary")
}
func mustNativeJSON(t *testing.T, x any) []byte {
	t.Helper()
	b, e := json.MarshalIndent(x, "", "  ")
	require.NoError(t, e)
	return b
}
