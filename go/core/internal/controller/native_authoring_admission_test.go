package controller

import (
	"encoding/base64"
	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/kagent-dev/kagent/go/core/internal/a2agateway"
	"github.com/kagent-dev/kagent/go/core/internal/grpcserver"
	"github.com/kagent-dev/kagent/go/core/internal/service/system"
	"github.com/kagent-dev/kagent/go/core/internal/service/taskstore"
	"net"

	"bytes"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"

	"context"
	"encoding/json"
	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	native "github.com/kagent-dev/kagent/go/core/internal/substrate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fmt"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/controller/apiclient"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"istio.io/istio/pkg/kube/krt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"strings"
)

// Opt-in real API admission and production informer/compiler graph. Compilation
// alone grants no Ready. Explicit golden/session modes run real native preparation
// and lifecycle against separately owned API, storage and compute services.
func TestCurrentKaimahiCodexRealAdmission(t *testing.T) {
	capture := os.Getenv("KMX_CODEX_RENDER_CAPTURE")
	if capture == "" {
		t.Skip("explicit independent render capture required")
	}
	raw, err := os.ReadFile(capture)
	require.NoError(t, err)
	var bundle struct {
		Secret      json.RawMessage
		ModelConfig kagentv1alpha3.ModelConfig
		Harness     kagentv1alpha3.Harness
		Agent       kagentv1alpha3.Agent
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&bundle))
	// CRD set includes the selected fork's real WorkerPool dependency, not a
	// fabricated discovery fixture or schema-subset validator.
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../../../helm/kagent-crds/templates", "../../../../../substrate-kagent/charts/substrate-crds/templates"}, ErrorIfCRDPathMissing: true}
	config, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	client, err := apiclient.New(config)
	require.NoError(t, err)
	ns := bundle.Agent.Namespace
	_, err = client.Kube().CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.Kube().CoreV1().Secrets(ns).Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: bundle.ModelConfig.Spec.APIKeySecret}, Data: map[string][]byte{bundle.ModelConfig.Spec.APIKeySecretKey: []byte("test-native-no-secret")}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = client.Substrate().ApiV1alpha1().WorkerPools(ns).Create(t.Context(), &atev1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Name: bundle.Harness.Spec.Substrate.WorkerPoolRef.Name}, Spec: atev1alpha1.WorkerPoolSpec{Replicas: 0, WorkerImage: "example.invalid/task-only-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SandboxClass: atev1alpha1.SandboxClassMicroVM}}, metav1.CreateOptions{})
	require.NoError(t, err)

	rawClient, e := rest.HTTPClientFor(config)
	require.NoError(t, e)
	var objects map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &objects))
	for _, item := range []struct{ key, resource, kind string }{{"ModelConfig", "modelconfigs", "ModelConfig"}, {"Harness", "harnesses", "Harness"}, {"Agent", "agents", "Agent"}} {
		var tm metav1.TypeMeta
		require.NoError(t, json.Unmarshal(objects[item.key], &tm))
		require.Equal(t, "api.kagent.dev/v1alpha3", tm.APIVersion)
		require.Equal(t, item.kind, tm.Kind)
		for _, dry := range []bool{true, false} {
			address := fmt.Sprintf("%s/apis/api.kagent.dev/v1alpha3/namespaces/%s/%s", strings.TrimRight(config.Host, "/"), ns, item.resource)
			if dry {
				address += "?dryRun=All"
			}
			request, e := http.NewRequestWithContext(t.Context(), http.MethodPost, address, bytes.NewReader(objects[item.key]))
			require.NoError(t, e)
			request.Header.Set("Content-Type", "application/json")
			response, e := rawClient.Do(request)
			require.NoError(t, e)
			body, e := io.ReadAll(response.Body)
			response.Body.Close()
			require.NoError(t, e)
			require.Equal(t, http.StatusCreated, response.StatusCode, "request=%s body=%s", address, string(body))
		}
		// Detect the exact bug typed-client normalization previously concealed.
		bad := bytes.ReplaceAll(objects[item.key], []byte("api.kagent.dev/v1alpha3"), []byte("kagent.dev/v1alpha3"))
		request, e := http.NewRequestWithContext(t.Context(), http.MethodPost, fmt.Sprintf("%s/apis/api.kagent.dev/v1alpha3/namespaces/%s/%s?dryRun=All", strings.TrimRight(config.Host, "/"), ns, item.resource), bytes.NewReader(bad))
		require.NoError(t, e)
		request.Header.Set("Content-Type", "application/json")
		response, e := rawClient.Do(request)
		require.NoError(t, e)
		body, e := io.ReadAll(response.Body)
		response.Body.Close()
		require.NoError(t, e)
		require.NotEqual(t, http.StatusCreated, response.StatusCode, string(body))
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done; client.Shutdown() })
	settings := testRuntimeDefaults()
	if os.Getenv("EXECUTION_CURRENT_PUBLIC") == "1" {
		settings.ControlPlaneUrl = new("http://control.native.test:28292")
		settings.GatewayUrl = new("http://control.native.test:28292")
	}

	images := &configv1.HarnessImagesConfig{Codex: "example.invalid/task-only-codex@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}

	if os.Getenv("EXECUTION_CURRENT_GOLDEN") == "1" {
		raw, e := os.ReadFile(filepath.Join(os.Getenv("KMX_TEMPLATE_CONTROL_ROOT"), "atelet-ready.json"))
		require.NoError(t, e)
		var info struct{ Image string }
		require.NoError(t, json.Unmarshal(raw, &info))
		require.NotEmpty(t, info.Image)
		images.Codex = info.Image
	}
	graph := NewCollections(client, []string{ns}, krt.NewOptionsBuilder(ctx.Done(), "native-admission", nil), settings, images)
	go func() { defer close(done); client.RunAndWait(ctx.Done()); <-ctx.Done() }()
	var observed AgentReconciliation
	require.Eventually(t, func() bool {
		rows := graph.Reconciliations.List()
		if len(rows) != 1 {
			return false
		}
		observed = rows[0]
		return observed.CompilationFailure == nil && observed.Target != nil
	}, 15*time.Second, 50*time.Millisecond, "real admitted resources failed production compiler")
	require.Nil(t, observed.ObservedActorTemplate, "no compute preparation performed")
	// Persist only source-free prepared identity and actual proto template. This
	// is admission+compilation evidence, not runtime readiness or enforcement.
	if out := os.Getenv("KMX_CODEX_ADMISSION_CAPTURE"); out != "" {
		data, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(observed.Target.ActorTemplate)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Clean(out), data, 0600))
	}
	current, err := client.Kagent().ApiV1alpha3().Agents(ns).Get(t.Context(), bundle.Agent.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, current.UID)
	require.Empty(t, current.Status.LatestSuccessfulRevision)
	t.Log("CURRENT_KAIMAHI_CODEX_REAL_API_ADMISSION_INFORMER_COMPILER_PASS no compute or Ready claimed")
	if root := os.Getenv("KMX_TEMPLATE_CONTROL_ROOT"); root != "" {
		dbctx, stopdb := context.WithCancel(context.WithoutCancel(t.Context()))
		t.Cleanup(stopdb)
		dsn := dbtest.StartT(dbctx, t)
		dbtest.MigrateT(t, dsn, false)
		pool, e := database.Connect(t.Context(), &database.PostgresConfig{Pool: dbtest.PoolConfig(dsn)})
		require.NoError(t, e)
		defer pool.Close()
		store := database.NewClient(pool, "public")

		var templates *native.Client
		if os.Getenv("EXECUTION_CURRENT_GOLDEN") == "1" {
			templates, e = native.Dial(t.Context(), native.Config{AteAPIEndpoint: "unix://" + filepath.Join(root, "control-rpc.sock"), CAFile: filepath.Join(root, "trust.pem"), EgressPolicies: store, DialTimeout: 10 * time.Second, CallTimeout: 30 * time.Second})
			require.NoError(t, e)
			defer templates.Close()
		} else {
			conn, e := grpc.NewClient("unix://"+filepath.Join(root, "template-control.sock"), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, e)
			defer conn.Close()
			templates = &native.Client{ControlClient: atepb.NewControlClient(conn)}
		}

		reconciler, e := NewReconciler(config, graph, store, templates)
		require.NoError(t, e)
		key := ns + "/" + bundle.Agent.Name
		require.NoError(t, reconciler.reconcileAgent(t.Context(), key))
		retained, e := templates.GetActorTemplate(t.Context(), observed.Target.ActorTemplate.GetMetadata().GetAtespace(), observed.Target.ActorTemplate.GetMetadata().GetName())
		require.NoError(t, e)
		require.NotEmpty(t, retained.GetMetadata().GetUid())
		require.True(t, native.ActorTemplateSpecEqual(retained, observed.Target.ActorTemplate), "native defaulting changed immutable compiler identity")
		revision, e := store.GetRuntimeRevision(t.Context(), observed.Target.RevisionID.String())
		require.NoError(t, e)
		require.Equal(t, retained.GetMetadata().GetUid(), revision.ActorTemplateUID)
		require.NoError(t, reconciler.reconcileAgent(t.Context(), key), "pending native template preparation must be idempotent")
		require.Eventually(t, func() bool {
			row := graph.Reconciliations.GetKey(key)
			return row != nil && row.ObservedActorTemplate != nil
		}, 5*time.Second, 20*time.Millisecond)
		require.NoError(t, reconciler.reconcileAgentStatus(t.Context(), key))
		current, e = client.Kagent().ApiV1alpha3().Agents(ns).Get(t.Context(), bundle.Agent.Name, metav1.GetOptions{})
		require.NoError(t, e)
		require.Empty(t, current.Status.LatestSuccessfulRevision)
		for _, condition := range current.Status.Conditions {
			if condition.Type == kagentv1alpha3.AgentConditionReady {
				require.NotEqual(t, metav1.ConditionTrue, condition.Status)
			}
		}
		t.Log("CURRENT_RAW_ADMISSION_PRODUCTION_RECONCILER_NATIVE_API_POSTGRES_PENDING_PASS no golden snapshot or Ready invented")
		if os.Getenv("EXECUTION_CURRENT_GOLDEN") == "1" {
			require.Eventually(t, func() bool {
				e = reconciler.reconcileAgent(t.Context(), key)
				if e != nil {
					t.Log("pending real native preparation", e)
					return false
				}
				row := graph.Reconciliations.GetKey(key)
				if row == nil || row.ObservedActorTemplate == nil {
					return false
				}
				if message := row.ObservedActorTemplate.GetStatus().GetGoldenSnapshotStatus().GetErrorMessage(); message != "" {
					t.Fatal("native golden preparation failed", message)
				}
				if row.ObservedActorTemplate.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() == nil {
					return false
				}
				return reconciler.reconcileAgentStatus(t.Context(), key) == nil
			}, 85*time.Second, 200*time.Millisecond)
			current, e = client.Kagent().ApiV1alpha3().Agents(ns).Get(t.Context(), bundle.Agent.Name, metav1.GetOptions{})
			require.NoError(t, e)
			require.Equal(t, observed.Target.RevisionID.String(), current.Status.LatestSuccessfulRevision)
			ready := false
			for _, condition := range current.Status.Conditions {
				if condition.Type == kagentv1alpha3.AgentConditionReady && condition.Status == metav1.ConditionTrue {
					ready = true
				}
			}
			require.True(t, ready)
			t.Log("CURRENT_REAL_NATIVE_GOLDEN_PREPARATION_READY_PASS actual compiler image native boot data snapshot tag and controller status")
			if os.Getenv("EXECUTION_CURRENT_SESSION") == "1" {
				requirement, e := os.ReadFile(filepath.Join(root, "requirement.json"))
				require.NoError(t, e)
				workflow := sessionsvc.NewActorWorkflow(store, templates, make(chan struct{}, 1), time.Second)
				service := sessionsvc.NewService(store, &auth.NoopAuthorizer{}, workflow)
				owner := auth.AuthSessionTo(t.Context(), &authimpl.SimpleSession{P: auth.Principal{User: auth.User{ID: "native-compiler-test"}}})
				session, e := service.CreateWithRequirement(owner, &apiv1.ResourceReference{Namespace: ns, Name: bundle.Agent.Name}, "compiled-native-live", "", requirement)
				require.NoError(t, e)
				require.Equal(t, observed.Target.RevisionID.String(), session.PreparedRevision)
				require.Equal(t, requirement, session.ExecutionRequirement)
				uid, e := store.SessionActorUID(owner, session.Id)
				require.NoError(t, e)
				require.NotEmpty(t, uid)
				actor, e := templates.GetActor(owner, ns, native.ActorName(session.Id))
				require.NoError(t, e)
				require.Equal(t, uid, actor.GetMetadata().GetUid())
				require.Equal(t, requirement, actor.ExecutionRequirement)
				require.Equal(t, atepb.ActorState_ACTOR_STATE_RUNNING, actor.GetStatus().GetState())

				again, e := service.CreateWithRequirement(owner, &apiv1.ResourceReference{Namespace: ns, Name: bundle.Agent.Name}, "compiled-native-live", "", requirement)
				require.NoError(t, e)
				require.Equal(t, session.Id, again.Id)

				firstDeleted := false
				defer func() {
					if firstDeleted {
						return
					}
					_, err := service.Delete(owner, session.Id)
					if err != nil {
						t.Logf("exact Session cleanup after test failure: %v", err)
					}
				}()
				if os.Getenv("EXECUTION_CURRENT_OVERLAP") == "1" {
					second, err := service.CreateWithRequirement(owner, &apiv1.ResourceReference{Namespace: ns, Name: bundle.Agent.Name}, "compiled-native-overlap", "", requirement)
					require.NoError(t, err)
					secondDeleted := false
					defer func() {
						if secondDeleted {
							return
						}
						_, err := service.Delete(owner, second.Id)
						if err != nil {
							t.Logf("exact overlap Session cleanup: %v", err)
						}
					}()
					secondUID, err := store.SessionActorUID(owner, second.Id)
					require.NoError(t, err)
					require.NotEqual(t, uid, secondUID)
					require.Equal(t, session.PreparedRevision, second.PreparedRevision)
					// Both governed Actors are concurrently running against the same daemon.
					currentCompiledGateway(t, ctx, root, store, service, session, requirement)
					currentCompiledGateway(t, ctx, root, store, service, second, requirement)
					_, err = service.Delete(owner, session.Id)
					require.NoError(t, err)
					firstDeleted = true
					survivor, err := templates.GetActor(owner, ns, native.ActorName(second.Id))
					require.NoError(t, err)
					require.Equal(t, secondUID, survivor.Metadata.Uid)
					require.Equal(t, atepb.ActorState_ACTOR_STATE_RUNNING, survivor.Status.State)
					// Identical first message is replayed, but the variant continuation is new.
					currentCompiledGateway(t, context.WithValue(ctx, currentOverlapAfterKey{}, true), root, store, service, second, requirement)
					_, err = service.Delete(owner, second.Id)
					require.NoError(t, err)
					secondDeleted = true
					require.NoError(t, os.WriteFile(filepath.Join(root, "compiled-overlap-pass.json"), []byte(fmt.Sprintf(`{"firstActor":%q,"secondActor":%q,"distinct":true,"sharedEnforcer":true,"survivorDispatchedAfterFirstDeletion":true}`, uid, secondUID)), 0600))
					t.Log("CURRENT_COMPILED_TWO_LIVE_BINDINGS_SHARED_ENFORCER_SURVIVOR_PASS exact independent deletion and survivor native tool dispatch")
				} else if os.Getenv("EXECUTION_CURRENT_PUBLIC") == "1" {
					currentCompiledGateway(t, ctx, root, store, service, session, requirement)
				}
				if !firstDeleted {
					_, e = service.Delete(owner, session.Id)
					require.NoError(t, e)
					firstDeleted = true
				}
				t.Log("CURRENT_COMPILER_READY_REVISION_NATIVE_GOVERNED_SESSION_PASS exact persisted revision requirement native activation idempotent create and deletion")
				if os.Getenv("EXECUTION_CURRENT_RESTORE_NEGATIVE") == "1" {
					control := templates.ControlClient
					failedRef := &atepb.ObjectRef{Atespace: ns, Name: "compiled-unavailable-enforcer"}
					_, e = control.CreateActor(owner, &atepb.CreateActorRequest{Actor: &atepb.Actor{Metadata: &atepb.ResourceMetadata{Atespace: ns, Name: failedRef.Name}, ActorTemplate: &atepb.ObjectRef{Atespace: ns, Name: retained.Metadata.Name}, ExecutionRequirement: requirement}})
					require.NoError(t, e)
					locator := os.Getenv("SUBSTRATE_EXECUTION_ENFORCER_CONFIG")
					original, e := os.ReadFile(locator)
					require.NoError(t, e)
					var cfg map[string]string
					require.NoError(t, json.Unmarshal(original, &cfg))
					cfg["Socket"] = filepath.Join(root, "absent-enforcer.sock")
					unavailable, e := json.Marshal(cfg)
					require.NoError(t, e)
					func() {
						require.NoError(t, os.WriteFile(locator, unavailable, 0600))
						defer func() { require.NoError(t, os.WriteFile(locator, original, 0600)) }()
						_, e = control.ResumeActor(owner, &atepb.ResumeActorRequest{Actor: failedRef})
						require.Error(t, e)
						require.Contains(t, e.Error(), "execution provider")
					}()
					failed, e := control.GetActor(owner, &atepb.GetActorRequest{Actor: failedRef})
					require.NoError(t, e)
					require.NotEqual(t, atepb.ActorState_ACTOR_STATE_RUNNING, failed.GetStatus().GetState())
					_, e = control.DeleteActor(owner, &atepb.DeleteActorRequest{Actor: failedRef, AnyState: true})
					require.NoError(t, e)
					t.Log("CURRENT_GOVERNED_RESTORE_UNAVAILABLE_ENFORCER_FAIL_CLOSED_PASS paused snapshot denied before resume exact actor deletion")
				}

			}

		}

	}

}

// This uses the real persisted Ready revision and existing governed Session;
// no discovery fixture, synthesized revision, or injected Ready is involved.
type currentOverlapAfterKey struct{}

func currentCompiledGateway(t *testing.T, ctx context.Context, root string, store *database.Client, service *sessionsvc.Service, session *apiv1.Session, requirement []byte) {
	t.Helper()
	authenticator := &authimpl.InsecureAuthenticator{}
	address, e := os.ReadFile(filepath.Join(root, "router-ready"))
	require.NoError(t, e)
	dialer, e := a2agateway.NewRuntimeDialer("http://"+string(bytes.TrimSpace(address)), authenticator, store.SessionActorUID)
	require.NoError(t, e)
	interactions := sessionsvc.NewInteractionService(store, nil, service, false)
	listener, e := net.Listen("unix", filepath.Join(root, "cg.sock"))
	require.NoError(t, e)
	server, e := grpcserver.New(grpcserver.Config{Listener: listener, Authenticator: authenticator, RuntimeAuthenticator: &taskstore.Authenticator{}, SystemService: system.NewService(nil, nil, nil, nil), SessionService: service, TaskStoreService: taskstore.NewService(store, nil, nil), HTTPHandler: a2agateway.NewHTTPHandler(a2agateway.New(interactions, dialer, "http://compiled.invalid"), authenticator, store)})
	require.NoError(t, e)
	run, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Start(run) }()
	defer func() { cancel(); require.NoError(t, <-done) }()
	require.NoError(t, os.WriteFile(filepath.Join(root, "kagent-control-path"), []byte(listener.Addr().String()), 0600))
	require.Eventually(t, func() bool { _, e := os.Stat(filepath.Join(root, "native-receiver-ready")); return e == nil }, 15*time.Second, 50*time.Millisecond)
	transport := &http.Transport{DialContext: func(c context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(c, "unix", listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 45 * time.Second}
	payload := map[string]any{"jsonrpc": "2.0", "id": "compiled-public", "method": "SendMessage", "params": map[string]any{"message": map[string]any{"role": "ROLE_USER", "messageId": "compiled-public-msg", "contextId": session.ContextId, "parts": []any{map[string]any{"text": "Run the native network probe and answer complete."}}}, "metadata": map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(requirement)}, "configuration": map[string]any{"returnImmediately": false}}}
	raw, e := json.Marshal(payload)
	require.NoError(t, e)
	request, e := http.NewRequestWithContext(ctx, "POST", "http://compiled.invalid/agents/"+session.Agent.Namespace+"/"+session.Agent.Name, bytes.NewReader(raw))
	require.NoError(t, e)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-User-Id", "native-compiler-test")

	if os.Getenv("EXECUTION_CURRENT_MODEL_DENY") == "1" {
		blocked, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		response, err := client.Do(request.WithContext(blocked))
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			require.NoError(t, closeErr)
			require.NoError(t, readErr)
			var result struct {
				Error json.RawMessage `json:"error"`
			}
			require.NoError(t, json.Unmarshal(body, &result))
			require.NotEmpty(t, result.Error, string(body))
		} else {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		task, err := store.GetSessionTaskByMessage(ctx, session.Id, "", "compiled-public-msg")
		require.NoError(t, err)
		require.NotNil(t, task)
		require.NotEqual(t, a2atype.TaskStateCompleted, task.Status.State)
		record, err := json.Marshal(map[string]any{"taskId": task.ID, "contextId": task.ContextID, "state": task.Status.State, "modelDestinationRemoved": true, "completed": false})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "compiled-model-refusal.json"), record, 0600))
		t.Log("CURRENT_COMPILED_GATEWAY_MODEL_DESTINATION_DENIED_PASS accepted non-completed Task; no model or tool receiver traffic")
		return
	}
	response, e := client.Do(request)
	require.NoError(t, e)
	defer response.Body.Close()
	body, e := io.ReadAll(response.Body)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "compiled-public-response.json"), body, 0600))
	require.Equal(t, 200, response.StatusCode)
	var result struct {
		Error  json.RawMessage `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Empty(t, result.Error, string(body))
	require.Contains(t, string(result.Result), "TASK_STATE_COMPLETED", string(body))
	var decoded struct {
		Task a2atype.Task `json:"task"`
	}
	require.NoError(t, json.Unmarshal(result.Result, &decoded))
	require.NotEmpty(t, decoded.Task.ID)
	stored, e := store.GetSessionTask(ctx, session.Id, string(decoded.Task.ID), nil)
	require.NoError(t, e)
	require.Equal(t, decoded.Task.ID, stored.ID)
	require.Equal(t, session.ContextId, stored.ContextID)
	require.Equal(t, a2atype.TaskStateCompleted, stored.Status.State)
	retained, e := store.GetSessionByID(ctx, session.Id)
	require.NoError(t, e)
	require.Equal(t, session.PreparedRevision, retained.PreparedRevision)
	require.Equal(t, requirement, []byte(retained.ExecutionRequirement))
	// Refused continuations must not create a Task or reach the model receiver.
	for _, mode := range []string{"omitted", "changed", "malformed", "nested"} {
		message := map[string]any{"role": "ROLE_USER", "messageId": "compiled-refusal-" + mode, "contextId": session.ContextId, "parts": []any{map[string]any{"text": "must not run"}}}
		params := map[string]any{"message": message}
		switch mode {
		case "changed":
			params["metadata"] = map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString([]byte(`{"changed":true}`))}
		case "malformed":
			params["metadata"] = map[string]any{"kagent.dev/execution-requirement/v1": "%%%"}
		case "nested":
			message["metadata"] = map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(requirement)}
		}
		input, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": mode, "method": "SendMessage", "params": params})
		require.NoError(t, e)
		req, e := http.NewRequestWithContext(ctx, "POST", "http://compiled.invalid/agents/"+session.Agent.Namespace+"/"+session.Agent.Name, bytes.NewReader(input))
		require.NoError(t, e)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "native-compiler-test")
		res, e := client.Do(req)
		require.NoError(t, e)
		out, e := io.ReadAll(res.Body)
		require.NoError(t, res.Body.Close())
		require.NoError(t, e)
		var refusal struct {
			Error  json.RawMessage `json:"error"`
			Result json.RawMessage `json:"result"`
		}
		require.NoError(t, json.Unmarshal(out, &refusal))
		require.NotEmpty(t, refusal.Error, string(out))
		require.Empty(t, refusal.Result, string(out))
		require.NoError(t, os.WriteFile(filepath.Join(root, "compiled-refusal-"+mode+".json"), out, 0600))
		_, e = store.GetSessionTaskByMessage(ctx, session.Id, "", "compiled-refusal-"+mode)
		require.Error(t, e)
	}
	t.Log("CURRENT_COMPILED_GATEWAY_IMMUTABLE_REQUIREMENT_REFUSALS_PASS omitted changed malformed nested no persisted Task or receiver traffic")

	if os.Getenv("EXECUTION_CURRENT_RECOVERY") == "1" {
		beforeUID, err := store.SessionActorUID(ctx, session.Id)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "restart-request"), nil, 0600))
		require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(root, "restart-ready")); return err == nil }, 15*time.Second, 50*time.Millisecond)
		send := func(input []byte) []byte {
			t.Helper()
			req, err := http.NewRequestWithContext(ctx, "POST", "http://compiled.invalid/agents/"+session.Agent.Namespace+"/"+session.Agent.Name, bytes.NewReader(input))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-User-Id", "native-compiler-test")
			res, err := client.Do(req)
			require.NoError(t, err)
			out, err := io.ReadAll(res.Body)
			require.NoError(t, res.Body.Close())
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)
			return out
		}
		if os.Getenv("EXECUTION_CURRENT_WARM_REFUSAL") == "1" {
			locator := os.Getenv("SUBSTRATE_EXECUTION_ENFORCER_CONFIG")
			original, err := os.ReadFile(locator)
			require.NoError(t, err)
			var cfg map[string]string
			require.NoError(t, json.Unmarshal(original, &cfg))
			cfg["Socket"] = filepath.Join(root, "absent-warm.sock")
			unavailable, err := json.Marshal(cfg)
			require.NoError(t, err)
			refused := map[string]any{"jsonrpc": "2.0", "id": "compiled-warm-refused", "method": "SendMessage", "params": map[string]any{"message": map[string]any{"role": "ROLE_USER", "messageId": "compiled-warm-refused-msg", "contextId": session.ContextId, "parts": []any{map[string]any{"text": "Do not run while exact policy lookup is unavailable."}}}, "metadata": map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(requirement)}, "configuration": map[string]any{"returnImmediately": false}}}
			payload, err := json.Marshal(refused)
			require.NoError(t, err)
			func() {
				require.NoError(t, os.WriteFile(locator, unavailable, 0600))
				defer func() { require.NoError(t, os.WriteFile(locator, original, 0600)) }()
				out := send(payload)
				var result struct {
					Error json.RawMessage `json:"error"`
				}
				require.NoError(t, json.Unmarshal(out, &result))
				require.NotEmpty(t, result.Error, string(out))
				require.NoError(t, os.WriteFile(filepath.Join(root, "compiled-warm-refusal.json"), out, 0600))
				_, err := store.GetSessionTaskByMessage(ctx, session.Id, "", "compiled-warm-refused-msg")
				require.Error(t, err)
			}()
			t.Log("CURRENT_COMPILED_WARM_UNAVAILABLE_ENFORCER_NO_TASK_PASS exact policy lookup refusal before Task dispatch then restored config")
		}
		// An accepted initial message remains exactly-once across provider restart.
		replay := send(raw)
		var first struct {
			Error  json.RawMessage `json:"error"`
			Result struct {
				Task a2atype.Task `json:"task"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(replay, &first))
		require.Empty(t, first.Error, string(replay))
		require.Equal(t, decoded.Task.ID, first.Result.Task.ID)
		next := map[string]any{"jsonrpc": "2.0", "id": "compiled-recovered", "method": "SendMessage", "params": map[string]any{"message": map[string]any{"role": "ROLE_USER", "messageId": "compiled-recovered-msg", "contextId": session.ContextId, "parts": []any{map[string]any{"text": "Repeat the native network probe and answer complete."}}}, "metadata": map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(requirement)}, "configuration": map[string]any{"returnImmediately": false}}}
		encoded, err := json.Marshal(next)
		require.NoError(t, err)
		out := send(encoded)
		var recovered struct {
			Error  json.RawMessage `json:"error"`
			Result struct {
				Task a2atype.Task `json:"task"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(out, &recovered))
		require.Empty(t, recovered.Error, string(out))
		require.Equal(t, a2atype.TaskStateCompleted, recovered.Result.Task.Status.State)
		require.Equal(t, session.ContextId, recovered.Result.Task.ContextID)
		require.NotEqual(t, decoded.Task.ID, recovered.Result.Task.ID)
		saved, err := store.GetSessionTask(ctx, session.Id, string(recovered.Result.Task.ID), nil)
		require.NoError(t, err)
		require.Equal(t, a2atype.TaskStateCompleted, saved.Status.State)
		require.NoError(t, os.WriteFile(filepath.Join(root, "compiled-recovered-response.json"), out, 0600))
		duplicate := send(encoded)
		var retry struct {
			Error  json.RawMessage `json:"error"`
			Result struct {
				Task a2atype.Task `json:"task"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(duplicate, &retry))
		require.Empty(t, retry.Error, string(duplicate))
		require.Equal(t, recovered.Result.Task.ID, retry.Result.Task.ID)
		afterUID, err := store.SessionActorUID(ctx, session.Id)
		require.NoError(t, err)
		require.Equal(t, beforeUID, afterUID)
		final, err := store.GetSessionByID(ctx, session.Id)
		require.NoError(t, err)
		require.Equal(t, requirement, []byte(final.ExecutionRequirement))
		require.Equal(t, session.PreparedRevision, final.PreparedRevision)
		t.Log("CURRENT_COMPILED_REVISION_GUARDD_SIGKILL_CONTINUATION_REPLAY_PASS same Actor requirement and revision; exact initial and continuation Tasks; no duplicate model/tool traffic")
	}
	if os.Getenv("EXECUTION_CURRENT_OVERLAP") == "1" {
		messageID := "overlap-live-msg"
		rpcID := "overlap-live"
		if ctx.Value(currentOverlapAfterKey{}) == true {
			messageID = "overlap-survivor-msg"
			rpcID = "overlap-survivor"
		}
		input, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpcID, "method": "SendMessage", "params": map[string]any{"message": map[string]any{"role": "ROLE_USER", "messageId": messageID, "contextId": session.ContextId, "parts": []any{map[string]any{"text": "Repeat the native network probe and answer complete."}}}, "metadata": map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(requirement)}, "configuration": map[string]any{"returnImmediately": false}}})
		require.NoError(t, err)
		request, err := http.NewRequestWithContext(ctx, "POST", "http://compiled.invalid/agents/"+session.Agent.Namespace+"/"+session.Agent.Name, bytes.NewReader(input))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-User-Id", "native-compiler-test")
		res, err := client.Do(request)
		require.NoError(t, err)
		out, err := io.ReadAll(res.Body)
		require.NoError(t, res.Body.Close())
		require.NoError(t, err)
		var result struct {
			Error  json.RawMessage `json:"error"`
			Result struct {
				Task a2atype.Task `json:"task"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(out, &result))
		require.Empty(t, result.Error, string(out))
		require.Equal(t, a2atype.TaskStateCompleted, result.Result.Task.Status.State)
		require.NotEqual(t, decoded.Task.ID, result.Result.Task.ID)
		require.Equal(t, session.ContextId, result.Result.Task.ContextID)
		task, err := store.GetSessionTaskByMessage(ctx, session.Id, "", messageID)
		require.NoError(t, err)
		require.Equal(t, result.Result.Task.ID, task.ID)
		require.NoError(t, os.WriteFile(filepath.Join(root, session.Id+"-"+rpcID+".json"), out, 0600))
	}
	t.Log("CURRENT_COMPILED_READY_REVISION_PUBLIC_GATEWAY_NATIVE_DISPATCH_PASS real persisted Task and Codex tool roundtrip")
}
