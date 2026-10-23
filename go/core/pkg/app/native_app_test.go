package app

// This opt-in fixture starts the production app.Run event loops and its public
// API. It never calls a reconciler method or writes status/ready/runtime rows.
import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	av1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
	kv1 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	"github.com/kagent-dev/kagent/go/core/pkg/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestCurrentKaimahiCodexRealAdmission(t *testing.T) {
	root := os.Getenv("KMX_TEMPLATE_CONTROL_ROOT")
	capture := os.Getenv("KMX_CODEX_RENDER_CAPTURE")
	if root == "" || capture == "" {
		t.Skip("explicit native owning services and compiler fixture required")
	}
	raw, err := os.ReadFile(capture)
	require.NoError(t, err)
	var bundle struct {
		ModelConfig kv1.ModelConfig
		Harness     kv1.Harness
		Agent       kv1.Agent
		Secret      json.RawMessage
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&bundle))
	bundle.Harness.Spec.Substrate.SnapshotPolicy.ColdStartOnly = true
	task := os.Getenv("EXECUTION_TASK_ROOT")
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join(task, "../delivery-20261022/kagent-guest/helm/kagent-crds/templates"), filepath.Join(task, "substrate-kagent-exact-resume-candidate-20261021/charts/substrate-crds/templates")}, ErrorIfCRDPathMissing: true}
	kc, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	kube, err := kubernetes.NewForConfig(kc)
	require.NoError(t, err)
	ns := bundle.Agent.Namespace
	_, err = kube.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = kube.CoreV1().Secrets(ns).Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: bundle.ModelConfig.Spec.APIKeySecret}, Data: map[string][]byte{bundle.ModelConfig.Spec.APIKeySecretKey: []byte("test-native-no-secret")}}, metav1.CreateOptions{})
	require.NoError(t, err)
	// Public ordinary Kubernetes create, no fixture status or ready assignments.
	worker := &av1.WorkerPool{TypeMeta: metav1.TypeMeta{APIVersion: "api.substrate.run/v1alpha1", Kind: "WorkerPool"}, ObjectMeta: metav1.ObjectMeta{Name: bundle.Harness.Spec.Substrate.WorkerPoolRef.Name, Namespace: ns}, Spec: av1.WorkerPoolSpec{Replicas: 0, WorkerImage: "example.invalid/task-only-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SandboxClass: av1.SandboxClassMicroVM}}
	// Use discovery-resolved group from the actual scheme, not a guessed API.
	worker.APIVersion = "api.substrate.run/v1alpha1"
	for _, crd := range env.CRDInstallOptions.CRDs {
		t.Logf("installed CRD %s group=%s", crd.Name, crd.Spec.Group)
	}
	httpClient, err := rest.HTTPClientFor(kc)
	require.NoError(t, err)
	createRaw := func(group, resource string, value any) {
		data, e := json.Marshal(value)
		require.NoError(t, e)
		request, e := http.NewRequestWithContext(t.Context(), "POST", strings.TrimRight(kc.Host, "/")+"/apis/"+group+"/namespaces/"+ns+"/"+resource, bytes.NewReader(data))
		require.NoError(t, e)
		request.Header.Set("Content-Type", "application/json")
		response, e := httpClient.Do(request)
		require.NoError(t, e)
		defer response.Body.Close()
		body, e := io.ReadAll(response.Body)
		require.NoError(t, e)
		require.Equal(t, 201, response.StatusCode, "request=%s body=%s", request.URL, string(body))
	}
	// The generated client owns the Substrate group spelling.
	worker.APIVersion = av1.SchemeGroupVersion.String()
	createRaw(worker.APIVersion, "workerpools", worker)
	configPath := filepath.Join(root, "normal-kubeconfig")
	kubeconfig := clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"native": {Server: kc.Host, CertificateAuthorityData: kc.CAData}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"native": {ClientCertificateData: kc.CertData, ClientKeyData: kc.KeyData}}, Contexts: map[string]*clientcmdapi.Context{"native": {Cluster: "native", AuthInfo: "native", Namespace: ns}}, CurrentContext: "native"}
	require.NoError(t, clientcmd.WriteToFile(kubeconfig, configPath))
	t.Setenv("KUBECONFIG", configPath)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dsn := dbtest.StartT(ctx, t)
	dsnFile := filepath.Join(root, "normal-db-url")
	require.NoError(t, os.WriteFile(dsnFile, []byte(dsn), 0600))
	listen, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	address := listen.Addr().String()
	require.NoError(t, listen.Close())
	router, e := os.ReadFile(filepath.Join(root, "router-ready"))
	require.NoError(t, e)
	cfg := &configv1.ControllerConfig{}
	cfgRaw := map[string]any{"kubernetes": map[string]any{"namespace": ns, "watchNamespaces": []string{ns}, "leaderElection": false}, "database": map[string]any{"owner": map[string]any{"urlFile": dsnFile, "role": "postgres"}, "readWrite": map[string]any{"urlFile": dsnFile, "role": "postgres"}, "vectorEnabled": false}, "server": map[string]any{"bindAddress": address}, "metrics": map[string]any{"bindAddress": "0"}, "substrate": map[string]any{"apiEndpoint": "unix://" + filepath.Join(root, "control-rpc.sock"), "caFile": filepath.Join(root, "trust.pem"), "routerUrl": "http://" + strings.TrimSpace(string(router))}, "runtimeDefaults": map[string]any{"controlPlaneUrl": "http://control.native.test:28292", "gatewayUrl": "http://control.native.test:28292"}}
	readyRaw, e := os.ReadFile(filepath.Join(root, "atelet-ready.json"))
	require.NoError(t, e)
	var ready struct{ Image string }
	require.NoError(t, json.Unmarshal(readyRaw, &ready))
	require.NotEmpty(t, ready.Image)
	cfgRaw["harnessImages"] = map[string]any{"codex": ready.Image}
	cfgBytes, e := json.Marshal(cfgRaw)
	require.NoError(t, e)
	require.NoError(t, protojson.Unmarshal(cfgBytes, cfg))
	config.DefaultController(cfg)
	require.NoError(t, os.WriteFile(filepath.Join(root, "normal-controller-config.json"), cfgBytes, 0600))
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, Options{}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			require.NoError(t, e)
		case <-time.After(20 * time.Second):
			t.Error("production app did not stop")
		}
	})
	conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, e)
	defer conn.Close()
	publicCtx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "normal-native-test"))
	require.Eventually(t, func() bool {
		c, stop := context.WithTimeout(publicCtx, time.Second)
		defer stop()
		res, e := health.NewHealthClient(conn).Check(c, &health.HealthCheckRequest{})
		return e == nil && res.Status == health.HealthCheckResponse_SERVING
	}, 25*time.Second, 100*time.Millisecond, "production app API startup")
	for _, item := range []struct {
		resource string
		value    any
	}{{"modelconfigs", bundle.ModelConfig}, {"harnesses", bundle.Harness}, {"agents", bundle.Agent}} {
		createRaw("api.kagent.dev/v1alpha3", item.resource, item.value)
	}
	agents := apiv1.NewAgentServiceClient(conn)
	ref := &apiv1.ResourceReference{Namespace: ns, Name: bundle.Agent.Name}
	require.Eventually(t, func() bool {
		c, stop := context.WithTimeout(publicCtx, 2*time.Second)
		defer stop()
		response, e := agents.GetAgent(c, &apiv1.GetAgentRequest{Ref: ref})
		if e != nil {
			return false
		}
		wire, _ := protojson.Marshal(response)
		_ = os.WriteFile(filepath.Join(root, "normal-last-agent.json"), wire, 0600)
		value := response.GetAgent().GetResource().GetValue().AsMap()
		state, ok := value["status"].(map[string]any)
		if !ok {
			return false
		}
		revision, _ := state["latestSuccessfulRevision"].(string)
		return revision != ""
	}, 65*time.Second, 150*time.Millisecond, "normal app must produce golden Ready without manual reconciliation")
	require.NoError(t, os.WriteFile(filepath.Join(root, "normal-ready-observed"), []byte("ordinary AgentService read observed latestSuccessfulRevision"), 0600))
	raw, e = os.ReadFile(filepath.Join(root, "requirement.json"))
	require.NoError(t, e)
	var session *apiv1.Session
	if os.Getenv("KMX_CURRENT_PUBLIC_CLI") == "" {
		sessions := apiv1.NewSessionServiceClient(conn)
		sessionResponse, e := sessions.CreateSession(publicCtx, &apiv1.CreateSessionRequest{Agent: ref, RequestId: "normal-native-original", ExecutionRequirement: raw})
		require.NoError(t, e)
		session = sessionResponse.GetSession()
		require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, session.State)
		defer func() {
			c, stop := context.WithTimeout(context.WithoutCancel(publicCtx), 20*time.Second)
			defer stop()
			_, e := sessions.DeleteSession(c, &apiv1.DeleteSessionRequest{SessionId: session.Id})
			require.NoError(t, e)
		}()
	}
	// Existing guest TaskStore bridge is transport-only to this normal API.
	bridge, e := net.Listen("unix", filepath.Join(root, "np.sock"))
	require.NoError(t, e)
	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	defer bridgeCancel()
	defer bridge.Close()
	go func() {
		for {
			down, e := bridge.Accept()
			if e != nil {
				return
			}
			go func() {
				defer down.Close()
				up, e := (&net.Dialer{}).DialContext(bridgeCtx, "tcp", address)
				if e != nil {
					return
				}
				defer up.Close()
				go func() { _, _ = io.Copy(up, down); _ = up.Close() }()
				_, _ = io.Copy(down, up)
			}()
		}
	}()
	require.NoError(t, os.WriteFile(filepath.Join(root, "kagent-control-path"), []byte(bridge.Addr().String()), 0600))
	if os.Getenv("KMX_CURRENT_PUBLIC_CLI") == "" {
		require.Eventually(t, func() bool { _, e := os.Stat(filepath.Join(root, "native-receiver-ready")); return e == nil }, 10*time.Second, 50*time.Millisecond)
	}
	if binary := os.Getenv("KMX_CURRENT_PUBLIC_CLI"); binary != "" {
		private, e := os.MkdirTemp("/run", "poc-kmx-")
		require.NoError(t, e)
		defer os.RemoveAll(private)
		workspace := filepath.Join(root, "workspace")
		instructions := filepath.Join(root, "instructions.md")
		require.NoError(t, os.WriteFile(instructions, []byte(bundle.Agent.Spec.Template.Inline.SystemPrompt), 0600))
		create := exec.CommandContext(t.Context(), binary, "create", bundle.Agent.Name, "--workspace", workspace, "--runtime", "kagent", "--runtime-version", "a0ad6cb98b02bf4a3537a2b6fdc04e71f315bdf3", "--kagent-runtime", "codex", "--model", bundle.ModelConfig.Spec.Model, "--instructions", instructions)
		create.Env = append(os.Environ(), "KMX_HOME="+filepath.Join(root, "kmx-state"))
		result, e := create.CombinedOutput()
		require.NoError(t, os.WriteFile(filepath.Join(root, "kmx-create.log"), result, 0600))
		require.NoError(t, e, string(result))
		member := filepath.Join(workspace, "agents", bundle.Agent.Name, "member.yaml")
		memberRaw, e := os.ReadFile(member)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(member, append(memberRaw, []byte("\npolicy: policy.json\n")...), 0600))
		var requirement struct {
			Bytes []byte `json:"bytes"`
		}
		require.NoError(t, json.Unmarshal(raw, &requirement))
		require.NoError(t, os.WriteFile(filepath.Join(workspace, "agents", bundle.Agent.Name, "policy.json"), requirement.Bytes, 0600))
		require.NoError(t, os.WriteFile(filepath.Join(workspace, "eval/example.yaml"), []byte("id: native\ninput: Run the native network probe and answer complete.\nexpectContains: [complete]\n"), 0600))
		host := map[string]any{"schemaVersion": 1, "address": address, "namespace": ns, "agent": bundle.Agent.Name, "harness": bundle.Harness.Name, "workerPool": bundle.Harness.Spec.Substrate.WorkerPoolRef.Name, "snapshotLocation": "gs://native-snapshots/prefix", "coldStartOnly": bundle.Harness.Spec.Substrate.SnapshotPolicy.ColdStartOnly, "baseURL": "http://model.native.test:28295/v1", "secretName": bundle.ModelConfig.Spec.APIKeySecret, "secretKey": bundle.ModelConfig.Spec.APIKeySecretKey}
		hostRaw, e := json.Marshal(host)
		require.NoError(t, e)
		hostFile := filepath.Join(private, "current-host.json")
		require.NoError(t, os.WriteFile(hostFile, hostRaw, 0600))
		evaluate := exec.CommandContext(t.Context(), binary, "eval", "--workspace", workspace, "--current-kagent-host", hostFile, "--out", filepath.Join(private, "kmx-result.json"), "--case-timeout", "45s")
		evaluate.Env = create.Env
		result, e = evaluate.CombinedOutput()
		require.NoError(t, os.WriteFile(filepath.Join(root, "kmx-eval.log"), result, 0600))
		require.NoError(t, e, string(result))
		resultRaw, readErr := os.ReadFile(filepath.Join(private, "kmx-result.json"))
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(filepath.Join(root, "kmx-result.json"), resultRaw, 0600))
		require.NoError(t, os.WriteFile(filepath.Join(root, "current-host.json"), hostRaw, 0600))
		t.Log("CURRENT_KAIMAHI_PUBLIC_CLI_NATIVE_PASS public kmx create/eval, normal app reconciliation, public Session and Task, actual native tool and deletion")
		t.Log("CURRENT_REAL_NATIVE_GOLDEN_PREPARATION_READY_PASS")
		return
	}
	payload := map[string]any{"jsonrpc": "2.0", "id": "normal-public", "method": "SendMessage", "params": map[string]any{"message": map[string]any{"role": "ROLE_USER", "messageId": "normal-native-msg", "contextId": session.ContextId, "parts": []any{map[string]any{"text": "Run the native network probe and answer complete."}}}, "configuration": map[string]any{"returnImmediately": false}, "metadata": map[string]any{"kagent.dev/execution-requirement/v1": base64.StdEncoding.EncodeToString(raw)}}}
	body, e := json.Marshal(payload)
	require.NoError(t, e)
	request, e := http.NewRequestWithContext(t.Context(), "POST", "http://"+address+"/agents/"+ns+"/"+bundle.Agent.Name, bytes.NewReader(body))
	require.NoError(t, e)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-User-Id", "normal-native-test")
	response, e := (&http.Client{Timeout: 35 * time.Second}).Do(request)
	require.NoError(t, e)
	defer response.Body.Close()
	answer, e := io.ReadAll(response.Body)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, "normal-public-response.json"), answer, 0600))
	require.Equal(t, 200, response.StatusCode, string(answer))
	require.Contains(t, string(answer), "TASK_STATE_COMPLETED")
	require.Contains(t, string(answer), "complete")
	var result struct {
		Result json.RawMessage
		Error  json.RawMessage
	}
	require.NoError(t, json.Unmarshal(answer, &result))
	require.Empty(t, result.Error, string(answer))
	t.Log("CURRENT_NORMAL_APP_NATIVE_PUBLIC_PASS production app Run, normal reconciliation/status, public Session and Task, native tool output; no status surgery or manual reconciler calls")
	t.Log("CURRENT_REAL_NATIVE_GOLDEN_PREPARATION_READY_PASS")
	_ = fmt.Sprintf("%s", session.Id)
}
