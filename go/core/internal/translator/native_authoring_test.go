package translator_test

import (
	"bytes"
	"encoding/json"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	codexconfig "github.com/kagent-dev/kagent/go/harness/codex/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"testing"
)

// Real compiler over Kaimahi's independently rendered current-fork objects.
// Mock collections resolve declared references; this does not claim admission.
func TestNativeKaimahiCodexAuthoringCompile(t *testing.T) {
	path := os.Getenv("KMX_CODEX_RENDER_CAPTURE")
	if path == "" {
		t.Skip("explicit independent render capture required")
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var b struct {
		Secret      json.RawMessage
		ModelConfig v1alpha3.ModelConfig
		Harness     v1alpha3.Harness
		Agent       v1alpha3.Agent
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	require.NoError(t, d.Decode(&b))
	for _, version := range []string{b.ModelConfig.APIVersion, b.Harness.APIVersion, b.Agent.APIVersion} {
		require.Equal(t, "api.kagent.dev/v1alpha3", version)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: b.ModelConfig.Namespace, Name: b.ModelConfig.Spec.APIKeySecret}, Data: map[string][]byte{b.ModelConfig.Spec.APIKeySecretKey: []byte("test-native-no-secret")}}
	result, err := compiler(t, &b.ModelConfig, &b.Harness, secret).CompileAgent(t.Context(), &b.Agent)
	require.NoError(t, err)
	var config codexconfig.Config
	require.NoError(t, json.Unmarshal(result.ConfigJSON, &config))
	require.NoError(t, config.Validate())
	require.Equal(t, "test-model", config.Model)
	require.Equal(t, "openai", config.Provider.Name)
	require.Equal(t, "http://model.native.test:28292/v1", config.Provider.BaseURL)
	require.Equal(t, b.Agent.Spec.Template.Inline.SystemPrompt, config.DeveloperInstruction)
	require.Equal(t, "default", result.WorkerPoolName)
	require.Equal(t, testHarnessImages.Codex, result.Image)
	if out := os.Getenv("KMX_CODEX_COMPILE_CAPTURE"); out != "" {
		require.NoError(t, os.WriteFile(out, result.ConfigJSON, 0600))
	}
	t.Log("CURRENT_KAIMAHI_V1ALPHA3_NATIVE_CODEX_REAL_COMPILER_PASS")
}
