package a2a

import (
	"bytes"
	"encoding/base64"
	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"strings"
	"testing"
)

func TestExecutionRequirementExactOpaqueCarrier(t *testing.T) {
	raw := []byte(`{ "opaque": 9007199254740993, "kind":"non-suite/v1" }`)
	encoded := base64.StdEncoding.EncodeToString(raw)
	for _, tc := range []struct {
		name    string
		value   any
		present bool
		valid   bool
	}{
		{"legacy", nil, false, true}, {"exact", encoded, true, true}, {"empty", "", true, false},
		{"number", 1, true, false}, {"null", nil, true, false}, {"object", map[string]any{}, true, false},
		{"bad-base64", "!", true, false}, {"newline", encoded + "\n", true, false},
		{"invalid-json", base64.StdEncoding.EncodeToString([]byte("{")), true, false},
		{"oversize", strings.Repeat("A", base64.StdEncoding.EncodedLen(maxExecutionRequirementBytes)+4), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &a2atype.SendMessageRequest{Message: a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("input")), Metadata: map[string]any{}}
			if tc.present {
				req.Metadata[ExecutionRequirementMetadataKey] = tc.value
			}
			got, err := ExecutionRequirement(req)
			if (err == nil) != tc.valid {
				t.Fatal("unexpected carrier result", err)
			}
			if tc.name == "exact" && !bytes.Equal(got, raw) {
				t.Fatal("opaque bytes changed")
			}
		})
	}
	for _, scope := range []string{"message", "part"} {
		t.Run(scope, func(t *testing.T) {
			req := &a2atype.SendMessageRequest{Message: a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("input"))}
			if scope == "message" {
				req.Message.SetMeta(ExecutionRequirementMetadataKey, encoded)
			} else {
				req.Message.Parts[0].Metadata = map[string]any{ExecutionRequirementMetadataKey: encoded}
			}
			if _, err := ExecutionRequirement(req); err == nil {
				t.Fatal("ambiguous carrier accepted")
			}
		})
	}
}
