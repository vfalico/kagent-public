package a2a

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
)

// ExecutionRequirementMetadataKey carries opaque whole-execution input, never
// a target, activation proof or authorization grant. Standard padded base64
// preserves exact bytes without JSON number coercion or reserialization.
const ExecutionRequirementMetadataKey = "kagent.dev/execution-requirement/v1"
const maxExecutionRequirementBytes = 1 << 20

// ExecutionRequirement decodes the request-level carrier only. Message/part
// carriers are refused rather than silently ignored or given ambiguous scope.
func ExecutionRequirement(req *a2atype.SendMessageRequest) ([]byte, error) {
	invalid := func() ([]byte, error) { return nil, errors.New("invalid execution requirement carrier") }
	if req == nil || req.Message == nil {
		return invalid()
	}
	if _, ok := req.Message.Metadata[ExecutionRequirementMetadataKey]; ok {
		return invalid()
	}
	for _, part := range req.Message.Parts {
		if part != nil {
			if _, ok := part.Metadata[ExecutionRequirementMetadataKey]; ok {
				return invalid()
			}
		}
	}
	value, present := req.Metadata[ExecutionRequirementMetadataKey]
	if !present {
		return nil, nil
	}
	encoded, ok := value.(string)
	if !ok || len(encoded) == 0 || len(encoded) > base64.StdEncoding.EncodedLen(maxExecutionRequirementBytes) {
		return invalid()
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(raw) == 0 || len(raw) > maxExecutionRequirementBytes || base64.StdEncoding.EncodeToString(raw) != encoded || !json.Valid(raw) {
		return invalid()
	}
	return append([]byte(nil), raw...), nil
}
