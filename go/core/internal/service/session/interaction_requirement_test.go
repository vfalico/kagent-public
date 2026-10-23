package session

import (
	"bytes"
	"context"
	"encoding/base64"
	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

type requirementAgentService struct{}

func (requirementAgentService) Get(context.Context, types.NamespacedName) (*v1alpha3.Agent, error) {
	return &v1alpha3.Agent{}, nil
}
func TestA2ASendFreezesRequirementBeforeDispatch(t *testing.T) {
	raw := []byte(`{ "format":"non-suite/v1", "opaque":9007199254740993 }`)
	store := &serviceTestStore{}
	s := NewInteractionService(&interactionTestStore{}, requirementAgentService{}, NewService(store, serviceTestAuthorizer{}, serviceTestWorkflow{}), false)
	req := &a2atype.SendMessageRequest{Message: a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("input")), Metadata: map[string]any{apia2a.ExecutionRequirementMetadataKey: base64.StdEncoding.EncodeToString(raw)}}
	got, err := s.resolveSend(serviceTestContext("alice"), types.NamespacedName{Namespace: "team-a", Name: "assistant"}, req)
	if err != nil || store.createInput == nil || !bytes.Equal(got.GetExecutionRequirement(), raw) || !bytes.Equal(store.createInput.GetExecutionRequirement(), raw) {
		t.Fatal("creation lost authority", err)
	}
}
func TestA2AContinuationRequiresExactAuthority(t *testing.T) {
	raw := []byte(`{"opaque":true}`)
	for _, mode := range []string{"exact", "omitted", "changed", "policy-free-injection", "malformed", "message-scope"} {
		t.Run(mode, func(t *testing.T) {
			stored := &apiv1.Session{Id: "0195f6d9-7b2a-7000-8000-000000000001", ContextId: "0195f6d9-7b2a-7000-8000-000000000001", State: apiv1.RuntimeState_RUNTIME_STATE_READY, Operation: apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE, Agent: &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"}, ExecutionRequirement: raw}
			if mode == "policy-free-injection" {
				stored.ExecutionRequirement = nil
			}
			store := &serviceTestStore{getResult: stored}
			s := NewInteractionService(&interactionTestStore{}, nil, NewService(store, serviceTestAuthorizer{}, nil), false)
			req := &a2atype.SendMessageRequest{Message: a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("input")), Metadata: map[string]any{}}
			req.Message.ContextID = stored.ContextId
			value := base64.StdEncoding.EncodeToString(raw)
			switch mode {
			case "omitted":
			case "changed":
				req.Metadata[apia2a.ExecutionRequirementMetadataKey] = base64.StdEncoding.EncodeToString([]byte(`{"opaque":false}`))
			case "malformed":
				req.Metadata[apia2a.ExecutionRequirementMetadataKey] = "!"
			case "message-scope":
				req.Message.SetMeta(apia2a.ExecutionRequirementMetadataKey, value)
			default:
				req.Metadata[apia2a.ExecutionRequirementMetadataKey] = value
			}
			_, err := s.resolveSend(serviceTestContext("alice"), types.NamespacedName{Namespace: "team-a", Name: "assistant"}, req)
			if (err == nil) != (mode == "exact") {
				t.Fatal("incorrect authority decision", mode, err)
			}
			if store.createInput != nil {
				t.Fatal("continuation created compute")
			}
		})
	}
}

func TestA2AGovernedPushRefusesBeforeReservation(t *testing.T) {
	store := &serviceTestStore{}
	s := NewInteractionService(&interactionTestStore{}, nil, NewService(store, serviceTestAuthorizer{}, nil), false)
	req := &a2atype.SendMessageRequest{Message: a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("input")), Metadata: map[string]any{apia2a.ExecutionRequirementMetadataKey: base64.StdEncoding.EncodeToString([]byte(`{"opaque":true}`))}}
	req.Config = &a2atype.SendMessageConfig{PushConfig: &a2atype.PushConfig{URL: "https://example.test/callback"}}
	if _, err := s.resolveSend(serviceTestContext("alice"), types.NamespacedName{Namespace: "team-a", Name: "assistant"}, req); err == nil || store.createInput != nil {
		t.Fatal("delegated callback admitted", err)
	}
}
