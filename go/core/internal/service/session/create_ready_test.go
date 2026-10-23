package session

import (
	"context"
	"testing"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
)

type createOutcomeWorkflow struct {
	serviceTestWorkflow
	state     apiv1.RuntimeState
	operation apiv1.RuntimeOperation
	absent    bool
}

func (w createOutcomeWorkflow) Create(_ context.Context, s *apiv1.Session) (*apiv1.Session, error) {
	if w.absent {
		return nil, nil
	}
	s.State = w.state
	s.Operation = w.operation
	return s, nil
}

func TestServiceCreateRequiresSettledLiveOutcome(t *testing.T) {
	for _, state := range []apiv1.RuntimeState{
		apiv1.RuntimeState_RUNTIME_STATE_UNSPECIFIED,
		apiv1.RuntimeState_RUNTIME_STATE_CREATING,
		apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED,
		apiv1.RuntimeState_RUNTIME_STATE_FAILED,
		apiv1.RuntimeState_RUNTIME_STATE_DELETING,
		apiv1.RuntimeState_RUNTIME_STATE_DELETED,
		apiv1.RuntimeState_RUNTIME_STATE_READY,
	} {
		t.Run(state.String(), func(t *testing.T) {
			store := &serviceTestStore{}
			service := NewService(store, serviceTestAuthorizer{}, createOutcomeWorkflow{state: state, operation: apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE})
			result, err := service.Create(serviceTestContext("alice"), &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"}, "stable-request", "")
			if state == apiv1.RuntimeState_RUNTIME_STATE_READY || state == apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED {
				if err != nil || result == nil {
					t.Fatalf("ready outcome: %v, %v", result, err)
				}
			} else if result != nil || !serviceerrors.IsCode(err, serviceerrors.CodeFailedPrecondition) {
				t.Fatalf("non-ready outcome was not refused: %v, %v", result, err)
			}
			if store.requestID != "stable-request" || store.createInput == nil {
				t.Fatal("durable reservation lost")
			}
		})
	}
	for _, tc := range []struct {
		name string
		w    createOutcomeWorkflow
	}{
		{"absent", createOutcomeWorkflow{absent: true}},
		{"pending", createOutcomeWorkflow{state: apiv1.RuntimeState_RUNTIME_STATE_READY, operation: apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := NewService(&serviceTestStore{}, serviceTestAuthorizer{}, tc.w)
			result, err := service.Create(serviceTestContext("alice"), &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"}, "stable-request", "")
			if result != nil || !serviceerrors.IsCode(err, serviceerrors.CodeFailedPrecondition) {
				t.Fatalf("unsafe create outcome: %v, %v", result, err)
			}
		})
	}
}
