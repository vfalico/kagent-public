package session

import (
	"context"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/stretchr/testify/require"
	"testing"
)

type protectedIdleStore struct {
	workflowStore
	called bool
}

func (s *protectedIdleStore) FinishGovernedSessionIdle(context.Context, *database.SessionQuiescence) error {
	s.called = true
	return nil
}
func TestGovernedIdleDoesNotCallComputeOrSnapshot(t *testing.T) {
	store := &protectedIdleStore{}
	w := NewActorWorkflow(store, nil, nil, 0)
	w.quiesceIdleSession(t.Context(), &database.SessionQuiescence{Session: &apiv1.Session{Id: "session", ExecutionRequirement: []byte("opaque")}})
	require.True(t, store.called)
}
