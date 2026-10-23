package database

import (
	"github.com/google/uuid"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSessionActorUIDTracksPersistedLifecycle(t *testing.T) {
	c := NewClient(setupTestDB(t), "public")
	sessionFixture(t, c, t.Context(), "team-a", "revision", "assistant", "kagent")
	s, _, err := c.CreateSession(t.Context(), newSessionRequest(uuid.NewString(), "assistant", "kagent", "conversation"), "identity-test")
	require.NoError(t, err)
	_, err = c.SessionActorUID(t.Context(), s.Id)
	require.Error(t, err, "creation has no runtime identity")
	_, err = finishSessionOperation(t.Context(), c, s.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, "runtime.example")
	require.NoError(t, err)
	uid, err := c.SessionActorUID(t.Context(), s.Id)
	require.NoError(t, err)
	require.Equal(t, "actor-"+s.Id, uid)
	require.NoError(t, deleteSession(t.Context(), c, s.Id))
	_, err = c.SessionActorUID(t.Context(), s.Id)
	require.Error(t, err, "tombstone cannot route")
	_, err = c.SessionActorUID(t.Context(), uuid.NewString())
	require.Error(t, err)
}
