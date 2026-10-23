package database

import (
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestGovernedIdleRetainsProtectionAndPermitsDelete(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := t.Context()
	sessionFixture(t, client, ctx, "team-a", "revision", "assistant", "kagent")
	request := newSessionRequest(uuid.NewString(), "assistant", "kagent", "")
	request.ExecutionRequirement = []byte("opaque immutable policy")
	session, _, err := client.CreateSession(ctx, request, uuid.NewString())
	require.NoError(t, err)
	session, err = markSessionReady(ctx, client, session.Id, "native.example")
	require.NoError(t, err)
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("bounded native task"))
	message.TaskID = "governed-idle"
	message.ContextID = session.ContextId
	task := a2a.NewSubmittedTask(message, message)
	task.Status.State = a2a.TaskStateCompleted
	version, err := client.CreateRuntimeTask(ctx, session.Id, taskMutationHash("governed-idle"), task, "")
	require.NoError(t, err)
	require.NoError(t, client.SettleSessionTask(ctx, session.Id, string(task.ID), version))
	work, err := client.ClaimSessionQuiescence(ctx)
	require.NoError(t, err)
	stale := *work
	stale.ExecutorID = uuid.New()
	require.Error(t, client.FinishGovernedSessionIdle(ctx, &stale))
	changed := *work
	changed.Session = proto.CloneOf(work.Session)
	changed.Session.ExecutionRequirement = []byte("changed")
	require.Error(t, client.FinishGovernedSessionIdle(ctx, &changed))
	require.NoError(t, client.FinishGovernedSessionIdle(ctx, work))
	require.NoError(t, client.FinishGovernedSessionIdle(ctx, work))
	retained, err := client.GetSession(ctx, session.Id, session.Creator)
	require.NoError(t, err)
	require.Equal(t, session.ExecutionRequirement, retained.ExecutionRequirement)
	beforeUID, err := client.SessionActorUID(ctx, session.Id)
	require.NoError(t, err)
	require.NotEmpty(t, beforeUID)
	require.Equal(t, session.State, retained.State)
	_, err = client.ClaimSessionQuiescence(ctx)
	require.ErrorIs(t, err, ErrNotFound)
	var snapshot *string
	require.NoError(t, client.db.QueryRow(ctx, "SELECT snapshot_uri FROM session_task_event WHERE sequence=$1", work.Version).Scan(&snapshot))
	require.Nil(t, snapshot)
	require.NoError(t, deleteSession(ctx, client, session.Id))
}
