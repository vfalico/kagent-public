package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
)

type durableSessionActors struct {
	lifecycleTestActors
	operationMu      sync.Mutex
	original         *ateapipb.Actor
	operationID      string
	retired          bool
	lostCreate       bool
	creates, retires int
	terminatedUID    string
}

func (a *durableSessionActors) CreateActorOperation(ctx context.Context, b substrate.ActorBinding, id string, raw []byte, retire bool) (*ateapipb.Actor, error) {
	a.operationMu.Lock()
	defer a.operationMu.Unlock()
	if a.operationID != "" && a.operationID != id {
		return nil, status.Error(codes.FailedPrecondition, "operation changed")
	}
	a.operationID = id
	if retire {
		a.retired = true
		a.retires++
		if a.original == nil {
			return &ateapipb.Actor{}, nil
		}
		return proto.CloneOf(a.original), nil
	}
	if a.retired {
		return nil, status.Error(codes.FailedPrecondition, "retired")
	}
	if a.original != nil {
		return proto.CloneOf(a.original), nil
	}
	actor, err := a.CreateActor(ctx, b.Atespace, b.Name, b.TemplateAtespace, b.TemplateName)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.actors[actorKey(b.Atespace, b.Name)].ExecutionRequirement = append([]byte(nil), raw...)
	a.mu.Unlock()
	actor.ExecutionRequirement = append([]byte(nil), raw...)
	a.original = proto.CloneOf(actor)
	a.creates++
	if a.lostCreate {
		return nil, status.Error(codes.Unavailable, "lost committed Create reply")
	}
	return actor, nil
}
func (a *durableSessionActors) TerminateActor(_ context.Context, s, n, uid string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.terminatedUID = uid
	actor := a.actors[actorKey(s, n)]
	if actor == nil {
		return status.Error(codes.NotFound, "absent")
	}
	if actor.GetMetadata().GetUid() != uid {
		return status.Error(codes.Aborted, "replacement")
	}
	delete(a.actors, actorKey(s, n))
	return nil
}
func TestGovernedSessionLostCreateRetainsOriginalGeneration(t *testing.T) {
	store, initial := lifecycleFixture(t)
	session, _, err := store.CreateSession(t.Context(), &apiv1.Session{Id: uuid.NewString(), Creator: initial.Creator, Agent: initial.Agent, ExecutionRequirement: []byte("opaque session requirement")}, uuid.NewString())
	require.NoError(t, err)
	operation, err := store.BeginSessionOperation(t.Context(), session.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	actors := &durableSessionActors{lifecycleTestActors: lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}, lostCreate: true}
	workflow := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second)
	_, err = workflow.Create(t.Context(), session)
	require.ErrorContains(t, err, "lost committed Create reply")
	require.Equal(t, operation.ID.String(), actors.operationID)
	require.True(t, actors.retired)
	require.Equal(t, "actor-uid", actors.terminatedUID)
	require.Empty(t, actors.actors)
	retained, err := store.GetSessionOperation(t.Context(), session.Id, operation.ID)
	require.NoError(t, err)
	require.Equal(t, operation.ID, retained.ID)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, retained.Instance.Operation)
	require.NotEqual(t, apiv1.RuntimeState_RUNTIME_STATE_READY, retained.Instance.State)
	// A new runtime/controller instance recovers original cleanup, never a fresh
	// friendly-name Actor or new operation. Replacement remains untouched.
	actors.actors[actorKey("team-a", substrate.ActorName(session.Id))] = &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: substrate.ActorName(session.Id), Uid: "replacement"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}}
	restarted := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second)
	_, err = restarted.Create(t.Context(), session)
	require.ErrorContains(t, err, "replacement")
	require.Equal(t, operation.ID.String(), actors.operationID)
	require.Equal(t, "actor-uid", actors.terminatedUID)
	require.Equal(t, "replacement", actors.actors[actorKey("team-a", substrate.ActorName(session.Id))].GetMetadata().GetUid())
	require.Equal(t, 1, actors.creates)
}

func TestGovernedRetiredCreateBecomesDeletedNeverReady(t *testing.T) {
	store, initial := lifecycleFixture(t)
	session, _, err := store.CreateSession(t.Context(), &apiv1.Session{Id: uuid.NewString(), Creator: initial.Creator, Agent: initial.Agent, ExecutionRequirement: []byte("opaque session requirement")}, uuid.NewString())
	require.NoError(t, err)
	operation, err := store.BeginSessionOperation(t.Context(), session.Id, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	actors := &durableSessionActors{lifecycleTestActors: lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}, lostCreate: true}
	first := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second)
	_, err = first.Create(t.Context(), session)
	require.Error(t, err)
	restarted := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second)
	terminal, err := restarted.Create(t.Context(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_DELETED, terminal.State)
	require.Equal(t, apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE, terminal.Operation)
	require.Empty(t, terminal.A2AAuthority)
	require.Empty(t, terminal.PreparedRevision)
	require.Equal(t, "CreationRetired", terminal.GetFailure().GetReason())
	require.Equal(t, 1, actors.creates)
	require.True(t, actors.retired)
	retained, err := store.GetSessionOperation(t.Context(), session.Id, operation.ID)
	require.NoError(t, err)
	require.Equal(t, terminal.State, retained.Instance.State)
	_, err = store.FinishRetiredSessionCreate(t.Context(), session.Id, operation.ID, uuid.New())
	require.Error(t, err)
	_, err = restarted.Create(t.Context(), session)
	require.Error(t, err)
	require.Equal(t, 1, actors.creates)
}
