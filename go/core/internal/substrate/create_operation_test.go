package substrate

import (
	"context"
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type operationActors struct {
	guardedExactActors
	original                           *ateapipb.Actor
	retired                            bool
	createLost                         bool
	resumeLost                         bool
	cleanupFailure                     bool
	creates, retirements, terminations int
	terminatedUID                      string
}

func (a *operationActors) CreateActorOperation(ctx context.Context, b ActorBinding, id string, raw []byte, retire bool) (*ateapipb.Actor, error) {
	if id != "original-operation" {
		return nil, errors.New("operation changed")
	}
	if retire {
		a.retirements++
		a.retired = true
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
	a.creates++
	actor, err := a.CreateActorWithRequirement(ctx, b.Atespace, b.Name, b.TemplateAtespace, b.TemplateName, raw)
	if err != nil {
		return nil, err
	}
	a.original = proto.CloneOf(actor)
	if a.createLost {
		return nil, status.Error(codes.Unavailable, "lost committed Create reply")
	}
	return actor, nil
}
func (a *operationActors) ResumeActorExact(ctx context.Context, s, n, uid string) (*ateapipb.Actor, error) {
	actor, err := a.guardedExactActors.ResumeActorExact(ctx, s, n, uid)
	if err == nil && a.resumeLost {
		return nil, status.Error(codes.Unavailable, "lost successful Resume reply")
	}
	return actor, err
}
func (a *operationActors) TerminateActor(_ context.Context, _, _, uid string) error {
	a.terminations++
	a.terminatedUID = uid
	if a.cleanupFailure {
		return status.Error(codes.Unavailable, "injected partial cleanup")
	}
	if a.actor == nil {
		return status.Error(codes.NotFound, "absent")
	}
	if a.actor.GetMetadata().GetUid() != uid {
		return status.Error(codes.Aborted, "replacement must remain")
	}
	a.actor = nil
	return nil
}
func operationBinding() ActorBinding {
	return ActorBinding{Revision: "rev", Atespace: "team", Name: "owned", TemplateAtespace: "team", TemplateName: "template", ExecutionRequirement: []byte("opaque requirement")}
}
func operationCreation() *ActorCreation {
	return &ActorCreation{OperationID: "original-operation", ExecutionRequirement: []byte("opaque requirement"), Resume: true}
}
func TestDurableGovernedCreateCutsRetainOriginalCleanup(t *testing.T) {
	for _, cut := range []string{"none", "create", "resume"} {
		t.Run(cut, func(t *testing.T) {
			a := &operationActors{createLost: cut == "create", resumeLost: cut == "resume"}
			tr, err := PrepareActorTransition(t.Context(), a, operationBinding(), apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, operationCreation())
			if err != nil {
				t.Fatal(err)
			}
			err = ApplyActorTransition(t.Context(), a, tr)
			if cut == "none" {
				if err != nil || a.retired || a.actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
					t.Fatal("allowed control", err)
				}
			} else {
				if err == nil || !a.retired || a.actor != nil || a.terminatedUID != "actor-uid" {
					t.Fatal("lost original cleanup", err, a.retired, a.terminatedUID)
				}
			}
			if a.creates != 1 {
				t.Fatal("duplicate create", a.creates)
			}
		})
	}
}
func TestDurableGovernedRetryIsCleanupOnly(t *testing.T) {
	for _, exists := range []bool{false, true} {
		a := &operationActors{}
		if exists {
			_, err := a.CreateActorOperation(t.Context(), operationBinding(), "original-operation", []byte("opaque requirement"), false)
			if err != nil {
				t.Fatal(err)
			}
			a.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		}
		tr, err := PrepareActorRetry(t.Context(), a, operationBinding(), apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, operationCreation())
		if err != nil {
			t.Fatal(err)
		}
		if err = ApplyActorTransition(t.Context(), a, tr); err == nil {
			t.Fatal("retry released")
		}
		if !a.retired || a.actor != nil || a.exactCalls != 0 {
			t.Fatal("retry did not retire", a)
		}
		if _, err = a.CreateActorOperation(t.Context(), operationBinding(), "original-operation", []byte("opaque requirement"), false); status.Code(err) != codes.FailedPrecondition {
			t.Fatal("delayed create won", err)
		}
	}
}
func TestDurableGovernedCleanupRetriesWithoutReplacementDeletion(t *testing.T) {
	a := &operationActors{createLost: true, cleanupFailure: true}
	tr, err := PrepareActorTransition(t.Context(), a, operationBinding(), apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, operationCreation())
	if err != nil {
		t.Fatal(err)
	}
	if err = ApplyActorTransition(t.Context(), a, tr); err == nil || a.actor == nil || !a.retired {
		t.Fatal("partial cleanup hidden", err)
	}
	originalUID := a.original.GetMetadata().GetUid()
	a.actor.Metadata.Uid = "replacement"
	a.cleanupFailure = false
	retry, err := PrepareActorRetry(t.Context(), a, operationBinding(), apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, operationCreation())
	if err != nil {
		t.Fatal(err)
	}
	if err = ApplyActorTransition(t.Context(), a, retry); err == nil {
		t.Fatal("replacement cleanup reported success")
	}
	if a.actor.GetMetadata().GetUid() != "replacement" || a.terminatedUID != originalUID {
		t.Fatal("deleted replacement")
	}
	// Original is now absent; exact owner reports NotFound rather than switching.
	a.actor = nil
	if err = ApplyActorTransition(t.Context(), a, retry); err == nil {
		t.Fatal("cleanup-only retry became release")
	}
	if a.terminatedUID != originalUID || a.creates != 1 {
		t.Fatal("owner changed")
	}
}

type createOperationRPC struct {
	ateapipb.ControlClient
	request *ateapipb.CreateActorRequest
}

func (f *createOperationRPC) CreateActor(_ context.Context, in *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.request = proto.CloneOf(in)
	return &ateapipb.Actor{}, nil
}
func TestCreateOperationClientCarriesFrozenOriginalIntent(t *testing.T) {
	f := &createOperationRPC{}
	c := &Client{ControlClient: f}
	raw := []byte("opaque")
	if _, err := c.CreateActorOperation(t.Context(), operationBinding(), "original-operation", raw, true); err != nil {
		t.Fatal(err)
	}
	raw[0] = 'X'
	if f.request.GetOperationId() != "original-operation" || !f.request.GetRetireOperation() || string(f.request.GetActor().GetExecutionRequirement()) != "opaque" || f.request.GetActor().GetMetadata().GetName() != "owned" {
		t.Fatal("lost frozen operation", f.request)
	}
}
