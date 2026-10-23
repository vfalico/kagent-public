package substrate

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exact-resume double models replacement after read-only preparation and before
// the owning mutation. It never starts the replacement; legacy Resume panics.
type guardedExactActors struct {
	requirementActors
	exactCalls  int
	suppliedUID string
}

func (a *guardedExactActors) ResumeActor(context.Context, string, string) (*ateapipb.Actor, error) {
	panic("governed name-only resume")
}
func (a *guardedExactActors) ResumeActorExact(ctx context.Context, s, n, uid string) (*ateapipb.Actor, error) {
	a.exactCalls++
	a.suppliedUID = uid
	if a.actor.GetMetadata().GetUid() != uid {
		return nil, status.Error(codes.Aborted, "replacement")
	}
	return a.transitionActors.ResumeActor(ctx, s, n)
}
func TestGovernedResumeRequiresOwningIncarnation(t *testing.T) {
	for _, operation := range []apiv1.RuntimeOperation{apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME} {
		for _, replacement := range []bool{false, true} {
			a := &guardedExactActors{}
			raw := []byte("opaque requirement")
			_, err := a.CreateActorWithRequirement(t.Context(), "team", "owned", "team", "template", raw)
			if err != nil {
				t.Fatal(err)
			}
			b := ActorBinding{Revision: "rev", Atespace: "team", Name: "owned", TemplateAtespace: "team", TemplateName: "template", ExecutionRequirement: raw}
			var tr *ActorTransition
			if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE {
				tr, err = PrepareActorRetry(t.Context(), a, b, operation, &ActorCreation{ExecutionRequirement: raw, Resume: true})
			} else {
				tr, err = PrepareActorTransition(t.Context(), a, b, operation, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			a.calls = nil
			if replacement {
				a.actor.Metadata.Uid = "replacement"
			}
			err = ApplyActorTransition(t.Context(), a, tr)
			if a.suppliedUID != "actor-uid" || a.exactCalls != 1 {
				t.Fatal("lost original identity", a.suppliedUID, a.exactCalls)
			}
			if replacement {
				if err == nil || a.actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
					t.Fatal("replacement started", err)
				}
			} else if err != nil || a.actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
				t.Fatal("exact control failed", err)
			}
		}
	}
}
func TestGovernedMissingExactResumeNeverFallsBack(t *testing.T) {
	a := &requirementActors{}
	raw := []byte("opaque")
	_, err := a.CreateActorWithRequirement(t.Context(), "team", "owned", "team", "template", raw)
	if err != nil {
		t.Fatal(err)
	}
	b := ActorBinding{Revision: "rev", Atespace: "team", Name: "owned", TemplateAtespace: "team", TemplateName: "template", ExecutionRequirement: raw}
	tr, err := PrepareActorTransition(t.Context(), a, b, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.calls = nil
	if err = ApplyActorTransition(t.Context(), a, tr); err == nil {
		t.Fatal("unsupported exact resume admitted")
	}
	for _, call := range a.calls {
		if call == "resume" {
			t.Fatal("unsafe fallback")
		}
	}
}

type exactResumeRPC struct {
	ateapipb.ControlClient
	request *ateapipb.ResumeActorRequest
}

func (f *exactResumeRPC) ResumeActor(_ context.Context, in *ateapipb.ResumeActorRequest, _ ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.request = in
	return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{}}, nil
}
func TestExactResumeClientCarriesUID(t *testing.T) {
	f := &exactResumeRPC{}
	c := &Client{ControlClient: f}
	if _, err := c.ResumeActorExact(t.Context(), "team", "owned", ""); err == nil || f.request != nil {
		t.Fatal("empty exact identity issued")
	}
	if _, err := c.ResumeActorExact(t.Context(), "team", "owned", "original"); err != nil {
		t.Fatal(err)
	}
	if f.request.GetActorUid() != "original" || f.request.GetActor().GetAtespace() != "team" || f.request.GetActor().GetName() != "owned" {
		t.Fatal("lost precondition", f.request)
	}
}
