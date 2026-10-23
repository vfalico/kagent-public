package substrate

import (
	"bytes"
	"context"
	"fmt"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"strings"
	"testing"
)

type requirementActors struct{ transitionActors }

func (a *requirementActors) CreateActorWithRequirement(ctx context.Context, s, n, ts, tn string, raw []byte) (*ateapipb.Actor, error) {
	actor, err := a.CreateActor(ctx, s, n, ts, tn)
	if err != nil {
		return nil, err
	}
	actor.ExecutionRequirement = append([]byte(nil), raw...)
	a.actor.ExecutionRequirement = append([]byte(nil), raw...)
	return actor, nil
}
func TestOpaqueRequirementFrozenBeforeCompute(t *testing.T) {
	a := &requirementActors{}
	raw := []byte(`{"format":"non-suite-test/v1"}`)
	want := append([]byte(nil), raw...)
	b := ActorBinding{Revision: "rev", Atespace: "team", Name: "instance", TemplateAtespace: "team", TemplateName: "rev"}
	tr, err := PrepareActorTransition(t.Context(), a, b, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, &ActorCreation{ExecutionRequirement: raw})
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = 'X'
	if err = ApplyActorTransition(t.Context(), a, tr); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.actor.GetExecutionRequirement(), want) {
		t.Fatal("source mutation dropped policy")
	}
	if strings.Join(a.calls, ",") != "create,policy" {
		t.Fatal(a.calls)
	}
	b.ExecutionRequirement = want
	_, err = PrepareActorTransition(t.Context(), a, b, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME, nil)
	if err != nil {
		t.Fatal("initial suspended startup refused", err)
	}
	a.actor.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: "retained"}
	if _, err = PrepareActorTransition(t.Context(), a, b, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME, nil); err == nil {
		t.Fatal("warm resume admitted")
	}
	b.ExecutionRequirement = []byte(`{"different":true}`)
	a.actor.Status.ExternalSnapshot = nil
	if _, err = PrepareActorTransition(t.Context(), a, b, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME, nil); err == nil {
		t.Fatal("changed authority admitted")
	}
}
func TestMissingRequirementControlNeverCreatesCompute(t *testing.T) {
	a := &transitionActors{}
	b := ActorBinding{Revision: "rev", Atespace: "team", Name: "instance", TemplateAtespace: "team", TemplateName: "rev"}
	tr, err := PrepareActorTransition(t.Context(), a, b, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, &ActorCreation{ExecutionRequirement: []byte(`{"test":1}`), Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = ApplyActorTransition(t.Context(), a, tr); err == nil || len(a.calls) != 0 {
		t.Fatal("unsupported control released compute", err, a.calls)
	}
}

type exactTerminationActors struct {
	transitionActors
	uid          string
	terminateErr error
}

func (a *exactTerminationActors) TerminateActor(_ context.Context, _, _, uid string) error {
	a.calls = append(a.calls, "terminate")
	a.uid = uid
	return a.terminateErr
}
func TestGovernedDeleteAvoidsSnapshotAndRequiresExactTerminator(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		a := &exactTerminationActors{}
		actor, err := a.CreateActor(t.Context(), "team", "owned", "team", "template")
		if err != nil {
			t.Fatal(err)
		}
		actor.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		actor.ExecutionRequirement = []byte(`{"opaque":true}`)
		a.actor = actor
		a.calls = nil
		var ctl LifecycleClient = a
		if unsupported {
			ctl = &a.transitionActors
		}
		binding := ActorBinding{ExecutionRequirement: actor.ExecutionRequirement, Atespace: "team", Name: "owned", TemplateAtespace: "team", TemplateName: "template"}
		tr, err := PrepareActorTransition(t.Context(), ctl, binding, apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = ApplyActorTransition(t.Context(), ctl, tr)
		if unsupported {
			if err == nil || len(a.calls) != 0 {
				t.Fatal("unsafe fallback", err, a.calls)
			}
		} else if err != nil || strings.Join(a.calls, ",") != "terminate" || a.uid != "actor-uid" {
			t.Fatal(err, a.calls, a.uid)
		}
	}
}

type changedResumeRequirement struct{ requirementActors }

func (a *changedResumeRequirement) ResumeActor(ctx context.Context, s, n string) (*ateapipb.Actor, error) {
	actor, err := a.transitionActors.ResumeActor(ctx, s, n)
	if err == nil {
		actor.ExecutionRequirement = nil
	}
	return actor, err
}
func (a *changedResumeRequirement) ResumeActorExact(ctx context.Context, s, n, uid string) (*ateapipb.Actor, error) {
	if uid != a.actor.GetMetadata().GetUid() {
		return nil, fmt.Errorf("changed uid")
	}
	return a.ResumeActor(ctx, s, n)
}
func TestResumeResultCannotEraseRequirement(t *testing.T) {
	a := &changedResumeRequirement{}
	raw := []byte(`{"opaque":true}`)
	actor, err := a.CreateActorWithRequirement(t.Context(), "team", "owned", "team", "template", raw)
	if err != nil {
		t.Fatal(err)
	}
	b := ActorBinding{Revision: "rev", Atespace: "team", Name: "owned", TemplateAtespace: "team", TemplateName: "template", ExecutionRequirement: raw}
	for _, state := range []ateapipb.ActorState{ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_RESUMING} {
		a.actor.Status.State = state
		if _, err := PrepareActorRetry(t.Context(), a, b, apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, &ActorCreation{ExecutionRequirement: raw}); err == nil {
			t.Fatal("warm creation retry adopted compute", state)
		}
	}
	a.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	for _, operation := range []apiv1.RuntimeOperation{apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE, apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME} {
		a.actor = actor
		a.actor.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		var tr *ActorTransition
		if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE {
			tr, err = PrepareActorRetry(t.Context(), a, b, operation, &ActorCreation{ExecutionRequirement: raw, Resume: true})
		} else {
			tr, err = PrepareActorTransition(t.Context(), a, b, operation, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := ApplyActorTransition(t.Context(), a, tr); err == nil || !strings.Contains(err.Error(), "execution requirement") {
			t.Fatal("resume reply erased authority", operation, err)
		}
	}
}
