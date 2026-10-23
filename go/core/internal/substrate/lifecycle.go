package substrate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
)

type LifecycleClient interface {
	GetActor(context.Context, string, string) (*ateapipb.Actor, error)
	CreateActor(context.Context, string, string, string, string) (*ateapipb.Actor, error)
	CreateActorFromTag(context.Context, string, string, string, string, string, string) (*ateapipb.Actor, error)
	EnsureActorEgressPolicy(context.Context, string, string, string) error
	ResumeActor(context.Context, string, string) (*ateapipb.Actor, error)
	SuspendActor(context.Context, string, string) (*ateapipb.Actor, error)
	DeleteActor(context.Context, string, string) error
}

var _ LifecycleClient = (*Client)(nil)

type ActorBinding struct {
	Revision             string
	ExecutionRequirement []byte
	Atespace             string
	Name                 string
	TemplateAtespace     string
	TemplateName         string
}

type ActorSnapshot struct {
	Tag          *ateapipb.ObjectRef
	URI          string
	ContentScope ateapipb.SnapshotContentScope
}

// ActorCreation describes runtime startup, independent of the owning API.
// Agent creation stays suspended; a standalone guest starts serving immediately.
type ActorCreation struct {
	// OperationID is the owning Session lifecycle generation, persisted before RPC.
	OperationID          string
	ExecutionRequirement []byte
	Snapshot             *ActorSnapshot
	Resume               bool
}

type ActorTransition struct {
	binding     ActorBinding
	operation   apiv1.RuntimeOperation
	actor       *ateapipb.Actor
	creation    *ActorCreation
	cleanupOnly bool
}

// ActorUID returns the observed runtime identity, including after creation.
func (t *ActorTransition) ActorUID() string {
	return t.actor.GetMetadata().GetUid()
}

// PrepareActorTransition performs read-only checks before the store's durable
// claim. It never adopts an existing Actor for an unissued creation.
func PrepareActorTransition(ctx context.Context, actors LifecycleClient, binding ActorBinding, operation apiv1.RuntimeOperation, creation *ActorCreation) (*ActorTransition, error) {
	return prepareActorTransition(ctx, actors, binding, operation, creation, false)
}

// PrepareActorRetry resumes previously issued intent. Substrate serializes
// lifecycle mutations with its Actor lease and resumes incomplete workflows.
// The immutable runtime name and pinned template identify an existing creation.
func PrepareActorRetry(ctx context.Context, actors LifecycleClient, binding ActorBinding, operation apiv1.RuntimeOperation, creation *ActorCreation) (*ActorTransition, error) {
	return prepareActorTransition(ctx, actors, binding, operation, creation, true)
}

func prepareActorTransition(ctx context.Context, actors LifecycleClient, binding ActorBinding, operation apiv1.RuntimeOperation, creation *ActorCreation, reconcile bool) (*ActorTransition, error) {
	if operation < apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE || operation > apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE {
		return nil, fmt.Errorf("invalid actor lifecycle operation %s", operation)
	}
	if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE {
		if creation == nil || binding.Revision == "" {
			return nil, fmt.Errorf("actor creation requires a prepared revision")
		}
		if snapshot := creation.Snapshot; snapshot != nil && (snapshot.Tag.GetAtespace() == "" || snapshot.Tag.GetName() == "" || snapshot.URI == "" || snapshot.ContentScope != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA) {
			return nil, fmt.Errorf("actor restoration requires a retained DATA snapshot")
		}
	}
	if creation != nil {
		copy := *creation
		copy.ExecutionRequirement = append([]byte(nil), creation.ExecutionRequirement...)
		creation = &copy
		if len(creation.ExecutionRequirement) > 0 && creation.Snapshot != nil {
			return nil, fmt.Errorf("governed snapshot fork is not supported; no compute created")
		}
	}
	// Governed public Session creation uses the durable owning operation, never
	// friendly-name adoption. Retry is cleanup-only: an uncertain earlier issue
	// must not acquire a new release authority, even if current state looks held.
	if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && creation != nil && len(creation.ExecutionRequirement) > 0 && creation.OperationID != "" && reconcile {
		if _, ok := actors.(ActorCreateOperations); !ok {
			return nil, fmt.Errorf("owning durable create operations unsupported")
		}
		return &ActorTransition{binding: binding, operation: operation, creation: creation, cleanupOnly: true}, nil
	}
	actor, err := actors.GetActor(ctx, binding.Atespace, binding.Name)
	if status.Code(err) == codes.NotFound {
		if operation != apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && operation != apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE {
			return nil, err
		}
		actor, err = nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get Actor %s/%s: %w", binding.Atespace, binding.Name, err)
	}
	if actor != nil {
		if operation != apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE && operation != apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && string(binding.ExecutionRequirement) != string(actor.GetExecutionRequirement()) {
			return nil, fmt.Errorf("persisted Actor execution requirement changed")
		}
		if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && creation != nil && string(actor.GetExecutionRequirement()) != string(creation.ExecutionRequirement) {
			return nil, fmt.Errorf("exact Actor requirement changed")
		}
		if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME && len(actor.GetExecutionRequirement()) > 0 && (actor.GetStatus().GetExternalSnapshot().GetSnapshotUri() != "" || actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED) {
			return nil, fmt.Errorf("governed warm resume is not supported; no compute resumed")
		}
		if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && reconcile && creation != nil && len(creation.ExecutionRequirement) > 0 && actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			return nil, fmt.Errorf("governed creation retry cannot adopt running or transitioning compute")
		}
		if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && !reconcile {
			return nil, fmt.Errorf("refuse to adopt existing Actor %s/%s while creation is still pending", binding.Atespace, binding.Name)
		}
		if !binding.Matches(actor) {
			return nil, fmt.Errorf("actor %s/%s identity or template changed", binding.Atespace, binding.Name)
		}
		if operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME || operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND {
			switch actor.GetStatus().GetState() {
			case ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
				ateapipb.ActorState_ACTOR_STATE_PAUSED, ateapipb.ActorState_ACTOR_STATE_RESUMING, ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
			default:
				return nil, fmt.Errorf("actor cannot perform %s from %s", operation, actor.GetStatus().GetState())
			}
		}
	}
	return &ActorTransition{binding: binding, operation: operation, actor: actor, creation: creation}, nil
}

// ApplyActorTransition requires durable execution authority. Errors retain the
// claim's durable intent and pins. Another bounded attempt can retry that intent.
func ApplyActorTransition(ctx context.Context, actors LifecycleClient, transition *ActorTransition) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	binding, actor := transition.binding, transition.actor
	switch transition.operation {
	case apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE:
		creation := transition.creation
		if len(creation.ExecutionRequirement) > 0 && creation.OperationID != "" {
			owner, ok := actors.(ActorCreateOperations)
			if !ok {
				return fmt.Errorf("owning durable create operations unsupported")
			}
			if transition.cleanupOnly {
				if e := retireActorCreateOperation(ctx, actors, owner, binding, creation); e != nil {
					return e
				}
				return ErrCreateRetired
			}
			// Install cleanup before issuing Create, including a reply lost before UID.
			// Retire serializes with Create and Resume in the compute owner's store.
			var complete bool
			defer func() {
				if !complete {
					cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
					defer cancel()
					cleanupErr := retireActorCreateOperation(cleanupCtx, actors, owner, binding, creation)
					if cleanupErr != nil {
						err = errors.Join(err, cleanupErr)
					}
				}
			}()
			actor, err = owner.CreateActorOperation(ctx, binding, creation.OperationID, creation.ExecutionRequirement, false)
			if err != nil {
				return err
			}
			if string(actor.GetExecutionRequirement()) != string(creation.ExecutionRequirement) {
				return fmt.Errorf("created Actor requirement mismatch")
			}
			if err = transition.checkResult(actor, ateapipb.ActorState_ACTOR_STATE_SUSPENDED); err != nil {
				return err
			}
			transition.actor = actor
			if err = actors.EnsureActorEgressPolicy(ctx, binding.Atespace, binding.Name, binding.Revision); err != nil {
				return err
			}
			if creation.Resume {
				actor, err = resumeTransitionActor(ctx, actors, transition, actor)
				if err != nil {
					return err
				}
				if err = transition.checkResult(actor, ateapipb.ActorState_ACTOR_STATE_RUNNING); err != nil {
					return err
				}
			}
			transition.actor = actor
			if err = ctx.Err(); err != nil {
				return err
			}
			complete = true
			return nil
		}
		if actor == nil {
			if len(creation.ExecutionRequirement) > 0 {
				ctl, ok := actors.(interface {
					CreateActorWithRequirement(context.Context, string, string, string, string, []byte) (*ateapipb.Actor, error)
				})
				if !ok {
					return fmt.Errorf("owning control lacks execution requirement support")
				}
				actor, err = ctl.CreateActorWithRequirement(ctx, binding.Atespace, binding.Name, binding.TemplateAtespace, binding.TemplateName, creation.ExecutionRequirement)
			} else if creation.Snapshot == nil {
				actor, err = actors.CreateActor(ctx, binding.Atespace, binding.Name, binding.TemplateAtespace, binding.TemplateName)
			} else {
				tag := creation.Snapshot.Tag
				actor, err = actors.CreateActorFromTag(ctx, binding.Atespace, binding.Name, binding.TemplateAtespace, binding.TemplateName, tag.Atespace, tag.Name)
			}
			if err == nil {
				if string(actor.GetExecutionRequirement()) != string(creation.ExecutionRequirement) {
					return fmt.Errorf("created Actor requirement mismatch")
				}
				err = transition.checkResult(actor, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
				if err == nil {
					transition.actor = actor
				}
			}
		}
		if err == nil && creation.Snapshot != nil {
			snapshot := creation.Snapshot
			observed := actor.GetStatus().GetExternalSnapshot()
			if !proto.Equal(actor.GetSourceTag(), snapshot.Tag) || observed.GetSnapshotUri() != snapshot.URI || observed.GetContentScope() != snapshot.ContentScope {
				err = fmt.Errorf("restored Actor does not match the retained checkpoint")
			}
		}
		if err == nil {
			err = actors.EnsureActorEgressPolicy(ctx, binding.Atespace, binding.Name, binding.Revision)
		}
		if err == nil && creation.Resume && actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
			actor, err = resumeTransitionActor(ctx, actors, transition, actor)
			if err == nil {
				err = transition.checkResult(actor, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			}
		}
	case apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME:
		if err := actors.EnsureActorEgressPolicy(ctx, binding.Atespace, binding.Name, binding.Revision); err != nil {
			return err
		}
		actor, err = resumeTransitionActor(ctx, actors, transition, actor)
		if err == nil {
			err = transition.checkResult(actor, ateapipb.ActorState_ACTOR_STATE_RUNNING)
		}
	case apiv1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND:
		actor, err = actors.SuspendActor(ctx, binding.Atespace, binding.Name)
		if err == nil {
			err = transition.checkResult(actor, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
		}
		if err == nil {
			snapshot := actor.GetStatus().GetExternalSnapshot()
			if snapshot.GetSnapshotUri() == "" || snapshot.GetContentScope() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
				err = fmt.Errorf("suspended Actor requires an external DATA snapshot")
			}
		}
	case apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE:
		if actor != nil && len(binding.ExecutionRequirement) > 0 {
			terminator, ok := actors.(interface {
				TerminateActor(context.Context, string, string, string) error
			})
			if !ok {
				return fmt.Errorf("governed exact destructive termination unsupported")
			}
			err = terminator.TerminateActor(ctx, binding.Atespace, binding.Name, actor.GetMetadata().GetUid())
			if status.Code(err) == codes.NotFound {
				err = nil
			}
			break
		}
		if actor != nil {
			switch actor.GetStatus().GetState() {
			case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_CRASHED, ateapipb.ActorState_ACTOR_STATE_DELETING:
			default:
				actor, err = actors.SuspendActor(ctx, binding.Atespace, binding.Name)
				if err == nil {
					err = transition.checkResult(actor, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
				}
			}
			if err == nil {
				err = actors.DeleteActor(ctx, binding.Atespace, binding.Name)
				if status.Code(err) == codes.NotFound {
					err = nil
				}
			}
		}
	}
	if err != nil {
		return fmt.Errorf("perform %s on Actor %s/%s: %w", transition.operation, binding.Atespace, binding.Name, err)
	}
	return nil
}

// resumeTransitionActor requires an exact owning-control precondition for every
// governed start. Never fall back to a name-only mutation, even if a later
// result check would notice the replacement. Ordinary callers remain unchanged.
func resumeTransitionActor(ctx context.Context, actors LifecycleClient, transition *ActorTransition, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	binding := transition.binding
	requirement := binding.ExecutionRequirement
	if transition.creation != nil {
		requirement = transition.creation.ExecutionRequirement
	}
	if len(requirement) == 0 {
		return actors.ResumeActor(ctx, binding.Atespace, binding.Name)
	}
	exact, ok := actors.(interface {
		ResumeActorExact(context.Context, string, string, string) (*ateapipb.Actor, error)
	})
	if !ok || actor.GetMetadata().GetUid() == "" {
		return nil, fmt.Errorf("governed exact resume unsupported")
	}
	return exact.ResumeActorExact(ctx, binding.Atespace, binding.Name, actor.GetMetadata().GetUid())
}

// Matches checks that the actor belongs to the expected runtime and template.
func (b ActorBinding) Matches(actor *ateapipb.Actor) bool {
	return actor.GetMetadata().GetName() == b.Name && actor.GetMetadata().GetAtespace() == b.Atespace && actor.GetMetadata().GetUid() != "" &&
		actor.GetActorTemplate().GetAtespace() == b.TemplateAtespace && actor.GetActorTemplate().GetName() == b.TemplateName
}

func (t *ActorTransition) checkResult(actor *ateapipb.Actor, state ateapipb.ActorState) error {
	expected := t.binding.ExecutionRequirement
	if t.operation == apiv1.RuntimeOperation_RUNTIME_OPERATION_CREATE && t.creation != nil {
		expected = t.creation.ExecutionRequirement
	}
	if string(actor.GetExecutionRequirement()) != string(expected) {
		return fmt.Errorf("Actor result changed exact execution requirement")
	}
	if !t.binding.Matches(actor) || actor.GetStatus().GetState() != state || (t.actor != nil && actor.GetMetadata().GetUid() != t.actor.GetMetadata().GetUid()) {
		return fmt.Errorf("actor returned unexpected identity or state")
	}
	return nil
}

// ActorCreateOperations uses the existing owning Control transaction and native
// exact termination; it neither interprets policy nor operates the enforcer.
type ActorCreateOperations interface {
	CreateActorOperation(context.Context, ActorBinding, string, []byte, bool) (*ateapipb.Actor, error)
	TerminateActor(context.Context, string, string, string) error
}

func retireActorCreateOperation(ctx context.Context, actors LifecycleClient, owner ActorCreateOperations, b ActorBinding, c *ActorCreation) error {
	original, err := owner.CreateActorOperation(ctx, b, c.OperationID, c.ExecutionRequirement, true)
	if err != nil {
		return fmt.Errorf("retire original create operation: %w", err)
	}
	uid := original.GetMetadata().GetUid()
	if uid == "" {
		return nil
	} // confirmed tombstone: no creating RPC can later win
	if original.GetMetadata().GetAtespace() != b.Atespace || original.GetMetadata().GetName() != b.Name || !b.Matches(original) || string(original.GetExecutionRequirement()) != string(c.ExecutionRequirement) {
		return fmt.Errorf("retired original create identity mismatch")
	}
	err = owner.TerminateActor(ctx, b.Atespace, b.Name, uid)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("terminate retired original create: %w", err)
	}
	return nil
}

// ErrCreateRetired is cleanup-only: owning retirement and exact termination
// finished. It must never be interpreted as READY or activation authority.
var ErrCreateRetired = errors.New("uncertain governed create operation retired; no execution authority")
