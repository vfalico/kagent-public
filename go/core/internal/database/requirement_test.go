package database

import (
	"bytes"
	"context"
	"errors"
	"github.com/google/uuid"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"testing"
)

// Real PostgreSQL persistence/idempotency, not mocked storage or guest proof.
func TestFrozenRequirementSurvivesDatabaseAndRejectsOmittedRetry(t *testing.T) {
	pool := setupTestDB(t)
	c := NewClient(pool, "public")
	ctx := context.Background()
	sessionFixture(t, c, ctx, "team-a", "revision-1", "assistant", "test")
	req := newSessionRequest(uuid.NewString(), "assistant", "test", "governed")
	raw := []byte(`{"opaque":{"integer":9007199254740993}}`)
	req.ExecutionRequirement = append([]byte(nil), raw...)
	got, created, err := c.CreateSession(ctx, req, "governed-request")
	if err != nil || !created {
		t.Fatal(got, created, err)
	}
	fresh := NewClient(pool, "public")
	read, err := fresh.GetSession(ctx, got.Id, "alice")
	if err != nil || !bytes.Equal(read.GetExecutionRequirement(), raw) {
		t.Fatal(read, err)
	}
	retry := newSessionRequest(uuid.NewString(), "assistant", "test", "retry")
	if _, _, err := fresh.CreateSession(ctx, retry, "governed-request"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("omitted requirement accepted", err)
	}
	retry.ExecutionRequirement = append([]byte(nil), raw...)
	replay, created, err := fresh.CreateSession(ctx, retry, "governed-request")
	if err != nil || created || replay.Id != got.Id {
		t.Fatal(replay, created, err)
	}
	ready, err := markSessionReady(ctx, c, got.Id, "actor.example")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ready.ExecutionRequirement, raw) {
		t.Fatal("lifecycle erased requirement")
	}
	_, _, err = fresh.ReserveSessionCheckpoint(ctx, &apiv1.Checkpoint{Id: uuid.NewString(), SessionId: got.Id, HeadTaskId: "not-needed"}, "alice", "checkpoint-governed")
	if !errors.Is(err, ErrFailedPrecondition) {
		t.Fatal("governed checkpoint reserved without gate", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM session_checkpoint WHERE source_session_id=$1`, got.Id).Scan(&n); err != nil || n != 0 {
		t.Fatal("side effect before gate", n, err)
	}
}
