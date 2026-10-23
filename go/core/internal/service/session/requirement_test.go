package session

import (
	"bytes"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"testing"
)

func TestServiceCreateFreezesOpaqueRequirement(t *testing.T) {
	store := &serviceTestStore{}
	s := NewService(store, serviceTestAuthorizer{}, serviceTestWorkflow{})
	raw := []byte(`{"format":"non-suite-test/v1","opaque":{"integer":9007199254740993}}`)
	want := append([]byte(nil), raw...)
	session, err := s.CreateWithRequirement(serviceTestContext("alice"), &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"}, "request-1", "", raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = 'X'
	if !bytes.Equal(session.GetExecutionRequirement(), want) || !bytes.Equal(store.createInput.GetExecutionRequirement(), want) {
		t.Fatal("caller mutation changed frozen authority")
	}
}
func TestServiceRejectsMalformedRequirementBeforeReservation(t *testing.T) {
	store := &serviceTestStore{}
	s := NewService(store, serviceTestAuthorizer{}, serviceTestWorkflow{})
	_, err := s.CreateWithRequirement(serviceTestContext("alice"), &apiv1.ResourceReference{Namespace: "team-a", Name: "assistant"}, "request-1", "", []byte(`{"missing"`))
	if err == nil || store.createInput != nil {
		t.Fatal("malformed input reserved execution")
	}
}
