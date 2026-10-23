package a2agateway

import (
	"bytes"
	"encoding/base64"
	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/stretchr/testify/require"
	"testing"
)

// Real HTTP/gRPC public transports and real Session services; storage and
// runtime are test doubles. This proves carrier routing, not enforcement.
func TestPublicA2ARequirementAcrossTransports(t *testing.T) {
	for _, protocol := range []a2atype.TransportProtocol{a2atype.TransportProtocolJSONRPC, a2atype.TransportProtocolGRPC} {
		t.Run(string(protocol), func(t *testing.T) {
			raw := []byte(`{ "format":"non-suite/v1", "opaque":9007199254740993 }`)
			store := &gatewayTestStore{created: map[string]*apiv1.Session{}}
			runtime := &gatewayTestRuntime{}
			gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, "")
			transport := newGatewayTestTransport(t, startCoreTestServer(t, gateway, nil), protocol)
			tenant := ""
			if protocol == a2atype.TransportProtocolGRPC {
				tenant = gatewayTestAgent
			}
			params := a2aclient.ServiceParams{"authorization": {"Bearer valid"}}
			request := func(id, context string, requirement []byte) *a2atype.SendMessageRequest {
				message := a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("hello"))
				message.ID = id
				message.ContextID = context
				req := &a2atype.SendMessageRequest{Tenant: tenant, Message: message, Config: &a2atype.SendMessageConfig{ReturnImmediately: true}}
				if requirement != nil {
					req.Metadata = map[string]any{apia2a.ExecutionRequirementMetadataKey: base64.StdEncoding.EncodeToString(requirement)}
				}
				return req
			}
			first, err := transport.SendMessage(t.Context(), params, request("first", "", raw))
			require.NoError(t, err)
			task := first.(*a2atype.Task)
			require.True(t, bytes.Equal(raw, store.session.GetExecutionRequirement()))
			require.Equal(t, 1, runtime.sendCalls)
			// Accepted retry still must present the exact authority before replay.
			store.reserveErr, store.replay, store.task = database.ErrMessageAccepted, task, task
			_, err = transport.SendMessage(t.Context(), params, request("first", "", raw))
			require.NoError(t, err)
			require.Equal(t, 1, runtime.sendCalls)
			for _, tc := range []struct {
				name string
				req  *a2atype.SendMessageRequest
			}{
				{"retry omission", request("first", "", nil)},
				{"retry changed", request("first", "", []byte(`{"opaque":false}`))},
				{"continuation omission", request("next", task.ContextID, nil)},
				{"continuation changed", request("next", task.ContextID, []byte(`{"opaque":false}`))},
			} {
				before := runtime.sendCalls
				_, err = transport.SendMessage(t.Context(), params, tc.req)
				require.Error(t, err, tc.name)
				require.Equal(t, before, runtime.sendCalls, tc.name)
			}
			// Separate callback registration is a delegated controller effect too.
			_, err = transport.CreateTaskPushConfig(t.Context(), params, &a2atype.PushConfig{Tenant: tenant, TaskID: task.ID, URL: "https://example.test/callback"})
			require.Error(t, err)
			require.Len(t, store.created, 1)
		})
	}
}
