# Runtime-owned stdio mode

`RUNTIME_CONTROL_STDIO=1` uses the configured A2A port and fixed readiness port
8081 as virtual listeners over stdin/stdout. The owning microVM worker selects the
actual prepared container; it must reserve stdout for protocol frames. Logs stay
on stderr. Native model, tools, descendants and control-plane calls remain real
workload network effects and need explicit policy endpoints; no proxy or TCP reply
exception is introduced. This mode provides no guest-initiated host connection or
command. Without the variable, existing TCP listeners are unchanged. See
`TestOrdinaryA2AStdio` and the byte-identical controlstdio conformance suite.
