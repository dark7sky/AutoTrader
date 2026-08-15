# `kis-ai-extensions` review

## Conclusion

`koreainvestment/kis-ai-extensions` is an AI coding-agent extension/plugin. It adds agent commands, skills, hooks, and MCP-oriented configuration; it is not a Python runtime library for this application's market-data and execution process.

It must not be installed into this repository's trading hot path. In particular, its `kis-order-executor` pattern gives an AI-driven workflow a path toward KIS order requests. Agent hooks are useful operator guardrails, but a hook can fail open or miss a direct Python/HTTP call, so it cannot be the final execution control.

## Ideas worth absorbing

- Keep REST access-token issuance at `/oauth2/tokenP` and WebSocket approval-key issuance at `/oauth2/Approval` as separate operations.
- Keep demo/VPS and real/production environments explicitly separated and validate the selected environment before any future order integration.
- Redact app keys, app secrets, bearer tokens, and approval keys in logs and CLI output.
- Expose authentication status as safe metadata such as environment, authenticated state, and expiry, never raw credentials or tokens.
- Use secret-guard and production-confirmation ideas as defense in depth around the application.

## Our execution boundary

The AI may produce only a validated `TradeDecision`. It may not choose quantity, construct an order request, or call a KIS order endpoint. A deterministic Risk Engine calculates permitted size and limits; a fixed Order Manager validates and executes any future order. The current implementation is earlier than that boundary: `smoke-kis` only authenticates and reads one current price, with no order, balance, or account-query API.

The repository is therefore used as a design reference, not as an installed dependency. We intentionally do not copy its agent configuration, MCP settings, or `kis-order-executor` into this project.
