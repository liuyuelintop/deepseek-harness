# Agent Note: Host-owned Web view Context for Typert Remote calls

Status: implemented

English | [中文](2026-08-31-host-owned-web-view-context.zh.md)

## Problem

A browser-selected Typert Context identity cannot authorize an operation for the Session displayed in one Web view. The caller can replace or replay that identity, while ordinary HTTP RPC carries no Host-owned view identity. A plugin that accepts a Session id, Agent Context, lease, token, or client id therefore cannot prove that it is acting on the Session visible in the requesting view.

## Decision

The Gateway owns one displayed-Session binding per physical Remote WebSocket and privately observes Session Controller's read-only `sessions.list.current` snapshot. It exposes no Client Remote method for replacing that binding. The Host validates a non-empty selection through `sessionQuery`, then creates a child Cordis Context whose `viewSessionId` is fixed to the validated Session. Clearing the selection leaves the connection without a view Context.

Typert descriptors opt into this authority with `invocation.kind: 'view'`. The Client exposes such a method as an ordinary unary Remote method with no Context identity or Session id in its business arguments. It sends the call through the already-authenticated physical WebSocket, and the Gateway injects the connection's current view Context after decoding the request. HTTP and direct invocations have no carrier-owned view Context and fail before business execution.

Navigation clears the old binding before resolving the new Session and cancels every active view call from the previous generation. A replacement WebSocket has no inherited binding; the Client rebinds the latest selection before its first view call. Socket and Session Controller disposal clear the binding and wait for active transport work to settle.

## Alternatives considered

- **Agent Context identity** — the Client adapter derives it from a caller-selected context, and a cold displayed Session may have no live Agent.
- **Plugin-issued leases or bearer tokens** — any caller that can request or replay the credential can use it from another scope, so the credential only relocates the confused-deputy problem.
- **A caller-supplied view or event-stream client id** — the Host cannot infer the originating physical view from an ordinary HTTP request, and a wire id is replayable.
- **Plugin-owned per-view transport** — it duplicates authentication, lifecycle, navigation, and multiplexing that the Gateway already owns, and it does not provide a reusable DSH authority for other view-scoped operations.

## Consequences

View-scoped Remote methods are unary and Web-only. They trade the HTTP carrier's simplicity for a Host-verifiable physical connection boundary and require Session Controller to be mounted as the sole view resolver. The binding identifies the displayed Session, not a live Agent and not the user who opened the browser.

Gateway tests pin the absence of a wire identity, rejection without a Host view Context, two-socket isolation, navigation cancellation, reconnect rebinding, and disposal. Session Controller tests pin validation of live and persisted Session identities. Domain plugins still own redaction and must treat an unavailable view Context or failed Session observation as a content-free failure.
