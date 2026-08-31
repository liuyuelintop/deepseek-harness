# Agent Note: Typert Remote 调用的 Host 自有 Web view Context

Status: implemented

[English](2026-08-31-host-owned-web-view-context.md) | 中文

## 问题

由浏览器选择的 Typert Context 身份无法授权某个 Web view 中所显示会话的操作。调用方可以替换或回放该身份，而普通 HTTP RPC 不携带 Host 自有的 view 身份。因此，接受会话 id、Agent Context、租约、token 或 client id 的插件无法证明自己操作的就是发起请求的 view 中可见的会话。

## 决策

网关为每条物理 Remote WebSocket 分别拥有一个显示会话绑定，并在内部观察 Session Controller 的只读 `sessions.list.current` 快照。它不公开任何可替换该绑定的 Client Remote 方法。Host 通过 `sessionQuery` 验证非空选择，然后创建一个 Cordis 子上下文，并把其中的 `viewSessionId` 固定为已验证的会话。清除选择后，该连接不再具有 view Context。

Typert 描述符通过 `invocation.kind: 'view'` 选择使用这项权限。客户端将这种方法公开为普通的一元 Remote 方法，其业务参数中没有 Context 身份或会话 id。客户端通过已经认证的物理 WebSocket 发送调用，网关在解码请求后注入该连接当前的 view Context。HTTP 调用和直接调用没有传输层自有的 view Context，因此会在业务代码执行前失败。

导航会先清除旧绑定，再解析新会话，并取消上一代绑定中的所有活动 view 调用。替换后的 WebSocket 不继承绑定；客户端会在第一次 view 调用前重新绑定最新选择。WebSocket 和 Session Controller dispose（资源释放）时会清除绑定，并等待活动传输工作完全停稳。

## 考虑过的替代方案

- **Agent Context 身份**：客户端适配器从调用方选择的上下文派生该身份，而显示的冷会话可能没有活动 Agent。
- **插件签发的租约或 bearer token**：任何能够申请或回放凭据的调用方都能从另一个 scope 使用它，因此凭据只会转移 confused-deputy 问题。
- **调用方提供的 view id 或事件流 client id**：Host 无法从普通 HTTP 请求推断其来源物理 view，而协议中的 id 可以回放。
- **插件自有的 per-view 传输**：这种方案会重复网关已经拥有的认证、生命周期、导航和多路复用能力，也无法为其他 view 作用域操作提供可复用的 DSH 权限。

## 后果

view 作用域 Remote 方法仅支持一元调用和 Web。该设计用 HTTP 传输的简单性换取 Host 可验证的物理连接边界，并要求挂载 Session Controller 作为唯一 view 解析器。该绑定标识显示的会话，而不标识活动 Agent，也不标识打开浏览器的用户。

网关测试固定了协议中没有身份、缺少 Host view Context 时拒绝、双 WebSocket 隔离、导航取消、重连再绑定以及 dispose 行为。Session Controller 测试固定了活动和持久化会话身份的验证。领域插件仍然负责脱敏，并且必须将 view Context 不可用或会话观察失败处理为不含内容的失败。
