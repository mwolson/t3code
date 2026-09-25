# OpenCode

T3 Code uses the OpenCode setup on the connected environment. With a remote environment, its
OpenCode login and configuration apply, not the setup on your desktop or phone.

T3 Code talks to OpenCode 2 through the built-in OpenCode provider, which is off by default.
It uses your OpenCode 2 service, the one `opencode service start` runs, and does not spawn a
server per thread. When that service is not running, T3 Code starts it with
`opencode service start`, using the `opencode` command on its PATH. T3 Code never stops, restarts
or replaces the service. T3 Code supports exactly OpenCode 2.0.15 and checks the service's
registration, credentials and reported version before it sends any request.

If T3 Code cannot start the service, provider status says why. Run `opencode service start` in a
terminal to see the error, then choose **Refresh provider status**. T3 Code leaves a running
service alone when it does not answer, is a different version, or its registration no longer
matches the running server: fix or restart it yourself, then refresh. Reconnecting the client also
retries the connection.

The background-subagent setting applies when T3 Code starts the service. It cannot change a
service that is already running. Enable background subagents on that service with
`OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS`.

T3 Code's orchestration tools (the `t3-code` MCP server, used to delegate tasks and manage T3
threads) are unavailable in OpenCode 2 threads. Other MCP servers you configured on the OpenCode 2
service stay available.

OpenCode 2 has no account quota API, so usage limits show as unsupported. T3 Code does not read
an OpenCode 1 login to estimate them.

## Server authentication

Without a server URL, T3 Code uses the OpenCode 2 service registered on the environment and its
registered credentials.

With a server URL, T3 Code connects to that external server and uses only the password in the
provider settings. It does not send a local `OPENCODE_SERVER_PASSWORD` to an external server.

## Approval scope

T3 answers OpenCode permission requests individually. Approving once affects that request only.
Approving for the session remembers the grant in the current T3 adapter session and still sends
individual approvals to OpenCode. It does not create a persistent workspace grant through
OpenCode's native `always` reply. Full-access mode also uses individual approvals.

## Refresh the model list

T3 Code loads the model list when an enabled OpenCode provider starts and keeps the list in its
cache. Reconnecting a client or using a refresh control asks OpenCode for the list again. The
periodic provider health setting does not refresh OpenCode's catalog.

After changing an OpenCode login or configuration outside T3 Code, open **Settings > Providers**,
select the environment, and choose **Refresh provider status**. Changing the provider's
configuration in T3 Code also replaces that provider connection.

On mobile, open the thread settings and select **Refresh models**. The control stays disabled while
the refresh runs and shows an error if the refresh fails.

OpenCode reads credential changes on each model-list request. Native OpenCode configuration files
can stay cached in the running service. After changing a login or config outside T3 Code, refresh
provider status. An external OpenCode server can require its own reload before a refresh returns
the new list.

If a refresh fails, T3 Code keeps the last known models, slash commands, and skills. Fix the
connection, then refresh again. A successful refresh can remove entries that OpenCode no longer
offers.

## Continue an existing thread

An existing thread keeps its selected model and options when that model is temporarily absent
from the catalog. The web picker shows an **Unavailable** row and keeps saved option values visible
until the model metadata returns. T3 Code does not switch the thread to the first model in the
list.

The stored selection does not guarantee that OpenCode can still run the model. If the provider
rejects it, select an available model before trying again.
