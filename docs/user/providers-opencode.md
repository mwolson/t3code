# OpenCode

T3 Code uses the OpenCode setup on the connected environment. With a remote environment, its
OpenCode login and configuration apply, not the setup on your desktop or phone.

T3 Code talks to OpenCode 2 through the built-in OpenCode provider. It attaches to the OpenCode
daemon already running on the environment, or starts that daemon. It does not spawn a second
OpenCode server per thread. If the daemon cannot start, update OpenCode 2, then refresh the
provider status. Reconnecting the client also retries the connection.

## Server authentication

Without a server URL, T3 Code uses the host OpenCode daemon. A password in the provider settings
overrides `OPENCODE_SERVER_PASSWORD` from the environment.

With a server URL, T3 Code connects to that external server and uses only the password in the
provider settings. It does not send a local `OPENCODE_SERVER_PASSWORD` to an external server.

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
can stay cached in a running daemon. After changing a login or config outside T3 Code, refresh
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
