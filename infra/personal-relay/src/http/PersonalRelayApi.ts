import { RelayApi } from "@t3tools/contracts/relay";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";

export const PersonalRelayApi = HttpApi.make("PersonalRelayApi").add(
  RelayApi.groups.health,
  RelayApi.groups.mobile,
  RelayApi.groups.token,
  HttpApiGroup.make("client").add(RelayApi.groups.client.endpoints.listDevicesV2),
  HttpApiGroup.make("server").add(RelayApi.groups.server.endpoints.publishAgentActivity),
);
