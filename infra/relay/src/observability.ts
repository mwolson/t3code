// @effect-diagnostics nodeBuiltinImport:off - one sha256 inside a synchronous Output.map.
import * as NodeCrypto from "node:crypto";

import * as Alchemy from "alchemy";
import * as Axiom from "@alchemy.run/axiom";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Output from "alchemy/Output";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";

import { relayResourceNameForStage } from "./deploymentConfig.ts";
import { MobileMonitoring } from "./mobileTelemetry/monitoring.ts";
import { RelayMonitoring } from "./monitoring.ts";

const relayRecentSpansQuery = (dataset: string) =>
  [
    `['${dataset}']`,
    `| where isnotnull(span_id) or isnotnull(trace_id)`,
    `| extend requestMethod = column_ifexists('attributes.http.request.method', ''), path = column_ifexists('attributes.url.path', ''), endpoint = column_ifexists('attributes.http.route', ''), statusCode = column_ifexists('attributes.http.response.status_code', 0), customAttributes = column_ifexists('attributes.custom', dynamic({}))`,
    `| extend userId = customAttributes['user']['id']`,
    `| project _time, name, trace_id, span_id, duration, requestMethod, path, statusCode, endpoint, userId`,
    `| order by _time desc`,
    `| limit 200`,
  ].join("\n");

export const RelayObservability = Effect.gen(function* () {
  const { stage } = yield* Alchemy.Stack;
  const traces = yield* Axiom.Dataset("RelayTracesDataset", {
    name: relayResourceNameForStage("t3-code-relay-traces", stage),
    kind: "otel:traces:v1",
    description: "T3 Code relay Worker HTTP request spans.",
    retentionDays: 30,
    useRetentionPeriod: true,
  });

  const workerIngestToken = yield* Axiom.ApiToken("RelayWorkerAxiomIngestToken", {
    name: relayResourceNameForStage("t3-code-relay-otel-ingest", stage),
    description: "Owned by Alchemy. Scoped OTLP ingest token for relay HTTP spans.",
    datasetCapabilities: Output.map(traces.name, (dataset) => ({
      [dataset]: { ingest: ["create" as const] },
    })),
  });

  const mobileIngestToken = yield* Axiom.ApiToken("RelayMobileAxiomIngestToken", {
    name: relayResourceNameForStage("t3-code-mobile-otel-ingest", stage),
    description: "Owned by Alchemy. Scoped OTLP ingest token for T3 Code mobile spans.",
    datasetCapabilities: Output.map(traces.name, (dataset) => ({
      [dataset]: { ingest: ["create" as const] },
    })),
  });

  const clientIngestToken = yield* Axiom.ApiToken("RelayClientAxiomIngestToken", {
    name: relayResourceNameForStage("t3-code-relay-client-otel-ingest", stage),
    description: "Owned by Alchemy. Scoped OTLP ingest token for first-party relay client spans.",
    datasetCapabilities: Output.map(traces.name, (dataset) => ({
      [dataset]: { ingest: ["create" as const] },
    })),
  });

  // expo-observe data, forwarded by src/mobileTelemetry. Mobile spans share the traces dataset.
  const mobileLogs = yield* Axiom.Dataset("MobileLogsDataset", {
    name: relayResourceNameForStage("t3-code-mobile-logs", stage),
    kind: "otel:logs:v1",
    description: "T3 Code mobile app errors, crashes, and log events from expo-observe.",
    retentionDays: 30,
    useRetentionPeriod: true,
  });
  const mobileMetrics = yield* Axiom.Dataset("MobileMetricsDataset", {
    name: relayResourceNameForStage("t3-code-mobile-metrics", stage),
    kind: "otel:metrics:v1",
    description: "T3 Code mobile app startup and update metrics from expo-observe.",
    retentionDays: 30,
    useRetentionPeriod: true,
  });
  const mobileTelemetryIngestToken = yield* Axiom.ApiToken("MobileTelemetryAxiomIngestToken", {
    name: relayResourceNameForStage("t3-code-mobile-telemetry-ingest", stage),
    description: "Owned by Alchemy. Scoped ingest token the relay forwards mobile telemetry with.",
    datasetCapabilities: Output.map(
      Output.all(traces.name, mobileLogs.name, mobileMetrics.name),
      (datasets) =>
        Object.fromEntries(datasets.map((dataset) => [dataset, { ingest: ["create" as const] }])),
    ),
  });

  const mobileSourceMaps = yield* Cloudflare.R2.Bucket("MobileSourceMapsBucket", {
    name: mobileSourceMapsBucketName(stage),
    // R2 refuses to delete a non-empty bucket, which would half tear down every non-prod stage.
    forceDestroy: stage !== "prod",
  });

  yield* Axiom.View("RelayRecentSpansView", {
    name: relayResourceNameForStage("t3-code-relay-recent-spans", stage),
    description: "Recent relay HTTP request spans.",
    datasets: [traces.name],
    aplQuery: Output.map(traces.name, relayRecentSpansQuery),
  });

  yield* RelayMonitoring(traces);
  yield* MobileMonitoring(mobileLogs, traces);

  return {
    traces,
    workerIngestToken,
    mobileIngestToken,
    clientIngestToken,
    mobileLogs,
    mobileMetrics,
    mobileTelemetryIngestToken,
    mobileSourceMaps,
  } as const;
});

const mobileSourceMapsBucketName = (stage: string) =>
  relayResourceNameForStage("t3-code-mobile-sourcemaps", stage);

/**
 * Write-only S3 credentials for the source maps bucket: the `MOBILE_SOURCEMAPS_R2` secret.
 * R2's S3 access key id is the API token's id and the secret the SHA-256 of its value.
 */
export const MobileSourceMapsUploadCredentials = Effect.gen(function* () {
  const { stage } = yield* Alchemy.Stack;
  const { accountId } = yield* yield* Cloudflare.CloudflareEnvironment;
  const bucketName = mobileSourceMapsBucketName(stage);
  const token = yield* Cloudflare.ApiToken.AccountApiToken("MobileSourceMapsUploadToken", {
    name: relayResourceNameForStage("t3-code-mobile-sourcemaps-upload", stage),
    policies: [
      {
        effect: "allow",
        permissionGroups: ["Workers R2 Storage Bucket Item Write"],
        resources: { [`com.cloudflare.edge.r2.bucket.${accountId}_default_${bucketName}`]: "*" },
      },
    ],
  });
  return Output.map(Output.all(token.tokenId, token.value), ([tokenId, value]) =>
    Redacted.make(
      JSON.stringify({
        endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
        bucketName,
        accessKeyId: tokenId,
        secretAccessKey: NodeCrypto.createHash("sha256")
          .update(Redacted.value(value))
          .digest("hex"),
      }),
    ),
  );
});

export { withSpanAttributes, withSchemaErrorSpanAttributes } from "./spanAttributes.ts";
