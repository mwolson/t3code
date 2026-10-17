import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

const Chunk = Schema.Struct({
  fileName: Schema.String,
  modules: Schema.Array(Schema.String),
  imports: Schema.Array(Schema.String),
  dynamicImports: Schema.Array(Schema.String),
});
const decodeGraph = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(Chunk)));
const forbidden = /(?:^|[/"' ])(?:alchemy(?:[/"']|$)|@cloudflare[/]|cloudflare:)/;

NodeRuntime.runMain(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dist = path.resolve(import.meta.dirname, "../dist");
    const chunks = yield* decodeGraph(
      yield* fs.readFileString(path.join(dist, "runtime-graph.json")),
    );
    if (
      !chunks.some((chunk) => chunk.fileName === "main.mjs") ||
      !chunks.some((chunk) => chunk.fileName === "admin.mjs")
    ) {
      return yield* Effect.die("Both personal relay entries must appear in the packed graph.");
    }
    for (const chunk of chunks) {
      for (const specifier of [...chunk.modules, ...chunk.imports, ...chunk.dynamicImports]) {
        if (forbidden.test(specifier))
          return yield* Effect.die(`Deployment-only module in runtime graph: ${specifier}`);
      }
      const source = yield* fs.readFileString(path.join(dist, chunk.fileName));
      if (
        /from\s*["'](?:alchemy|@cloudflare\/|cloudflare:)|import\(["'](?:alchemy|@cloudflare\/|cloudflare:)/.test(
          source,
        )
      ) {
        return yield* Effect.die(`Deployment-only external import in ${chunk.fileName}`);
      }
    }
    yield* Effect.logInfo(`Runtime graph is neutral (${chunks.length} chunks, both entries).`);
  }).pipe(Effect.provide(NodeServices.layer)),
);
