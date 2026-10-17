import { defineConfig, mergeConfig } from "vite-plus";
import rootConfig from "../../vite.config.ts";

export default mergeConfig(
  rootConfig,
  defineConfig({
    test: {
      fileParallelism: false,
      exclude:
        process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL === undefined
          ? ["**/*.pg.test.ts"]
          : [],
    },
    pack: {
      entry: { main: "src/main.ts", admin: "src/adminEntry.ts" },
      format: "esm",
      outDir: "dist",
      clean: true,
      deps: { alwaysBundle: () => true, onlyBundle: false },
      outputOptions: {
        plugins: [
          {
            name: "personal-relay-runtime-graph",
            generateBundle(_options, bundle) {
              const chunks = Object.values(bundle).flatMap((item) =>
                item.type === "chunk"
                  ? [
                      {
                        fileName: item.fileName,
                        modules: Object.keys(item.modules),
                        imports: item.imports,
                        dynamicImports: item.dynamicImports,
                      },
                    ]
                  : [],
              );
              this.emitFile({
                type: "asset",
                fileName: "runtime-graph.json",
                source: JSON.stringify(chunks, null, 2),
              });
            },
          },
        ],
      },
    },
  }),
);
