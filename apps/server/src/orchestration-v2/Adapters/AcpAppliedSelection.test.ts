import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import { acpInitialAppliedSelection } from "@t3tools/provider-acp/server/adapter";

const requested = { instanceId: ProviderInstanceId.make("acp-seed"), model: "requested-model" };

it("does not invent an applied model before an unacknowledged model step", () => {
  assert.deepEqual(acpInitialAppliedSelection({}, requested, undefined, true), {
    instanceId: requested.instanceId,
  });
});

it("keeps the requested label when the agent exposes no model step", () => {
  assert.equal(acpInitialAppliedSelection({}, requested, undefined, false).model, requested.model);
});

it("seeds native-reported model and option values rather than requested values", () => {
  assert.deepEqual(
    acpInitialAppliedSelection(
      {
        models: { currentModelId: "native-model", availableModels: [] },
        configOptions: [
          {
            id: "effort",
            name: "Effort",
            type: "select",
            currentValue: "low",
            options: [{ value: "low", name: "Low" }],
          },
        ],
      },
      { ...requested, options: [{ id: "effort", value: "high" }] },
      undefined,
      true,
    ),
    {
      instanceId: requested.instanceId,
      model: "native-model",
      options: [{ id: "effort", value: "low" }],
    },
  );
});
