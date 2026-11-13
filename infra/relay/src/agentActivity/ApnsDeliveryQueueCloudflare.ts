import * as Alchemy from "alchemy/RuntimeContext";
import type * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  ApnsDeliveryQueueSender,
  ApnsDeliveryQueueSenderError,
  layer,
} from "./ApnsDeliveryQueue.ts";

export const layerCloudflareQueues = (
  sender: Cloudflare.Queues.WriteQueueClient,
  alchemyRuntimeContext: Alchemy.BaseRuntimeContext,
) =>
  layer.pipe(
    Layer.provide(
      Layer.succeed(
        ApnsDeliveryQueueSender,
        ApnsDeliveryQueueSender.of({
          send: Effect.fn("ApnsDeliveryQueueSender.send")(
            function* (body) {
              return yield* sender.send(body);
            },
            Effect.provideService(Alchemy.RuntimeContext, alchemyRuntimeContext),
            Effect.mapError((cause) => new ApnsDeliveryQueueSenderError({ cause })),
          ),
        }),
      ),
    ),
  );
