/**
 * NATS transport connector service.
 *
 * @since 0.1.0
 */
import { Context, Effect, Layer } from "effect";
import { wsconnect } from "@nats-io/nats-core";
import type { ConnectionOptions, NatsConnection } from "@nats-io/nats-core";
import * as NatsError from "./NatsError.ts";
import * as Connect from "./internal/connect.ts";

/** @since 0.1.0 @category models */
export interface Connector {
  readonly connect: (
    options: ConnectionOptions,
  ) => Effect.Effect<NatsConnection, NatsError.ConnectionError | NatsError.TimeoutError>;
}

/** @since 0.1.0 @category services */
export class NatsConnector extends Context.Service<NatsConnector, Connector>()("effect-nats/NatsConnector") {}

/** @since 0.1.0 @category layers */
export const layerWebSocket: Layer.Layer<NatsConnector> = Layer.succeed(
  NatsConnector,
  NatsConnector.of({
    connect: (options) => Connect.connect({ dial: wsconnect, connectionOptions: options }),
  }),
);
