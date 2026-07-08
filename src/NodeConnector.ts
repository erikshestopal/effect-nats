/**
 * Node/Bun TCP NATS connector.
 *
 * @since 0.1.0
 */
import { Layer } from "effect";
import { connect } from "@nats-io/transport-node";
import * as NatsConnector from "./NatsConnector.ts";
import * as Connect from "./internal/connect.ts";

export const layer: Layer.Layer<NatsConnector.NatsConnector> = Layer.succeed(
  NatsConnector.NatsConnector,
  NatsConnector.NatsConnector.of({
    connect: (options) => Connect.connect({ dial: connect, connectionOptions: options }),
  }),
);
