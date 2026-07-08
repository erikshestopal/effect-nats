/**
 * Shared layers and helpers for effect-nats examples.
 *
 * Point `NATS_URL` at your broker (default `nats://127.0.0.1:4222`).
 * JetStream/KV/ObjectStore examples need a server started with `-js`.
 */
import { Console, Effect, Layer } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as JetStreamManager from "effect-nats/JetStreamManager";
import * as NatsClient from "effect-nats/NatsClient";
import * as NodeConnector from "effect-nats/NodeConnector";

/** Override with `NATS_URL` in the environment (defaults to local nats-server). */
export const natsUrl = Bun.env["NATS_URL"] ?? "nats://127.0.0.1:4222";

export const encoder = new TextEncoder();
export const decoder = new TextDecoder();

/** Scoped TCP client. */
export const NatsLive = NatsClient.layer({ servers: natsUrl }).pipe(Layer.provide(NodeConnector.layer));

/** JetStream + JsMessage on a single shared connection. */
export const JetStreamLive = JetStream.layer().pipe(Layer.provide(NatsLive));

/** JetStream manager on a single shared connection. */
export const ManagerLive = JetStreamManager.layer().pipe(Layer.provide(NatsLive));

/** Publish + consume + admin APIs on one connection. */
export const FullJetStreamLive = Layer.merge(JetStream.layer(), JetStreamManager.layer()).pipe(Layer.provide(NatsLive));

/**
 * Run an example program to completion, logging the success value (if any)
 * and exiting non-zero on failure.
 */
export const runMain = <A, E>(options: { readonly label: string; readonly effect: Effect.Effect<A, E> }): void => {
  Effect.runPromise(
    options.effect.pipe(
      Effect.tap((value) => Console.log(`${options.label}:`, value)),
      Effect.tapError((error) => Console.error(`${options.label} failed:`, error)),
    ),
  ).then(
    () => undefined,
    () => {
      process.exitCode = 1;
    },
  );
};
