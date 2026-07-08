/**
 * JetStream publish service.
 *
 * @since 0.1.0
 */
import { Context, Effect, Layer, Option, Schema } from "effect";
import { jetstream } from "@nats-io/jetstream";
import type { Payload } from "@nats-io/nats-core";
import type { JetStreamClient } from "@nats-io/jetstream";
import type { Input as DurationInput } from "effect/Duration";
import * as JetStreamError from "./JetStreamError.ts";
import * as NatsClient from "./NatsClient.ts";
import * as NatsError from "./NatsError.ts";
import * as NatsHeaders from "./NatsHeaders.ts";
import * as JsOptions from "./internal/jsOptions.ts";
import * as JsErrors from "./internal/mapJsError.ts";

/** @since 0.1.0 @category models */
export interface Service {
  readonly client: JetStreamClient;
  readonly publish: (
    subject: string,
    options?: PublishOptions,
  ) => Effect.Effect<
    PubAck,
    | JetStreamError.JetStreamError
    | JetStreamError.JetStreamNotEnabledError
    | JetStreamError.WrongLastSequenceError
    | JetStreamError.JetStreamApiError
    | NatsError.TimeoutError
  >;
}

/** @since 0.1.0 @category options */
export type JetStreamOptions = {
  readonly apiPrefix?: string;
  readonly domain?: string;
  readonly timeout?: DurationInput;
};

/** @since 0.1.0 @category options */
export type PublishOptions = {
  readonly payload?: Payload;
  readonly headers?: NatsHeaders.Input;
  readonly msgID?: string;
  readonly timeout?: DurationInput;
  readonly retries?: number;
  readonly expect?: {
    readonly streamName?: string;
    readonly lastMsgID?: string;
    readonly lastSequence?: number;
    readonly lastSubjectSequence?: number;
  };
};

/** @since 0.1.0 @category models */
export class PubAck extends Schema.Class<PubAck>("effect-nats/JetStream/PubAck")({
  stream: Schema.String,
  seq: Schema.Finite,
  duplicate: Schema.Boolean,
  domain: Schema.Option(Schema.String),
}) {}

/** @since 0.1.0 @category services */
export class JetStream extends Context.Service<JetStream, Service>()("effect-nats/JetStream") {}

/** @since 0.1.0 @category constructors */
export const make = (options: JetStreamOptions = {}): Effect.Effect<Service, never, NatsClient.NatsClient> =>
  Effect.gen(function* () {
    const nats = yield* NatsClient.NatsClient;
    const client = jetstream(nats.connection, JsOptions.translateOptions(options));
    return JetStream.of({
      client,
      publish: (subject, publishOptions = {}) =>
        Effect.tryPromise({
          try: () => client.publish(subject, publishOptions.payload, JsOptions.translatePublishOptions(publishOptions)),
          catch: JsErrors.mapPublishError,
        }).pipe(
          Effect.map((ack) =>
            PubAck.make({
              stream: ack.stream,
              seq: ack.seq,
              duplicate: ack.duplicate,
              domain: Option.fromNullishOr(ack.domain),
            }),
          ),
        ),
    });
  });

/** @since 0.1.0 @category layers */
export const layer = (options: JetStreamOptions = {}): Layer.Layer<JetStream, never, NatsClient.NatsClient> =>
  Layer.effect(JetStream, make(options));
