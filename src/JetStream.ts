/**
 * JetStream publish service.
 *
 * @since 0.1.0
 */
import { Context, Effect, Layer, Option, Predicate, Schema, Stream } from "effect";
import { jetstream } from "@nats-io/jetstream";
import type { Payload } from "@nats-io/nats-core";
import type {
  Consumer,
  ConsumerInfo,
  ConsumerMessages,
  ConsumerNotification,
  JetStreamClient,
  OrderedConsumerOptions,
} from "@nats-io/jetstream";
import type { Input as DurationInput } from "effect/Duration";
import * as JetStreamError from "./JetStreamError.ts";
import * as JsMessage from "./JsMessage.ts";
import * as NatsClient from "./NatsClient.ts";
import * as NatsError from "./NatsError.ts";
import * as NatsHeaders from "./NatsHeaders.ts";
import * as Iterators from "./internal/iterator.ts";
import * as JsOptions from "./internal/jsOptions.ts";
import * as JsErrors from "./internal/mapJsError.ts";

/**
 * Runtime JetStream operations.
 *
 * @example
 * ```ts
 * import { Effect, Stream } from "effect"
 * import * as JetStream from "effect-nats/JetStream"
 * import * as JsMessage from "effect-nats/JsMessage"
 *
 * const program = Effect.gen(function*() {
 *   const js = yield* JetStream.JetStream
 *   yield* js.publish("orders.created")
 *   const consumer = yield* js.consumer("ORDERS", "processor")
 *   yield* consumer.consume().pipe(
 *     JsMessage.tapAck(() => Effect.void),
 *     Stream.runDrain
 *   )
 * })
 * ```
 *
 * @since 0.1.0
 * @category models
 */
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
  readonly consumer: (
    stream: string,
    name?: string | Partial<OrderedConsumerOptions>,
  ) => Effect.Effect<
    JsConsumer,
    | JetStreamError.StreamNotFoundError
    | JetStreamError.ConsumerNotFoundError
    | JetStreamError.JetStreamApiError
    | JetStreamError.JetStreamError
  >;
}

export type JetStreamOptions = {
  readonly apiPrefix?: string;
  readonly domain?: string;
  readonly timeout?: DurationInput;
};

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

export type NextOptions = {
  readonly expires?: DurationInput;
};

export type FetchOptions = {
  readonly maxMessages?: number;
  readonly maxBytes?: number;
  readonly expires?: DurationInput;
};

export type ConsumeOptions = {
  readonly maxMessages?: number;
  readonly maxBytes?: number;
  readonly thresholdMessages?: number;
  readonly thresholdBytes?: number;
  readonly abortOnMissingResource?: boolean;
  readonly bind?: boolean;
  readonly onNotification?: (notification: ConsumerNotification) => Effect.Effect<unknown>;
};

export interface JsConsumer {
  readonly next: (
    options?: NextOptions,
  ) => Effect.Effect<Option.Option<JsMessage.JsMessage>, JetStreamError.JetStreamErrors>;
  readonly fetch: (options?: FetchOptions) => Stream.Stream<JsMessage.JsMessage, JetStreamError.JetStreamErrors>;
  readonly consume: (options?: ConsumeOptions) => Stream.Stream<JsMessage.JsMessage, JetStreamError.JetStreamErrors>;
  readonly info: (options?: {
    readonly cached?: boolean;
  }) => Effect.Effect<ConsumerInfo, JetStreamError.JetStreamApiError | JetStreamError.JetStreamError>;
}

export class PubAck extends Schema.Class<PubAck>("effect-nats/JetStream/PubAck")({
  stream: Schema.String,
  seq: Schema.Finite,
  duplicate: Schema.Boolean,
  domain: Schema.Option(Schema.String),
}) {}

export class JetStream extends Context.Service<JetStream, Service>()("effect-nats/JetStream") {}

export const make = Effect.fnUntraced(function* (options: JetStreamOptions = {}) {
  const nats = yield* NatsClient.NatsClient;
  const messages = yield* JsMessage.JsMessageService;
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
    consumer: (stream, name) =>
      Effect.tryPromise({
        try: () => client.consumers.get(stream, name),
        catch: JsErrors.mapConsumerError(
          Option.match(Option.liftPredicate(Predicate.isString)(name), {
            onNone: () => ({ stream }),
            onSome: (consumer) => ({ stream, consumer }),
          }),
        ),
      }).pipe(Effect.map((consumer) => makeConsumer({ consumer, messages }))),
  });
});

const makeConsumer = (state: { readonly consumer: Consumer; readonly messages: JsMessage.Service }): JsConsumer => ({
  next: (options = {}) =>
    Effect.tryPromise({
      try: () => state.consumer.next(JsOptions.translateNextOptions(options)),
      catch: JsErrors.mapJetStreamError,
    }).pipe(Effect.map((message) => Option.map(Option.fromNullishOr(message), state.messages.fromJsMsg))),
  fetch: (options = {}) =>
    Iterators.streamFromQueuedIterator({
      acquire: Effect.tryPromise({
        try: () => state.consumer.fetch(JsOptions.translateFetchOptions(options)),
        catch: JsErrors.mapJetStreamError,
      }),
      transform: state.messages.fromJsMsg,
      onError: JsErrors.mapJetStreamError,
      onRelease: closeConsumerMessages,
    }),
  consume: (options = {}) =>
    Iterators.streamFromQueuedIterator({
      acquire: Effect.gen(function* () {
        const consumerMessages = yield* Effect.tryPromise({
          try: () => state.consumer.consume(JsOptions.translateConsumeOptions(options)),
          catch: JsErrors.mapJetStreamError,
        });
        if (Predicate.isNotUndefined(options.onNotification)) {
          const onNotification = options.onNotification;
          yield* Stream.fromAsyncIterable(consumerMessages.status(), JsErrors.mapJetStreamError).pipe(
            Stream.mapEffect((notification) =>
              onNotification(notification).pipe(
                Effect.matchCauseEffect({
                  onFailure: (cause) => Effect.logWarning(cause),
                  onSuccess: () => Effect.void,
                }),
              ),
            ),
            Stream.runDrain,
            Effect.forkScoped({ startImmediately: true }),
          );
        }
        return consumerMessages;
      }),
      transform: state.messages.fromJsMsg,
      onError: JsErrors.mapJetStreamError,
      onRelease: closeConsumerMessages,
    }),
  info: (options = {}) =>
    Effect.tryPromise({
      try: () => state.consumer.info(options.cached),
      catch: JsErrors.mapJetStreamError,
    }),
});

const closeConsumerMessages = (messages: ConsumerMessages): Effect.Effect<void> =>
  Effect.tryPromise(() => messages.close()).pipe(Effect.asVoid, Effect.ignore);

/**
 * Provides JetStream and the JetStream message acknowledgment service.
 *
 * @see {@link make} for constructing the service effectfully
 *
 * @since 0.1.0
 * @category layers
 */
export const layer = (
  options: JetStreamOptions = {},
): Layer.Layer<JetStream | JsMessage.JsMessageService, never, NatsClient.NatsClient> =>
  Layer.effect(JetStream, make(options)).pipe(Layer.provideMerge(JsMessage.layer));

export type { ConsumerInfo, ConsumerNotification, OrderedConsumerOptions };
