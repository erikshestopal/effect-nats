/**
 * JetStream message view and acknowledgments.
 *
 * @since 0.1.0
 */
import { Cause, Context, DateTime, Duration, Effect, Layer, Option, Predicate, Schema } from "effect";
import type { Input as DurationInput } from "effect/Duration";
import type { JsMsg } from "@nats-io/jetstream";
import * as NatsError from "./NatsError.ts";
import * as NatsHeaders from "./NatsHeaders.ts";

export class JsMessage extends Schema.Class<JsMessage>("effect-nats/JsMessage")({
  subject: Schema.String,
  payload: Schema.Uint8Array,
  replyTo: Schema.Option(Schema.String),
  headers: NatsHeaders.NatsHeaders,
  stream: Schema.String,
  consumer: Schema.String,
  seq: Schema.Finite,
  deliveryCount: Schema.Finite,
  redelivered: Schema.Boolean,
  pending: Schema.Finite,
  time: Schema.DateTimeUtc,
}) {
  /** @since 0.1.0 */
  get text(): string {
    return decoder.decode(this.payload);
  }

  /** @since 0.1.0 */
  json<S extends Schema.Top>(schema: S): Effect.Effect<S["Type"], Schema.SchemaError, S["DecodingServices"]> {
    return Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(this.text);
  }
}

export type NakOptions = {
  readonly delay?: DurationInput;
};

export type TermOptions = {
  readonly reason?: string;
};

export type ProcessWithOptions = {
  readonly handler: (msg: JsMessage) => Effect.Effect<unknown, unknown, unknown>;
  readonly nakDelay?: DurationInput;
};

/**
 * Operations that require the SDK-backed JetStream message handle.
 *
 * @example
 * ```ts
 * import { Effect } from "effect"
 * import * as JsMessage from "effect-nats/JsMessage"
 *
 * const handle = (msg: JsMessage.JsMessage) => Effect.gen(function*() {
 *   const messages = yield* JsMessage.JsMessageService
 *   yield* messages.processWith({ handler: () => Effect.void })(msg)
 * })
 * ```
 *
 * @since 0.1.0
 * @category services
 */
export interface Service {
  readonly fromJsMsg: (msg: JsMsg) => JsMessage;
  readonly ack: (self: JsMessage) => Effect.Effect<void>;
  readonly nak: (self: JsMessage, options?: NakOptions) => Effect.Effect<void>;
  readonly working: (self: JsMessage) => Effect.Effect<void>;
  readonly term: (self: JsMessage, options?: TermOptions) => Effect.Effect<void>;
  readonly ackAck: (self: JsMessage) => Effect.Effect<boolean, NatsError.TimeoutError>;
  readonly processWith: <A, E, R>(options: {
    readonly handler: (msg: JsMessage) => Effect.Effect<A, E, R>;
    readonly nakDelay?: DurationInput;
  }) => (msg: JsMessage) => Effect.Effect<void, never, R>;
}

const decoder = new TextDecoder();

/** @since 0.1.0 @category services */
export class JsMessageService extends Context.Service<JsMessageService, Service>()("effect-nats/JsMessage") {}

/** @since 0.1.0 @category guards */
export const isJsMessage = Schema.is(JsMessage);

/** @since 0.1.0 @category constructors */
export const make = Effect.sync(() => {
  const sdkMessages = new WeakMap<JsMessage, JsMsg>();

  const sdkMessage = (self: JsMessage) => sdkMessages.get(self);

  const fromJsMsg = (msg: JsMsg): JsMessage => {
    const info = msg.info;
    const message = JsMessage.make({
      subject: msg.subject,
      payload: msg.data,
      replyTo: Option.none(),
      headers: Predicate.isNotUndefined(msg.headers) ? NatsHeaders.fromMsgHdrs(msg.headers) : NatsHeaders.empty,
      stream: info.stream,
      consumer: info.consumer,
      seq: info.streamSequence,
      deliveryCount: info.deliveryCount,
      redelivered: info.redelivered,
      pending: info.pending,
      time: DateTime.fromDateUnsafe(msg.time),
    });
    sdkMessages.set(message, msg);
    return message;
  };

  const ack = (self: JsMessage) => Effect.sync(() => sdkMessage(self)?.ack());
  const nak = (self: JsMessage, options: NakOptions = {}) =>
    Effect.sync(() =>
      sdkMessage(self)?.nak(Predicate.isUndefined(options.delay) ? undefined : Duration.toMillis(options.delay)),
    );
  const working = (self: JsMessage) => Effect.sync(() => sdkMessage(self)?.working());
  const term = (self: JsMessage, options: TermOptions = {}) =>
    Effect.sync(() => sdkMessage(self)?.term(options.reason));
  const ackAck = (self: JsMessage) =>
    Effect.tryPromise({
      try: () => sdkMessage(self)?.ackAck() ?? Promise.resolve(false),
      /* v8 ignore next */
      catch: () => new NatsError.TimeoutError(),
    });
  const processWith =
    <A, E, R>(options: {
      readonly handler: (msg: JsMessage) => Effect.Effect<A, E, R>;
      readonly nakDelay?: DurationInput;
    }) =>
    (msg: JsMessage): Effect.Effect<void, never, R> =>
      options.handler(msg).pipe(
        Effect.matchCauseEffect({
          onFailure: (cause) =>
            Cause.hasFails(cause)
              ? nak(msg, Predicate.isUndefined(options.nakDelay) ? {} : { delay: options.nakDelay })
              : term(msg).pipe(Effect.andThen(Effect.logError(cause))),
          onSuccess: () => ack(msg),
        }),
        Effect.ignore,
      );

  return JsMessageService.of({
    fromJsMsg,
    ack,
    nak,
    working,
    term,
    ackAck,
    processWith,
  });
});

/** @since 0.1.0 @category layers */
export const layer: Layer.Layer<JsMessageService> = Layer.effect(JsMessageService, make);
