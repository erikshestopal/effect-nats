/**
 * JetStream message view and acknowledgments.
 *
 * @since 0.1.0
 */
import { Cause, Context, DateTime, Duration, Effect, Layer, Option, Predicate, Result, Schema, Stream } from "effect";
import { dual, identity } from "effect/Function";
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
  get text(): string {
    return decoder.decode(this.payload);
  }

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

export type AckOptions = {
  readonly nakDelay?: DurationInput;
};

/**
 * Operations that require the SDK-backed JetStream message handle.
 *
 * @example
 * ```ts
 * import { Effect, Stream } from "effect"
 * import * as JsMessage from "effect-nats/JsMessage"
 *
 * const handle = (messagesStream: Stream.Stream<JsMessage.JsMessage>) => Effect.gen(function*() {
 *   yield* messagesStream.pipe(JsMessage.tapAck(() => Effect.void), Stream.runDrain)
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
  readonly confirmAck: (self: JsMessage) => Effect.Effect<boolean, NatsError.TimeoutError>;
}

const decoder = new TextDecoder();

export class JsMessageService extends Context.Service<JsMessageService, Service>()("effect-nats/JsMessage") {}

export const isJsMessage = Schema.is(JsMessage);

export const ack = (self: JsMessage): Effect.Effect<void, never, JsMessageService> =>
  Effect.flatMap(JsMessageService, (messages) => messages.ack(self));

export const nak: {
  (options?: NakOptions): (self: JsMessage) => Effect.Effect<void, never, JsMessageService>;
  (self: JsMessage, options?: NakOptions): Effect.Effect<void, never, JsMessageService>;
} = dual(
  (args) => isJsMessage(args[0]),
  (self: JsMessage, options?: NakOptions): Effect.Effect<void, never, JsMessageService> =>
    Effect.flatMap(JsMessageService, (messages) => messages.nak(self, options)),
);

export const working = (self: JsMessage): Effect.Effect<void, never, JsMessageService> =>
  Effect.flatMap(JsMessageService, (messages) => messages.working(self));

export const term: {
  (options?: TermOptions): (self: JsMessage) => Effect.Effect<void, never, JsMessageService>;
  (self: JsMessage, options?: TermOptions): Effect.Effect<void, never, JsMessageService>;
} = dual(
  (args) => isJsMessage(args[0]),
  (self: JsMessage, options?: TermOptions): Effect.Effect<void, never, JsMessageService> =>
    Effect.flatMap(JsMessageService, (messages) => messages.term(self, options)),
);

export const confirmAck = (self: JsMessage): Effect.Effect<boolean, NatsError.TimeoutError, JsMessageService> =>
  Effect.flatMap(JsMessageService, (messages) => messages.confirmAck(self));

const nakOnFailure = (self: JsMessage, options?: AckOptions) =>
  Predicate.isUndefined(options?.nakDelay) ? nak(self) : nak(self, { delay: options.nakDelay });

const applyAckPolicy = <A, E, R>(
  self: JsMessage,
  effect: Effect.Effect<A, E, R>,
  options?: AckOptions,
): Effect.Effect<Result.Result<A, void>, never, R | JsMessageService> =>
  effect.pipe(
    Effect.matchCauseEffect({
      onFailure: (cause) =>
        Cause.hasFails(cause)
          ? nakOnFailure(self, options).pipe(Effect.as(Result.fail(undefined)))
          : term(self).pipe(Effect.andThen(Effect.logError(cause)), Effect.as(Result.fail(undefined))),
      onSuccess: (value) => ack(self).pipe(Effect.as(Result.succeed(value))),
    }),
  );

export const tapAck: {
  <A, E2, R2>(
    f: (message: JsMessage) => Effect.Effect<A, E2, R2>,
    options?: AckOptions,
  ): <E, R>(self: Stream.Stream<JsMessage, E, R>) => Stream.Stream<JsMessage, E, R | R2 | JsMessageService>;
  <E, R, A, E2, R2>(
    self: Stream.Stream<JsMessage, E, R>,
    f: (message: JsMessage) => Effect.Effect<A, E2, R2>,
    options?: AckOptions,
  ): Stream.Stream<JsMessage, E, R | R2 | JsMessageService>;
} = dual(
  (args) => Stream.isStream(args[0]),
  <E, R, A, E2, R2>(
    self: Stream.Stream<JsMessage, E, R>,
    f: (message: JsMessage) => Effect.Effect<A, E2, R2>,
    options?: AckOptions,
  ): Stream.Stream<JsMessage, E, R | R2 | JsMessageService> =>
    self.pipe(Stream.tap((message) => applyAckPolicy(message, f(message), options).pipe(Effect.asVoid))),
);

export const mapEffectAcked: {
  <A, E2, R2>(
    f: (message: JsMessage) => Effect.Effect<A, E2, R2>,
    options?: AckOptions,
  ): <E, R>(self: Stream.Stream<JsMessage, E, R>) => Stream.Stream<A, E, R | R2 | JsMessageService>;
  <E, R, A, E2, R2>(
    self: Stream.Stream<JsMessage, E, R>,
    f: (message: JsMessage) => Effect.Effect<A, E2, R2>,
    options?: AckOptions,
  ): Stream.Stream<A, E, R | R2 | JsMessageService>;
} = dual(
  (args) => Stream.isStream(args[0]),
  <E, R, A, E2, R2>(
    self: Stream.Stream<JsMessage, E, R>,
    f: (message: JsMessage) => Effect.Effect<A, E2, R2>,
    options?: AckOptions,
  ): Stream.Stream<A, E, R | R2 | JsMessageService> =>
    self.pipe(
      Stream.mapEffect((message) => applyAckPolicy(message, f(message), options)),
      Stream.filterMap(identity),
    ),
);

export const make = Effect.sync(() => {
  const sdkMessages = new WeakMap<JsMessage, JsMsg>();

  const sdkMessage = (self: JsMessage) => sdkMessages.get(self);

  const fromJsMsg = (msg: JsMsg): JsMessage => {
    const info = msg.info;
    const message = JsMessage.make({
      subject: msg.subject,
      payload: msg.data,
      replyTo: Option.none(),
      headers: Option.getOrElse(
        Option.map(Option.fromUndefinedOr(msg.headers), NatsHeaders.fromMsgHdrs),
        () => NatsHeaders.empty,
      ),
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
  const confirmAck = (self: JsMessage) =>
    Effect.tryPromise({
      try: () => sdkMessage(self)?.ackAck() ?? Promise.resolve(false),
      /* v8 ignore next */
      catch: () => new NatsError.TimeoutError(),
    });
  return JsMessageService.of({
    fromJsMsg,
    ack,
    nak,
    working,
    term,
    confirmAck,
  });
});

export const layer: Layer.Layer<JsMessageService> = Layer.effect(JsMessageService, make);
