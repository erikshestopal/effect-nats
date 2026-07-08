/**
 * Typed JetStream errors.
 *
 * @since 0.1.0
 */
import { Option, Schema } from "effect";

/** @since 0.1.0 @category errors */
export class JetStreamNotEnabledError extends Schema.TaggedErrorClass<JetStreamNotEnabledError>(
  "effect-nats/JetStreamError/JetStreamNotEnabledError",
)("JetStreamNotEnabledError", { cause: Schema.Defect() }) {}

/** @since 0.1.0 @category errors */
export class JetStreamError extends Schema.TaggedErrorClass<JetStreamError>(
  "effect-nats/JetStreamError/JetStreamError",
)("JetStreamError", { cause: Schema.Defect() }) {}

/** @since 0.1.0 @category errors */
export class JetStreamStatusError extends Schema.TaggedErrorClass<JetStreamStatusError>(
  "effect-nats/JetStreamError/JetStreamStatusError",
)("JetStreamStatusError", { code: Schema.Finite, description: Schema.String }) {}

/** @since 0.1.0 @category errors */
export class JetStreamApiError extends Schema.TaggedErrorClass<JetStreamApiError>(
  "effect-nats/JetStreamError/JetStreamApiError",
)("JetStreamApiError", { code: Schema.Finite, status: Schema.Finite, description: Schema.String }) {}

/** @since 0.1.0 @category errors */
export class StreamNotFoundError extends Schema.TaggedErrorClass<StreamNotFoundError>(
  "effect-nats/JetStreamError/StreamNotFoundError",
)("StreamNotFoundError", { stream: Schema.String }) {}

/** @since 0.1.0 @category errors */
export class ConsumerNotFoundError extends Schema.TaggedErrorClass<ConsumerNotFoundError>(
  "effect-nats/JetStreamError/ConsumerNotFoundError",
)("ConsumerNotFoundError", { stream: Schema.String, consumer: Schema.Option(Schema.String) }) {}

/** @since 0.1.0 @category errors */
export class InvalidNameError extends Schema.TaggedErrorClass<InvalidNameError>(
  "effect-nats/JetStreamError/InvalidNameError",
)("InvalidNameError", { name: Schema.String }) {}

/** @since 0.1.0 @category errors */
export class WrongLastSequenceError extends Schema.TaggedErrorClass<WrongLastSequenceError>(
  "effect-nats/JetStreamError/WrongLastSequenceError",
)("WrongLastSequenceError", { expected: Schema.Option(Schema.Finite), cause: Schema.Defect() }) {}

/** @since 0.1.0 @category errors */
export const JetStreamErrors = Schema.Union([
  JetStreamNotEnabledError,
  JetStreamError,
  JetStreamStatusError,
  JetStreamApiError,
  StreamNotFoundError,
  ConsumerNotFoundError,
  InvalidNameError,
  WrongLastSequenceError,
]);

/** @since 0.1.0 @category errors */
export type JetStreamErrors = typeof JetStreamErrors.Type;

/** @since 0.1.0 @category constructors */
export const wrongLastSequence = (options: { readonly cause: unknown; readonly expected?: number }) =>
  new WrongLastSequenceError({ cause: options.cause, expected: Option.fromNullishOr(options.expected) });
