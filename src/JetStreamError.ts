/**
 * Typed JetStream errors.
 *
 * @since 0.1.0
 */
import { Option, Schema } from "effect";

export class JetStreamNotEnabledError extends Schema.TaggedErrorClass<JetStreamNotEnabledError>(
  "effect-nats/JetStreamError/JetStreamNotEnabledError",
)("JetStreamNotEnabledError", { cause: Schema.Defect() }) {}

export class JetStreamError extends Schema.TaggedErrorClass<JetStreamError>(
  "effect-nats/JetStreamError/JetStreamError",
)("JetStreamError", { cause: Schema.Defect() }) {}

export class JetStreamStatusError extends Schema.TaggedErrorClass<JetStreamStatusError>(
  "effect-nats/JetStreamError/JetStreamStatusError",
)("JetStreamStatusError", { code: Schema.Finite, description: Schema.String }) {}

export class JetStreamApiError extends Schema.TaggedErrorClass<JetStreamApiError>(
  "effect-nats/JetStreamError/JetStreamApiError",
)("JetStreamApiError", { code: Schema.Finite, status: Schema.Finite, description: Schema.String }) {}

export class StreamNotFoundError extends Schema.TaggedErrorClass<StreamNotFoundError>(
  "effect-nats/JetStreamError/StreamNotFoundError",
)("StreamNotFoundError", { stream: Schema.String }) {}

export class ConsumerNotFoundError extends Schema.TaggedErrorClass<ConsumerNotFoundError>(
  "effect-nats/JetStreamError/ConsumerNotFoundError",
)("ConsumerNotFoundError", { stream: Schema.String, consumer: Schema.Option(Schema.String) }) {}

export class InvalidNameError extends Schema.TaggedErrorClass<InvalidNameError>(
  "effect-nats/JetStreamError/InvalidNameError",
)("InvalidNameError", { name: Schema.String }) {}

export class WrongLastSequenceError extends Schema.TaggedErrorClass<WrongLastSequenceError>(
  "effect-nats/JetStreamError/WrongLastSequenceError",
)("WrongLastSequenceError", { expected: Schema.Option(Schema.Finite), cause: Schema.Defect() }) {}

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

export type JetStreamErrors = typeof JetStreamErrors.Type;

export const wrongLastSequence = (options: { readonly cause: unknown; readonly expected?: number }) =>
  new WrongLastSequenceError({ cause: options.cause, expected: Option.fromNullishOr(options.expected) });
