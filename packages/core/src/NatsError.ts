/**
 * Typed NATS core errors.
 *
 * @since 0.1.0
 */
import { Schema } from "effect";

export class ConnectionError extends Schema.TaggedErrorClass<ConnectionError>("effect-nats/NatsError/ConnectionError")(
  "ConnectionError",
  { cause: Schema.Defect() },
) {}

export class TimeoutError extends Schema.TaggedErrorClass<TimeoutError>("effect-nats/NatsError/TimeoutError")(
  "TimeoutError",
  {},
) {}

export class ClosedConnectionError extends Schema.TaggedErrorClass<ClosedConnectionError>(
  "effect-nats/NatsError/ClosedConnectionError",
)("ClosedConnectionError", {}) {}

export class DrainingConnectionError extends Schema.TaggedErrorClass<DrainingConnectionError>(
  "effect-nats/NatsError/DrainingConnectionError",
)("DrainingConnectionError", {}) {}

export class NoRespondersError extends Schema.TaggedErrorClass<NoRespondersError>(
  "effect-nats/NatsError/NoRespondersError",
)("NoRespondersError", { subject: Schema.String }) {}

export class RequestError extends Schema.TaggedErrorClass<RequestError>("effect-nats/NatsError/RequestError")(
  "RequestError",
  { subject: Schema.String, cause: Schema.Defect() },
) {}

export class AuthorizationError extends Schema.TaggedErrorClass<AuthorizationError>(
  "effect-nats/NatsError/AuthorizationError",
)("AuthorizationError", {}) {}

export class PermissionViolationError extends Schema.TaggedErrorClass<PermissionViolationError>(
  "effect-nats/NatsError/PermissionViolationError",
)("PermissionViolationError", {
  operation: Schema.Union([Schema.Literal("publish"), Schema.Literal("subscription")]),
  subject: Schema.String,
  queue: Schema.Option(Schema.String),
}) {}

export class ProtocolError extends Schema.TaggedErrorClass<ProtocolError>("effect-nats/NatsError/ProtocolError")(
  "ProtocolError",
  { cause: Schema.Defect() },
) {}

export class InvalidSubjectError extends Schema.TaggedErrorClass<InvalidSubjectError>(
  "effect-nats/NatsError/InvalidSubjectError",
)("InvalidSubjectError", { subject: Schema.String }) {}

export class UserAuthenticationExpiredError extends Schema.TaggedErrorClass<UserAuthenticationExpiredError>(
  "effect-nats/NatsError/UserAuthenticationExpiredError",
)("UserAuthenticationExpiredError", {}) {}

export class NoReplySubjectError extends Schema.TaggedErrorClass<NoReplySubjectError>(
  "effect-nats/NatsError/NoReplySubjectError",
)("NoReplySubjectError", { subject: Schema.String }) {}

export const NatsError = Schema.Union([
  ConnectionError,
  TimeoutError,
  ClosedConnectionError,
  DrainingConnectionError,
  NoRespondersError,
  RequestError,
  AuthorizationError,
  PermissionViolationError,
  ProtocolError,
  InvalidSubjectError,
  UserAuthenticationExpiredError,
  NoReplySubjectError,
]);

export type NatsError = typeof NatsError.Type;

export const isNatsError = Schema.is(NatsError);
