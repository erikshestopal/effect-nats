import { Option } from "effect";
import {
  AuthorizationError,
  ClosedConnectionError,
  DrainingConnectionError,
  InvalidSubjectError,
  NoRespondersError,
  PermissionViolationError,
  ProtocolError,
  RequestError,
  TimeoutError,
  UserAuthenticationExpiredError,
} from "@nats-io/nats-core";
import * as NatsError from "../NatsError.ts";

export const mapConnectError = (cause: unknown): NatsError.ConnectionError | NatsError.TimeoutError => {
  // ast-grep-ignore: no-instanceof
  if (cause instanceof TimeoutError) {
    return new NatsError.TimeoutError();
  }
  return new NatsError.ConnectionError({ cause });
};

export const mapClosed = (cause: void | Error): Option.Option<NatsError.NatsError> => {
  if (cause) {
    return Option.some(mapError(cause));
  }
  return Option.none();
};

export const mapPublishError = (options: {
  readonly subject: string;
  readonly cause: unknown;
}):
  | NatsError.InvalidSubjectError
  | NatsError.ClosedConnectionError
  | NatsError.DrainingConnectionError
  | NatsError.PermissionViolationError => {
  const cause = options.cause;
  // ast-grep-ignore: no-instanceof
  if (cause instanceof InvalidSubjectError) {
    return new NatsError.InvalidSubjectError({ subject: options.subject });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof ClosedConnectionError) {
    return new NatsError.ClosedConnectionError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof DrainingConnectionError) {
    return new NatsError.DrainingConnectionError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof PermissionViolationError) {
    return new NatsError.PermissionViolationError({
      operation: cause.operation,
      subject: cause.subject,
      queue: Option.fromNullishOr(cause.queue),
    });
  }
  throw cause;
};

export const mapFlushError = (cause: unknown): NatsError.ClosedConnectionError | NatsError.TimeoutError => {
  // ast-grep-ignore: no-instanceof
  if (cause instanceof TimeoutError) {
    return new NatsError.TimeoutError();
  }
  return new NatsError.ClosedConnectionError();
};

export const mapRttError = (
  cause: unknown,
): NatsError.ClosedConnectionError | NatsError.DrainingConnectionError | NatsError.TimeoutError => {
  // ast-grep-ignore: no-instanceof
  if (cause instanceof TimeoutError) {
    return new NatsError.TimeoutError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof DrainingConnectionError) {
    return new NatsError.DrainingConnectionError();
  }
  return new NatsError.ClosedConnectionError();
};

export const mapRequestError = (options: { readonly subject: string; readonly cause: unknown }) => {
  const cause = options.cause;
  // ast-grep-ignore: no-instanceof
  if (cause instanceof TimeoutError) {
    return new NatsError.TimeoutError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof NoRespondersError) {
    return new NatsError.NoRespondersError({ subject: cause.subject });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof InvalidSubjectError) {
    return new NatsError.InvalidSubjectError({ subject: options.subject });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof ClosedConnectionError) {
    return new NatsError.ClosedConnectionError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof DrainingConnectionError) {
    return new NatsError.DrainingConnectionError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof RequestError) {
    // ast-grep-ignore: no-instanceof
    if (cause.cause instanceof NoRespondersError) {
      return new NatsError.NoRespondersError({ subject: cause.cause.subject });
    }
    // ast-grep-ignore: no-instanceof
    if (cause.cause instanceof TimeoutError) {
      return new NatsError.TimeoutError();
    }
    return new NatsError.RequestError({ subject: options.subject, cause });
  }
  return new NatsError.RequestError({ subject: options.subject, cause });
};

export const mapError = (cause: unknown): NatsError.NatsError => {
  // ast-grep-ignore: no-instanceof
  if (cause instanceof TimeoutError) {
    return new NatsError.TimeoutError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof ClosedConnectionError) {
    return new NatsError.ClosedConnectionError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof DrainingConnectionError) {
    return new NatsError.DrainingConnectionError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof AuthorizationError) {
    return new NatsError.AuthorizationError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof UserAuthenticationExpiredError) {
    return new NatsError.UserAuthenticationExpiredError();
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof ProtocolError) {
    return new NatsError.ProtocolError({ cause });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof InvalidSubjectError) {
    return new NatsError.InvalidSubjectError({ subject: cause.message });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof NoRespondersError) {
    return new NatsError.NoRespondersError({ subject: cause.subject });
  }
  return new NatsError.ConnectionError({ cause });
};
