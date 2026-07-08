import { Option } from "effect";
import {
  AuthorizationError,
  ClosedConnectionError,
  DrainingConnectionError,
  InvalidSubjectError,
  NoRespondersError,
  ProtocolError,
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

const mapError = (cause: Error): NatsError.NatsError => {
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
