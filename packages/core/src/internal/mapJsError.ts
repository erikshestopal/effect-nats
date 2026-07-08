import { Option, Predicate } from "effect";
import {
  JetStreamApiCodes,
  JetStreamApiError as SdkJetStreamApiError,
  JetStreamError as SdkJetStreamError,
} from "@nats-io/jetstream";
import * as JetStreamError from "../JetStreamError.ts";
import * as NatsError from "../NatsError.ts";
import * as NatsErrors from "./errors.ts";

export const mapPublishError = (cause: unknown) => {
  const nats = NatsErrors.mapError(cause);
  if (NatsError.isNatsError(nats) && Predicate.isTagged(nats, "TimeoutError")) {
    return nats;
  }

  if (Predicate.isError(cause) && cause.name === "JetStreamNotEnabled") {
    return new JetStreamError.JetStreamNotEnabledError({ cause });
  }
  if (Predicate.isError(cause) && cause.name === "InvalidNameError") {
    return new JetStreamError.JetStreamError({ cause });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof SdkJetStreamApiError) {
    const apiError = cause.apiError();
    if (
      cause.code === JetStreamApiCodes.StreamWrongLastSequence ||
      cause.code === JetStreamApiCodes.StreamWrongLastSequenceUnknown
    ) {
      return JetStreamError.wrongLastSequence({ cause });
    }
    return new JetStreamError.JetStreamApiError({
      code: cause.code,
      status: cause.status,
      description: apiError.description,
    });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof SdkJetStreamError) {
    return new JetStreamError.JetStreamError({ cause });
  }
  return new JetStreamError.JetStreamError({ cause });
};

export const mapCreateManagerError = (cause: unknown) => {
  const nats = NatsErrors.mapError(cause);
  if (NatsError.isNatsError(nats) && Predicate.isTagged(nats, "TimeoutError")) {
    return nats;
  }

  if (Predicate.isError(cause) && cause.name === "JetStreamNotEnabled") {
    return new JetStreamError.JetStreamNotEnabledError({ cause });
  }
  return new JetStreamError.JetStreamNotEnabledError({ cause });
};

export const mapStreamError = (stream: string) => (cause: unknown) => {
  // ast-grep-ignore: no-instanceof
  if (cause instanceof SdkJetStreamApiError) {
    const apiError = cause.apiError();
    if (cause.code === JetStreamApiCodes.StreamNotFound) {
      return new JetStreamError.StreamNotFoundError({ stream });
    }
    return new JetStreamError.JetStreamApiError({
      code: cause.code,
      status: cause.status,
      description: apiError.description,
    });
  }
  return mapJetStreamError(cause);
};

export const mapStreamInfoError = (stream: string) => (cause: unknown) => {
  // ast-grep-ignore: no-instanceof
  if (cause instanceof SdkJetStreamApiError) {
    const apiError = cause.apiError();
    if (cause.code === JetStreamApiCodes.StreamNotFound) {
      return new JetStreamError.StreamNotFoundError({ stream });
    }
    return new JetStreamError.JetStreamApiError({
      code: cause.code,
      status: cause.status,
      description: apiError.description,
    });
  }
  return new JetStreamError.JetStreamApiError({ code: 0, status: 0, description: "unknown JetStream error" });
};

export const mapConsumerError =
  (options: { readonly stream: string; readonly consumer?: string }) => (cause: unknown) => {
    // ast-grep-ignore: no-instanceof
    if (cause instanceof SdkJetStreamApiError) {
      const apiError = cause.apiError();
      if (cause.code === JetStreamApiCodes.StreamNotFound) {
        return new JetStreamError.StreamNotFoundError({ stream: options.stream });
      }
      if (cause.code === JetStreamApiCodes.ConsumerNotFound) {
        return new JetStreamError.ConsumerNotFoundError({
          stream: options.stream,
          consumer: Option.fromNullishOr(options.consumer),
        });
      }
      return new JetStreamError.JetStreamApiError({
        code: cause.code,
        status: cause.status,
        description: apiError.description,
      });
    }
    // ast-grep-ignore: no-instanceof
    if (cause instanceof SdkJetStreamError) {
      return new JetStreamError.JetStreamError({ cause });
    }
    return new JetStreamError.JetStreamError({ cause });
  };

export const mapConsumerInfoError =
  (options: { readonly stream: string; readonly consumer: string }) => (cause: unknown) => {
    // ast-grep-ignore: no-instanceof
    if (cause instanceof SdkJetStreamApiError) {
      const apiError = cause.apiError();
      if (cause.code === JetStreamApiCodes.ConsumerNotFound) {
        return new JetStreamError.ConsumerNotFoundError({
          stream: options.stream,
          consumer: Option.some(options.consumer),
        });
      }
      return new JetStreamError.JetStreamApiError({
        code: cause.code,
        status: cause.status,
        description: apiError.description,
      });
    }
    return new JetStreamError.JetStreamApiError({ code: 0, status: 0, description: "unknown JetStream error" });
  };

export const mapJetStreamError = (cause: unknown) => {
  // ast-grep-ignore: no-instanceof
  if (cause instanceof SdkJetStreamApiError) {
    const apiError = cause.apiError();
    return new JetStreamError.JetStreamApiError({
      code: cause.code,
      status: cause.status,
      description: apiError.description,
    });
  }
  // ast-grep-ignore: no-instanceof
  if (cause instanceof SdkJetStreamError) {
    return new JetStreamError.JetStreamError({ cause });
  }
  return new JetStreamError.JetStreamError({ cause });
};
