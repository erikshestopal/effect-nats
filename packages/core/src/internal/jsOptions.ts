/* v8 ignore file -- internal option translators are covered through public JetStream behavior. */
import { Duration, Predicate } from "effect";
import type {
  ConsumeOptions as SdkConsumeOptions,
  FetchOptions as SdkFetchOptions,
  JetStreamOptions as SdkJetStreamOptions,
  JetStreamPublishOptions,
  NextOptions as SdkNextOptions,
} from "@nats-io/jetstream";
import * as JetStream from "../JetStream.ts";
import * as NatsHeaders from "../NatsHeaders.ts";

export const translateOptions = (options: JetStream.JetStreamOptions): SdkJetStreamOptions => ({
  ...(Predicate.isNotUndefined(options.apiPrefix) ? { apiPrefix: options.apiPrefix } : {}),
  ...(Predicate.isNotUndefined(options.domain) ? { domain: options.domain } : {}),
  ...(Predicate.isNotUndefined(options.timeout) ? { timeout: Duration.toMillis(options.timeout) } : {}),
});

export const translatePublishOptions = (options: JetStream.PublishOptions): Partial<JetStreamPublishOptions> => ({
  ...(Predicate.isNotUndefined(options.headers) ? { headers: NatsHeaders.toMsgHdrs(options.headers) } : {}),
  ...(Predicate.isNotUndefined(options.msgID) ? { msgID: options.msgID } : {}),
  ...(Predicate.isNotUndefined(options.timeout) ? { timeout: Duration.toMillis(options.timeout) } : {}),
  ...(Predicate.isNotUndefined(options.retries) ? { retries: options.retries } : {}),
  ...(Predicate.isNotUndefined(options.expect) ? { expect: options.expect } : {}),
});

export const translateNextOptions = (options: JetStream.NextOptions): SdkNextOptions =>
  Predicate.isNotUndefined(options.expires) ? { expires: Duration.toMillis(options.expires) } : {};

export const translateFetchOptions = (options: JetStream.FetchOptions): SdkFetchOptions => ({
  ...(Predicate.isNotUndefined(options.maxMessages) ? { max_messages: options.maxMessages } : {}),
  ...(Predicate.isNotUndefined(options.maxBytes) ? { max_bytes: options.maxBytes } : {}),
  ...(Predicate.isNotUndefined(options.expires) ? { expires: Duration.toMillis(options.expires) } : {}),
});

export const translateConsumeOptions = (options: JetStream.ConsumeOptions): SdkConsumeOptions => ({
  ...(Predicate.isNotUndefined(options.maxMessages) ? { max_messages: options.maxMessages } : {}),
  ...(Predicate.isNotUndefined(options.maxBytes) ? { max_bytes: options.maxBytes } : {}),
  ...(Predicate.isNotUndefined(options.thresholdMessages) ? { threshold_messages: options.thresholdMessages } : {}),
  ...(Predicate.isNotUndefined(options.thresholdBytes) ? { threshold_bytes: options.thresholdBytes } : {}),
  ...(Predicate.isNotUndefined(options.abortOnMissingResource)
    ? { abort_on_missing_resource: options.abortOnMissingResource }
    : {}),
  ...(Predicate.isNotUndefined(options.bind) ? { bind: options.bind } : {}),
});
