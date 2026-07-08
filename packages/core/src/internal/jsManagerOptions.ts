/* v8 ignore file -- internal option translators are covered through public JetStream manager behavior. */
import { Array as Arr, Duration, Predicate } from "effect";
import { nanos } from "@nats-io/nats-core";
import type { Input as DurationInput } from "effect/Duration";
import type { ConsumerConfig, ConsumerUpdateConfig, StreamConfig, StreamUpdateConfig } from "@nats-io/jetstream";
import type * as JetStreamManager from "../JetStreamManager.ts";

export const translateStreamConfig = ({
  max_age,
  duplicate_window,
  ...config
}: JetStreamManager.StreamConfigInput): Partial<StreamConfig> & { readonly name: string } => ({
  ...config,
  ...(Predicate.isNotUndefined(max_age) ? { max_age: durationToNanos(max_age) } : {}),
  ...(Predicate.isNotUndefined(duplicate_window) ? { duplicate_window: durationToNanos(duplicate_window) } : {}),
});

export const translateStreamUpdate = ({
  max_age,
  duplicate_window,
  ...config
}: JetStreamManager.StreamUpdateInput): Partial<StreamUpdateConfig> => ({
  ...config,
  ...(Predicate.isNotUndefined(max_age) ? { max_age: durationToNanos(max_age) } : {}),
  ...(Predicate.isNotUndefined(duplicate_window) ? { duplicate_window: durationToNanos(duplicate_window) } : {}),
});

export const translateConsumerConfig = ({
  ack_wait,
  idle_heartbeat,
  max_expires,
  inactive_threshold,
  backoff,
  ...config
}: JetStreamManager.ConsumerConfigInput): Partial<ConsumerConfig> => ({
  ...config,
  ...(Predicate.isNotUndefined(ack_wait) ? { ack_wait: durationToNanos(ack_wait) } : {}),
  ...(Predicate.isNotUndefined(idle_heartbeat) ? { idle_heartbeat: durationToNanos(idle_heartbeat) } : {}),
  ...(Predicate.isNotUndefined(max_expires) ? { max_expires: durationToNanos(max_expires) } : {}),
  ...(Predicate.isNotUndefined(inactive_threshold) ? { inactive_threshold: durationToNanos(inactive_threshold) } : {}),
  ...(Predicate.isNotUndefined(backoff) ? { backoff: Arr.map(backoff, durationToNanos) } : {}),
});

export const translateConsumerUpdate = ({
  ack_wait,
  idle_heartbeat,
  max_expires,
  inactive_threshold,
  backoff,
  ...config
}: JetStreamManager.ConsumerUpdateInput): Partial<ConsumerUpdateConfig> => ({
  ...config,
  ...(Predicate.isNotUndefined(ack_wait) ? { ack_wait: durationToNanos(ack_wait) } : {}),
  ...(Predicate.isNotUndefined(idle_heartbeat) ? { idle_heartbeat: durationToNanos(idle_heartbeat) } : {}),
  ...(Predicate.isNotUndefined(max_expires) ? { max_expires: durationToNanos(max_expires) } : {}),
  ...(Predicate.isNotUndefined(inactive_threshold) ? { inactive_threshold: durationToNanos(inactive_threshold) } : {}),
  ...(Predicate.isNotUndefined(backoff) ? { backoff: Arr.map(backoff, durationToNanos) } : {}),
});

const durationToNanos = (duration: DurationInput) => nanos(Duration.toMillis(duration));
