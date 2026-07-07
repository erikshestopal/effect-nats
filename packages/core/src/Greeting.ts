import { Effect, Schema } from "effect";

/**
 * A small schema-backed model used by the starter package.
 *
 * @since 1.0.0
 * @category models
 */
export class Greeting extends Schema.Class<Greeting>("Greeting")({
  message: Schema.String,
}) {}

/**
 * Build a greeting in Effect so new packages start with the project style.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = (options: { readonly name: string }): Effect.Effect<Greeting> =>
  Effect.succeed(Greeting.make({ message: `Hello, ${options.name}!` }));
