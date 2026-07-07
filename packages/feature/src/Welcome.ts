import { Effect } from "effect";
import * as Greeting from "@effect-nats/core/Greeting";

/**
 * Compose the core package from another workspace package.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = (options: { readonly name: string }): Effect.Effect<string> =>
  Greeting.make(options).pipe(Effect.map((greeting) => `${greeting.message} Welcome to Effect.`));
