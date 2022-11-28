import { retryable } from "../deps.ts";

/**
 * The default retry strategy for cosmos operations.
 */
export const CosmosDefaultRetryStrategy: number[] = [
  100,
  200,
  300,
  400,
  500,
  1000,
  2000,
  3000,
  5000,
];

/**
 * Executes the given operation using the cosmos retry strategy.
 * @param operation An asynchronous operation that queries a
 * Cosmos database.
 */
export function cosmosRetryable<T>(operation: () => Promise<T>): Promise<T> {
  return retryable(operation, {
    retryIntervalsInMilliseconds: CosmosDefaultRetryStrategy,
  });
}
