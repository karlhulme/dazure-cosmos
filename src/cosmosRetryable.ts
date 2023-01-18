import { retryable } from "../deps.ts";

/**
 * The default retry strategy for cosmos operations
 * which is based 9 attempts with a backoff strategy
 * that will last approximately 12.5 seconds in total.
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
    isErrorTransient: isCosmosTransientError,
  });
}

/**
 * Returns true if the given error appears to be a Cosmos specific
 * transient issue that may be resolved if we simply try executing
 * the query again.
 * @param err An error.
 */
export function isCosmosTransientError(err: Error) {
  // Indicates the headers produced by generateCosmosReqHeaders
  // have expired and need to be generated again.
  return err.message.includes(
    "authorization token is not valid at the current time",
  );
}
