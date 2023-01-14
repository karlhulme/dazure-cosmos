import { OperationTransitoryError } from "../deps.ts";

/**
 * Identifies transitory errors and raises an OperationTransitoryError
 * so that the operation can be retried.  If the response does not contain
 * a transitory error then this function takes no action.
 * @param response A fetch response.
 */
export function ensureRaisingOfTransitoryErrors(response: Response) {
  if (response.status === 429) {
    throw new OperationTransitoryError("Too many requests.");
  }

  if (response.status === 503) {
    throw new OperationTransitoryError("Gateway unavailable.");
  }

  if (response.status === 504) {
    throw new OperationTransitoryError("Gateway time-out.");
  }
}
