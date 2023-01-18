import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";

/**
 * The result of deleting a database.
 */
interface DeleteDatabaseResult {
  /**
   * True if a database was deleted.
   */
  didDelete: boolean;
}

/**
 * Deletes a database.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 */
export async function deleteDatabase(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
): Promise<DeleteDatabaseResult> {
  const result = await cosmosRetryable(async () => {
    const reqHeaders = await generateCosmosReqHeaders({
      key: cryptoKey,
      method: "DELETE",
      resourceType: "dbs",
      resourceLink: `dbs/${databaseName}`,
    });

    const response = await fetch(`${cosmosUrl}/dbs/${databaseName}`, {
      method: "DELETE",
      headers: {
        Authorization: reqHeaders.authorizationHeader,
        "x-ms-date": reqHeaders.xMsDateHeader,
        "content-type": "application/json",
        "x-ms-version": reqHeaders.xMsVersion,
      },
    });

    ensureRaisingOfTransitoryErrors(response);

    if (!response.ok && response.status !== 404) {
      throw new Error(`Unable to delete database.\n${await response.text()}`);
    }

    await response.body?.cancel();

    return {
      didDelete: response.ok,
    };
  });

  return result;
}
