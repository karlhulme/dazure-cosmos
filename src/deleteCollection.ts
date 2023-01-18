import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";

/**
 * The result of deleting a collection.
 */
interface DeleteCollectionResult {
  /**
   * True if a collection was deleted.
   */
  didDelete: boolean;
}

/**
 * Deletes a collection.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 * @param collectionName The name of the collection to delete.
 */
export async function deleteCollection(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
): Promise<DeleteCollectionResult> {
  const result = await cosmosRetryable(async () => {
    const reqHeaders = await generateCosmosReqHeaders({
      key: cryptoKey,
      method: "DELETE",
      resourceType: "colls",
      resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
    });

    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}`,
      {
        method: "DELETE",
        headers: {
          Authorization: reqHeaders.authorizationHeader,
          "x-ms-date": reqHeaders.xMsDateHeader,
          "content-type": "application/json",
          "x-ms-version": reqHeaders.xMsVersion,
        },
      },
    );

    ensureRaisingOfTransitoryErrors(response);

    if (!response.ok && response.status !== 404) {
      throw new Error(`Unable to delete collection.\n${await response.text()}`);
    }

    await response.body?.cancel();

    return {
      didDelete: response.ok,
    };
  });

  return result;
}
