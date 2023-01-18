import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";

/**
 * Creates a new collection.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to the database.
 * @param databaseName The name of a database.
 * @param collectionName The name for the new collection
 */
export async function createCollection(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
) {
  await cosmosRetryable(async () => {
    const reqHeaders = await generateCosmosReqHeaders({
      key: cryptoKey,
      method: "POST",
      resourceType: "colls",
      resourceLink: `dbs/${databaseName}`,
    });

    const response = await fetch(`${cosmosUrl}/dbs/${databaseName}/colls`, {
      method: "POST",
      headers: {
        Authorization: reqHeaders.authorizationHeader,
        "x-ms-date": reqHeaders.xMsDateHeader,
        "content-type": "application/json",
        "x-ms-version": reqHeaders.xMsVersion,
      },
      body: JSON.stringify({
        id: collectionName,
        partitionKey: {
          paths: [
            "/partitionKey",
          ],
          kind: "Hash",
          Version: 2,
        },
      }),
    });

    ensureRaisingOfTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(`Unable to create collection.\n${await response.text()}`);
    }

    await response.body?.cancel();
  });
}
