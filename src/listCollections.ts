import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";

/**
 * Returns an array of the collection names for
 * the given database.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 */
export async function listCollections(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
): Promise<string[]> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "GET",
    resourceType: "colls",
    resourceLink: `dbs/${databaseName}`,
  });

  const list = await cosmosRetryable(async () => {
    const response = await fetch(`${cosmosUrl}/dbs/${databaseName}/colls`, {
      headers: {
        Authorization: reqHeaders.authorizationHeader,
        "x-ms-date": reqHeaders.xMsDateHeader,
        "content-type": "application/json",
        "x-ms-version": reqHeaders.xMsVersion,
      },
    });

    handleCosmosTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(`Unable to list collections.\n${await response.text()}`);
    }

    const result = await response.json();

    return result.DocumentCollections.map((col: { id: string }) => col.id);
  });

  return list;
}
