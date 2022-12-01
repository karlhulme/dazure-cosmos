import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";

/**
 * Returns the names of the databases for a given
 * cosmos instance.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 */
export async function listDatabases(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
): Promise<string[]> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "GET",
    resourceType: "dbs",
  });

  const list = await cosmosRetryable(async () => {
    const response = await fetch(`${cosmosUrl}/dbs`, {
      headers: {
        Authorization: reqHeaders.authorizationHeader,
        "x-ms-date": reqHeaders.xMsDateHeader,
        "content-type": "application/json",
        "x-ms-version": reqHeaders.xMsVersion,
      },
    });

    handleCosmosTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(`Unable to list databases.\n${await response.text()}`);
    }

    const result = await response.json();

    return result.Databases.map((db: { id: string }) => db.id);
  });

  return list;
}
