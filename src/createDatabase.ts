import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";

/**
 * Creates a new database.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to the database.
 * @param databaseName The name of the new database.
 */
export async function createDatabase(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
) {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "POST",
    resourceType: "dbs",
  });

  await cosmosRetryable(async () => {
    const response = await fetch(`${cosmosUrl}/dbs`, {
      method: "POST",
      headers: {
        Authorization: reqHeaders.authorizationHeader,
        "x-ms-date": reqHeaders.xMsDateHeader,
        "content-type": "application/json",
        "x-ms-version": reqHeaders.xMsVersion,
      },
      body: JSON.stringify({
        id: databaseName,
      }),
    });

    ensureRaisingOfTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(`Unable to create database.\n${await response.text()}`);
    }

    await response.body?.cancel();
  });
}
