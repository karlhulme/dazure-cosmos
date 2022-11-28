import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";

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

    handleCosmosTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(`Unable to create database.\n${await response.text()}`);
    }

    await response.body?.cancel();
  });
}
