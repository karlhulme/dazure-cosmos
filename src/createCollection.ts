import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";

export async function createCollection(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
) {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "POST",
    resourceType: "colls",
    resourceLink: `dbs/${databaseName}`,
  });

  await cosmosRetryable(async () => {
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

    handleCosmosTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(`Unable to create collection.\n${await response.text()}`);
    }

    await response.body?.cancel();
  });
}
