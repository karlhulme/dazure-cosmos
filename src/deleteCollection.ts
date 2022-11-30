import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";

interface DeleteCollectionResult {
  didDelete: boolean;
}

export async function deleteCollection(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
): Promise<DeleteCollectionResult> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "DELETE",
    resourceType: "colls",
    resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
  });

  const result = await cosmosRetryable(async () => {
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

    handleCosmosTransitoryErrors(response);

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
