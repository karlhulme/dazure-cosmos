import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";

interface CosmosPartitionKey {
  paths: string[];
  kind: "Hash";
}

interface CosmosCollection {
  id: string;
  partitionKey: CosmosPartitionKey;
}

export async function getCollection(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
): Promise<CosmosCollection> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "GET",
    resourceType: "colls",
    resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
  });

  const collection = await cosmosRetryable(async () => {
    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}`,
      {
        headers: {
          Authorization: reqHeaders.authorizationHeader,
          "x-ms-date": reqHeaders.xMsDateHeader,
          "content-type": "application/json",
          "x-ms-version": reqHeaders.xMsVersion,
        },
      },
    );

    handleCosmosTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(
        `Unable to get collection ${databaseName}/${collectionName}.\n${await response
          .text()}`,
      );
    }

    const result = await response.json() as CosmosCollection;

    return result;
  });

  return collection;
}
