import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

export async function getDocument(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  partition: string,
  documentId: string,
): Promise<Record<string, unknown> | null> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "GET",
    resourceType: "docs",
    resourceLink:
      `dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`,
  });

  const doc = await cosmosRetryable(async () => {
    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`,
      {
        headers: {
          Authorization: reqHeaders.authorizationHeader,
          "x-ms-date": reqHeaders.xMsDateHeader,
          "content-type": "application/json",
          "x-ms-version": reqHeaders.xMsVersion,
          "x-ms-documentdb-partitionkey": formatPartitionKeyValue(
            partition,
          ),
        },
      },
    );

    handleCosmosTransitoryErrors(response);

    if (!response.ok && response.status !== 404) {
      throw new Error(
        `Unable to get document ${databaseName}/${collectionName}/${documentId}.\n${await response
          .text()}`,
      );
    }

    if (response.status === 404) {
      await response.body?.cancel();
      return null;
    } else {
      const result = await response.json() as Record<string, unknown>;
      return result;
    }
  });

  return doc;
}
