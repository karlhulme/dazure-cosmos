import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

/**
 * Returns true if the document was deleted.  Returns false if the document
 * does not exist.  In all other cases an error is raised.
 */
export async function deleteDocument(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  partition: string,
  documentId: string,
): Promise<boolean> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "DELETE",
    resourceType: "docs",
    resourceLink:
      `dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`,
  });

  const didDelete = await cosmosRetryable(async () => {
    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`,
      {
        method: "DELETE",
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
        `Unable to delete document ${databaseName}/${collectionName}/${documentId}.\n${await response
          .text()}`,
      );
    }

    await response.body?.cancel();

    return response.ok;
  });

  return didDelete;
}
