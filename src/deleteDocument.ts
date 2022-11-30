import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

interface DeleteDocumentOptions {
  sessionToken?: string;
}

interface DeleteDocumentResult {
  didDelete: boolean;
  sessionToken: string;
  requestCharge: number;
  requestDurationMilliseconds: number;
}

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
  options: DeleteDocumentOptions,
): Promise<DeleteDocumentResult> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "DELETE",
    resourceType: "docs",
    resourceLink:
      `dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`,
  });

  const optionalHeaders: Record<string, string> = {};

  if (options.sessionToken) {
    optionalHeaders["x-ms-session-token"] = options.sessionToken;
  }

  const result = await cosmosRetryable(async () => {
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
          ...optionalHeaders,
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

    return {
      didDelete: response.ok,
      sessionToken: response.headers.get("x-ms-session-token") as string,
      requestCharge: parseFloat(
        response.headers.get("x-ms-request-charge") as string,
      ),
      requestDurationMilliseconds: parseFloat(
        response.headers.get("x-ms-request-duration-ms") as string,
      ),
    };
  });

  return result;
}
