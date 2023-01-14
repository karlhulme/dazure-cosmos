import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

interface DeleteDocumentOptions {
  sessionToken?: string;
}

/**
 * The result of deleting a document.
 */
interface DeleteDocumentResult {
  /**
   * True if a document was deleted.
   */
  didDelete: boolean;

  /**
   * A session token.
   */
  sessionToken: string;

  /**
   * The number of RUs consumed by the request.
   */
  requestCharge: number;

  /**
   * The number of milliseconds spent serving the request.
   */
  requestDurationMilliseconds: number;
}

/**
 * Deletes the document with the given document id.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 * @param collectionName The name of a collection.
 * @param partition A partition key value.
 * @param documentId The id of a document.
 * @param options A property bag of options.
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

    ensureRaisingOfTransitoryErrors(response);

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
