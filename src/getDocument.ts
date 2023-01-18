import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

/**
 * The options for fetching a document.
 */
interface GetDocumentOptions {
  /**
   * A session token.
   */
  sessionToken?: string;
}

/**
 * The result of fetching a document.
 */
interface GetDocumentResult {
  /**
   * The document or null if the document was not found.
   */
  doc: Record<string, unknown> | null;

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
 * Fetches a document based on it's id.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 * @param collectionName The name of a collection.
 * @param partition A partition key value.
 * @param documentId The id of a document.
 * @param options A property bag of options.
 */
export async function getDocument(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  partition: string,
  documentId: string,
  options: GetDocumentOptions,
): Promise<GetDocumentResult> {
  const optionalHeaders: Record<string, string> = {};

  if (options.sessionToken) {
    optionalHeaders["x-ms-session-token"] = options.sessionToken;
  }

  const result = await cosmosRetryable(async () => {
    const reqHeaders = await generateCosmosReqHeaders({
      key: cryptoKey,
      method: "GET",
      resourceType: "docs",
      resourceLink:
        `dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`,
    });

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
          ...optionalHeaders,
        },
      },
    );

    ensureRaisingOfTransitoryErrors(response);

    if (!response.ok && response.status !== 404) {
      throw new Error(
        `Unable to get document ${databaseName}/${collectionName}/${documentId}.\n${await response
          .text()}`,
      );
    }

    const requestCharge = parseFloat(
      response.headers.get("x-ms-request-charge") as string,
    );

    const requestDurationMilliseconds = parseFloat(
      response.headers.get("x-ms-request-duration-ms") as string,
    );

    if (response.status === 404) {
      await response.body?.cancel();
      return {
        doc: null,
        requestCharge,
        requestDurationMilliseconds,
      };
    } else {
      const doc = await response.json() as Record<string, unknown>;
      return {
        doc,
        requestCharge,
        requestDurationMilliseconds,
      };
    }
  });

  return result;
}
