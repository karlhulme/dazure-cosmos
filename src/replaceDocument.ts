import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

/**
 * The result of replacing a document.
 */
interface ReplaceDocumentResult {
  /**
   * True if a document was replaced.
   */
  didReplace: boolean;

  /**
   * A session token
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
 * Options for when replacing a document.
 */
interface ReplaceDocumentOptions {
  /**
   * The etag value that must be present on the existing document
   * if it is to be replaced.  If the document was not replaced
   * then didReplace will be false on the result object.
   */
  ifMatch?: string;

  /**
   * A session token.
   */
  sessionToken?: string;
}

/**
 * Replaces a document with the given document.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 * @param collectionName The name of a collection.
 * @param partition A partition key value.
 * @param document A document.
 * @param options A property bag of options.
 */
export async function replaceDocument(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  partition: string,
  document: Record<string, unknown>,
  options: ReplaceDocumentOptions,
): Promise<ReplaceDocumentResult> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "PUT",
    resourceType: "docs",
    resourceLink:
      `dbs/${databaseName}/colls/${collectionName}/docs/${document.id}`,
  });

  const result = await cosmosRetryable(async () => {
    const optionalHeaders: Record<string, string> = {};

    if (options.ifMatch) {
      optionalHeaders["If-Match"] = options.ifMatch;
    }

    if (options.sessionToken) {
      optionalHeaders["x-ms-session-token"] = options.sessionToken;
    }

    if (document.partitionKey !== partition) {
      document.partitionKey = partition;
    }

    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}/docs/${document.id}`,
      {
        method: "PUT",
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
        body: JSON.stringify(document),
      },
    );

    handleCosmosTransitoryErrors(response);

    // 412 errors means the pre-condition failed and the document
    // wasn't updated.  In this circumstance we cancel reading from
    // the stream and return false.
    if (!response.ok && response.status !== 412) {
      throw new Error(
        `Unable to replace document ${databaseName}/${collectionName}/${document.id}.\n${await response
          .text()}`,
      );
    }

    await response.body?.cancel();

    return {
      didReplace: response.ok,
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
