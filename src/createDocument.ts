import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

/**
 * The options to use when creating the document.
 */
interface CreateDocumentOptions {
  /**
   * True if the document is to be upserted over an original.
   * If not set, the document must not exist otherwise the
   * operation will fail.
   */
  upsertDocument?: boolean;

  /**
   * A session token.
   */
  sessionToken?: string;
}

/**
 * The result of creating a new document.
 */
interface CreateDocumentResult {
  /**
   * True if a new document was created.
   */
  didCreate: boolean;

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
 * Creates a new document.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to the database.
 * @param databaseName The name of the database.
 * @param collectionName The name of the collection.
 * @param partition The partition for the new document.
 * @param document The data for the new document.
 * @param options A property bag of options.
 */
export async function createDocument(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  partition: string,
  document: Record<string, unknown>,
  options: CreateDocumentOptions,
): Promise<CreateDocumentResult> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "POST",
    resourceType: "docs",
    resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
  });

  const result = await cosmosRetryable(async () => {
    const optionalHeaders: Record<string, string> = {};

    if (options.upsertDocument) {
      optionalHeaders["x-ms-documentdb-is-upsert"] = "True";
    }

    if (options.sessionToken) {
      optionalHeaders["x-ms-session-token"] = options.sessionToken;
    }

    if (document.partitionKey !== partition) {
      document.partitionKey = partition;
    }

    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}/docs`,
      {
        method: "POST",
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

    if (!response.ok) {
      throw new Error(
        `Unable to create document ${databaseName}/${collectionName}/${document.id}.\n${await response
          .text()}`,
      );
    }

    await response.body?.cancel();

    return {
      didCreate: response.status === 201,
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
