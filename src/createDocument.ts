import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

interface CreateDocumentOptions {
  upsertDocument?: boolean;
  sessionToken?: string;
}

interface CreateDocumentResult {
  didCreate: boolean;
  sessionToken: string;
  requestCharge: number;
  requestDurationMilliseconds: number;
}

/**
 * Returns true if a new document was created.  Returns false if an
 * existing document was replaced because the upsert option was specified.
 * In all other cases an error is raised.
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
