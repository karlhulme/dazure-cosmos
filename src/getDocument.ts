import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

interface GetDocumentOptions {
  sessionToken?: string;
}

interface GetDocumentResult {
  doc: Record<string, unknown> | null;
  requestCharge: number;
  requestDurationMilliseconds: number;
}

export async function getDocument(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  partition: string,
  documentId: string,
  options: GetDocumentOptions,
): Promise<GetDocumentResult> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "GET",
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
