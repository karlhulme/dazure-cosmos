import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";
import { formatPartitionKeyValue } from "./formatPartitionKeyValue.ts";

/**
 * A parameter that is substituted into a Cosmos query.
 */
interface CosmosQueryParameter {
  /**
   * The name of a parameter, e.g. @city.
   */
  name: string;

  /**
   * The value of a parameter, e.g. "Bournemouth".
   */
  value: unknown;
}

/**
 * The result of querying documents against a gateway that
 * manages the connection to multiple containers.
 */
interface QueryDocumentsGatewayResult {
  /**
   * An array of records.
   */
  records: Record<string, unknown>[];

  /**
   * The resultant cost of the query.
   */
  queryCharge: number;
}

/**
 * Executes the given query against the gateway.  You should use
 * this function to select sets of documents (modelled as
 * DocStoreRecords) and not use any aggregates such as SUM or TOTAL.
 * To execute a query involving aggregates use queryDocumentsContainersDirect.
 * @param cryptoKey The crypto key.
 * @param cosmosUrl The cosmos url.
 * @param databaseName The database name.
 * @param collectionName The collection name.
 * @param partition The name of a partition.  This is specified to
 * ensure the query can be satisfied by a single container.
 * @param query The query to execute.
 * @param parameters The parameter to substitute into the query.
 */
export async function queryDocumentsGateway(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  partition: string,
  query: string,
  parameters: CosmosQueryParameter[],
): Promise<QueryDocumentsGatewayResult> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "POST",
    resourceType: "docs",
    resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
  });

  const records: Record<string, unknown>[] = [];

  let continuationToken: string | null = null;
  let queryCharge = 0.0;
  let isAllRecordsLoaded = false;

  while (!isAllRecordsLoaded) {
    const optionalHeaders: Record<string, string> = {};

    if (continuationToken) {
      optionalHeaders["x-ms-continuation"] = continuationToken;
    }

    await cosmosRetryable(async () => {
      const response = await fetch(
        `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}/docs`,
        {
          method: "POST",
          headers: {
            Authorization: reqHeaders.authorizationHeader,
            "x-ms-date": reqHeaders.xMsDateHeader,
            "content-type": "application/query+json",
            "x-ms-version": reqHeaders.xMsVersion,
            "x-ms-documentdb-partitionkey": formatPartitionKeyValue(
              partition,
            ),
            ...optionalHeaders,
          },
          body: JSON.stringify({
            query,
            parameters,
          }),
        },
      );

      handleCosmosTransitoryErrors(response);

      if (!response.ok) {
        const errMsg =
          `Unable to query collection (gateway) ${databaseName}/${collectionName} with query ${query} and parameters ${
            JSON.stringify(parameters)
          }.\n${await response.text()}`;

        throw new Error(errMsg);
      }

      continuationToken = response.headers.get("x-ms-continuation");

      if (!continuationToken) {
        isAllRecordsLoaded = true;
      }

      queryCharge += parseFloat(
        response.headers.get("x-ms-request-charge") as string,
      );

      const result = await response.json();

      records.push(...result.Documents);
    });
  }

  return {
    records,
    queryCharge,
  };
}
