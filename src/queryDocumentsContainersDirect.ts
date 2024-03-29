import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";

// Any query that requires state across continuations cannot be served by the gateway.
// This covers cross-partition queries that require use of TOP, ORDER BY, OFFSET LIMIT,
// Aggregates, DISTINCT or GROUP BY.  In this circumstance we must issue the query
// against each logical container (based on pkrange) and combine the results ourselves.
// More information at this link:
// https://docs.microsoft.com/en-us/rest/api/cosmos-db/querying-cosmosdb-resources-using-the-rest-api

/**
 * Options for querying a document container.
 */
interface QueryDocumentsContainerOptions {
  /**
   * A session token.
   */
  sessionToken?: string;
}

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
 * The result of querying a set of containers directly.
 */
interface QueryDocumentsContainerDirectResult {
  /**
   * An array of values.
   */
  data: unknown;

  /**
   * The resultant cost of the query.
   */
  requestCharge: number;

  /**
   * The duration of the request in milliseconds.
   */
  requestDurationMilliseconds: number;
}

/**
 * Executes the given query on each of the containers.  Each
 * container will return an array of unknown values, whereby
 * each value could be doc store record or a simple value.
 * This function will be combine all the results in a single array
 * and then the given transform function should produce a final set.
 * If transform is not supplied then an array of all results retrived
 * from Cosmos will be returned.
 * @param cryptoKey The crypto key.
 * @param cosmosUrl The cosmos url.
 * @param databaseName The database name.
 * @param collectionName The collection name.
 * @param query The query to execute.  If you're intending to use the 'sum'
 * transform then you typically want a value select statement similar to
 * SELECT VALUE SUM(d.field).
 * @param parameters The parameter to substitute into the query.
 * @param transform A function that is given the results retrieved
 * from each of the logical containers where the query was executed.
 * This function would typically order the result records or for
 * aggregates it would sum/avg the values held in each result element.
 * Although this transform could cull the result set to meet a
 * specific limit, this is rarely useful since the documents have
 * already been retrieved.
 * @param options An options property bag.
 */
export async function queryDocumentsContainersDirect(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  query: string,
  parameters: CosmosQueryParameter[],
  transform: "concatArrays" | "sum",
  options: QueryDocumentsContainerOptions,
): Promise<QueryDocumentsContainerDirectResult> {
  // Retrieve the pk ranges for the collection.  This can change at
  // any time so we perform this lookup on each query.
  // There is potential performance uplift by only doing this once
  // and then responding to errors informing us that the given range
  // has changed.
  const pkRanges = await getPkRangesForContainer(
    cryptoKey,
    cosmosUrl,
    databaseName,
    collectionName,
    options,
  );

  /**
   * Then request the documents from all of the physical partitions
   * simultaenously.  For a small number of physical partitions it's fine
   * to do this, but we may have to throttle the requests if
   * the number of physical partitions is larger.  Note that this will
   * require a cross-partition (i.e. logical partitions) query.
   */
  const promises = pkRanges.map((pkr) =>
    getValueArrayForPkRange(
      cryptoKey,
      cosmosUrl,
      databaseName,
      collectionName,
      pkr,
      query,
      parameters,
      options,
    )
  );

  // Wait for the promises to resolve.
  const arrayOfContainerResults = await Promise.all(promises);

  // Combine the arrays.  Using push may be faster than concat:
  // https://dev.to/uilicious/javascript-array-push-is-945x-faster-than-array-concat-1oki
  let requestCharge = 0.0;
  let requestDurationMilliseconds = 0.0;
  const combinedValueArray: unknown[] = [];

  for (const containerResult of arrayOfContainerResults) {
    combinedValueArray.push(...containerResult.records);
    requestCharge += containerResult.requestCharge;
    requestDurationMilliseconds += containerResult.requestDurationMilliseconds;
  }

  const data = transform === "sum"
    ? combinedValueArray.reduce(
      (agg, cur) => (agg as number) + (cur as number),
      0,
    )
    : combinedValueArray;

  return {
    data,
    requestCharge,
    requestDurationMilliseconds,
  };
}

/**
 * Returns the partition key ranges for a container.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 * @param collectionName The name of a collection.
 * @param options A property bag of options.
 */
async function getPkRangesForContainer(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  options: QueryDocumentsContainerOptions,
) {
  const optionalHeaders: Record<string, string> = {};

  if (options.sessionToken) {
    optionalHeaders["x-ms-session-token"] = options.sessionToken;
  }

  const pkRanges = await cosmosRetryable(async () => {
    const pkRangesReqHeaders = await generateCosmosReqHeaders({
      key: cryptoKey,
      method: "GET",
      resourceType: "pkranges",
      resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
    });

    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}/pkranges`,
      {
        method: "GET",
        headers: {
          Authorization: pkRangesReqHeaders.authorizationHeader,
          "x-ms-date": pkRangesReqHeaders.xMsDateHeader,
          "x-ms-version": pkRangesReqHeaders.xMsVersion,
          ...optionalHeaders,
        },
      },
    );

    ensureRaisingOfTransitoryErrors(response);

    if (!response.ok) {
      const errMsg =
        `Unable to get pk-ranges from collection ${databaseName}/${collectionName}.\n${await response
          .text()}`;

      throw new Error(errMsg);
    }

    const pkRangesFull = await response.json() as {
      _rid: string;
      PartitionKeyRanges: { id: string }[];
    };

    return pkRangesFull.PartitionKeyRanges.map((pkr) =>
      `${pkRangesFull._rid},${pkr.id}`
    );
  });

  return pkRanges;
}

/**
 * Returns the result of executing the given query against
 * the given partition key ranges.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl The url to a database.
 * @param databaseName The name of a database.
 * @param collectionName The name of a collection.
 * @param pkRange A partition key range.
 * @param query A cosmos SQL query.
 * @param parameters An array of cosmos SQL parameters.
 * @param options A property bag of options.
 */
async function getValueArrayForPkRange(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
  pkRange: string,
  query: string,
  parameters: CosmosQueryParameter[],
  options: QueryDocumentsContainerOptions,
) {
  const records: unknown[] = [];

  let continuationToken: string | null = null;
  let requestCharge = 0.0;
  let requestDurationMilliseconds = 0.0;
  let isAllRecordsLoaded = false;

  while (!isAllRecordsLoaded) {
    const optionalHeaders: Record<string, string> = {};

    if (continuationToken) {
      optionalHeaders["x-ms-continuation"] = continuationToken;
    }

    if (options.sessionToken) {
      optionalHeaders["x-ms-session-token"] = options.sessionToken;
    }

    await cosmosRetryable(async () => {
      const reqHeaders = await generateCosmosReqHeaders({
        key: cryptoKey,
        method: "POST",
        resourceType: "docs",
        resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
      });

      const response = await fetch(
        `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}/docs`,
        {
          method: "POST",
          headers: {
            Authorization: reqHeaders.authorizationHeader,
            "x-ms-date": reqHeaders.xMsDateHeader,
            "content-type": "application/query+json",
            "x-ms-version": reqHeaders.xMsVersion,
            "x-ms-documentdb-partitionkeyrangeid": pkRange,
            "x-ms-documentdb-query-enablecrosspartition": "True",
            ...optionalHeaders,
          },
          body: JSON.stringify({
            query,
            parameters,
          }),
        },
      );

      ensureRaisingOfTransitoryErrors(response);

      if (!response.ok) {
        const errMsg =
          `Unable to query collection (container-direct-mode) ${databaseName}/${collectionName}/${pkRange} with query ${query} and parameters ${
            JSON.stringify(parameters)
          }.\n${await response.text()}`;

        throw new Error(errMsg);
      }

      continuationToken = response.headers.get("x-ms-continuation");

      if (!continuationToken) {
        isAllRecordsLoaded = true;
      }

      requestCharge += parseFloat(
        response.headers.get("x-ms-request-charge") as string,
      );

      requestDurationMilliseconds += parseFloat(
        response.headers.get("x-ms-request-duration-ms") as string,
      );

      const result = await response.json();

      records.push(...result.Documents);
    });
  }

  return {
    records,
    requestCharge,
    requestDurationMilliseconds,
  };
}
