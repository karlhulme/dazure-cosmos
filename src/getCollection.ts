import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { ensureRaisingOfTransitoryErrors } from "./ensureRaisingOfTransitoryErrors.ts";

/**
 * Represents the definition of a partition key.
 */
interface CosmosPartitionKey {
  /**
   * The paths of a partition key.
   */
  paths: string[];

  /**
   * The type of a partition key.
   */
  kind: "Hash";
}

/**
 * Information about a cosmos collection.
 */
interface CosmosCollection {
  /**
   * The id of a collection.
   */
  id: string;

  /**
   * The definition of the partition key for a collection.
   */
  partitionKey: CosmosPartitionKey;
}

/**
 * Returns partition key information for a collection.
 * @param cryptoKey A crypto key.
 * @param cosmosUrl A url to a database.
 * @param databaseName The name of a database.
 * @param collectionName The name of a collection.
 */
export async function getCollection(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
  collectionName: string,
): Promise<CosmosCollection> {
  const collection = await cosmosRetryable(async () => {
    const reqHeaders = await generateCosmosReqHeaders({
      key: cryptoKey,
      method: "GET",
      resourceType: "colls",
      resourceLink: `dbs/${databaseName}/colls/${collectionName}`,
    });

    const response = await fetch(
      `${cosmosUrl}/dbs/${databaseName}/colls/${collectionName}`,
      {
        headers: {
          Authorization: reqHeaders.authorizationHeader,
          "x-ms-date": reqHeaders.xMsDateHeader,
          "content-type": "application/json",
          "x-ms-version": reqHeaders.xMsVersion,
        },
      },
    );

    ensureRaisingOfTransitoryErrors(response);

    if (!response.ok) {
      throw new Error(
        `Unable to get collection ${databaseName}/${collectionName}.\n${await response
          .text()}`,
      );
    }

    const result = await response.json() as CosmosCollection;

    return result;
  });

  return collection;
}
