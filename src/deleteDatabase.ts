import { generateCosmosReqHeaders } from "./generateCosmosReqHeaders.ts";
import { cosmosRetryable } from "./cosmosRetryable.ts";
import { handleCosmosTransitoryErrors } from "./handleCosmosTransitoryErrors.ts";

interface DeleteDatabaseResult {
  didDelete: boolean;
}

export async function deleteDatabase(
  cryptoKey: CryptoKey,
  cosmosUrl: string,
  databaseName: string,
): Promise<DeleteDatabaseResult> {
  const reqHeaders = await generateCosmosReqHeaders({
    key: cryptoKey,
    method: "DELETE",
    resourceType: "dbs",
    resourceLink: `dbs/${databaseName}`,
  });

  const result = await cosmosRetryable(async () => {
    const response = await fetch(`${cosmosUrl}/dbs/${databaseName}`, {
      method: "DELETE",
      headers: {
        Authorization: reqHeaders.authorizationHeader,
        "x-ms-date": reqHeaders.xMsDateHeader,
        "content-type": "application/json",
        "x-ms-version": reqHeaders.xMsVersion,
      },
    });

    handleCosmosTransitoryErrors(response);

    if (!response.ok && response.status !== 404) {
      throw new Error(`Unable to delete database.\n${await response.text()}`);
    }

    await response.body?.cancel();

    return {
      didDelete: response.ok,
    };
  });

  return result;
}
