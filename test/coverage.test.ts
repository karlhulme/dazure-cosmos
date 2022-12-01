import { assert, assertEquals } from "../deps.ts";
import {
  convertCosmosKeyToCryptoKey,
  getDocument,
  listCollections,
  listDatabases,
  queryDocumentsContainersDirect,
  queryDocumentsGateway,
} from "../src/index.ts";

const testCosmosUrl = Deno.env.get("COSMOS_URL");

if (!testCosmosUrl) {
  throw new Error("COSMOS_URL is not defined.");
}

const testCosmosKey = Deno.env.get("COSMOS_KEY");

if (!testCosmosKey) {
  throw new Error("COSMOS_KEY is not defined.");
}

Deno.test("List databases and collections.", async () => {
  const cryptoKey = await convertCosmosKeyToCryptoKey(testCosmosKey);

  const databaseNames = await listDatabases(cryptoKey, testCosmosUrl);

  assert(databaseNames.includes("deno"));

  const collectionNames = await listCollections(
    cryptoKey,
    testCosmosUrl,
    "deno",
  );

  assertEquals(
    collectionNames.sort(),
    ["albums", "movies"],
  );
});

Deno.test("Query documents using gateway.", async () => {
  const cryptoKey = await convertCosmosKeyToCryptoKey(testCosmosKey);

  const queryResult = await queryDocumentsGateway(
    cryptoKey,
    testCosmosUrl,
    "deno",
    "movies",
    "_central",
    "SELECT d.id FROM Docs d WHERE d.id = @docId",
    [{
      name: "@docId",
      value: "007",
    }],
    {},
  );

  assertEquals(queryResult.records.length, 1);
  assert(queryResult.requestCharge > 0);
  assert(queryResult.requestDurationMilliseconds > 0);
});

Deno.test("Query documents using containers directly.", async () => {
  const cryptoKey = await convertCosmosKeyToCryptoKey(testCosmosKey);

  const queryResult = await queryDocumentsContainersDirect(
    cryptoKey,
    testCosmosUrl,
    "deno",
    "albums",
    "SELECT VALUE SUM(d.trackCount) FROM Docs d WHERE d.id = @id1 OR d.id = @id2 OR d.id = @id3",
    [
      { name: "@id1", value: "001" },
      { name: "@id2", value: "002" },
      { name: "@id3", value: "003" },
    ],
    "sum",
    {},
  );

  assertEquals(queryResult.data, 42);
});

Deno.test("Get single document.", async () => {
  const cryptoKey = await convertCosmosKeyToCryptoKey(testCosmosKey);

  const getDocResult = await getDocument(
    cryptoKey,
    testCosmosUrl,
    "deno",
    "movies",
    "_central",
    "007",
    {},
  );

  assertEquals(getDocResult.doc?.title, "No time to die");
  assert(getDocResult.requestCharge > 0);
  assert(getDocResult.requestDurationMilliseconds > 0);
});
