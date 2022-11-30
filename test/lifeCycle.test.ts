import { assertEquals } from "../deps.ts";
import {
  convertCosmosKeyToCryptoKey,
  createCollection,
  createDatabase,
  createDocument,
  deleteCollection,
  deleteDatabase,
  deleteDocument,
  getDocument,
  listDatabases,
  replaceDocument,
} from "../src/index.ts";

const testCosmosUrl = Deno.env.get("COSMOS_URL");

if (!testCosmosUrl) {
  throw new Error("COSMOS_URL is not defined.");
}

const testCosmosKey = Deno.env.get("COSMOS_KEY");

if (!testCosmosKey) {
  throw new Error("COSMOS_KEY is not defined.");
}

Deno.test("Create a database and a collection, manipulate some documents, and tear down again.", async () => {
  const tempDb = "lib-temp";
  const tempCol = "lifecyle";

  const cryptoKey = await convertCosmosKeyToCryptoKey(testCosmosKey);

  const databaseNames = await listDatabases(cryptoKey, testCosmosUrl);

  // Ensure the temp db does not exist before we start.
  if (databaseNames.includes(tempDb)) {
    await deleteDatabase(cryptoKey, testCosmosUrl, tempDb);
  }

  await createDatabase(cryptoKey, testCosmosUrl, tempDb);

  await createCollection(cryptoKey, testCosmosUrl, tempDb, tempCol);

  const newDocId = crypto.randomUUID();

  const createDocResult = await createDocument(
    cryptoKey,
    testCosmosUrl,
    tempDb,
    tempCol,
    "_central",
    {
      id: newDocId,
      foo: "bar",
    },
    {},
  );

  assertEquals(createDocResult.didCreate, true);

  const getDocResult = await getDocument(
    cryptoKey,
    testCosmosUrl,
    tempDb,
    tempCol,
    "_central",
    newDocId,
    {
      sessionToken: createDocResult.sessionToken,
    },
  );

  assertEquals(getDocResult.doc?.foo, "bar");

  const replaceDocResult = await replaceDocument(
    cryptoKey,
    testCosmosUrl,
    tempDb,
    tempCol,
    "_central",
    {
      id: newDocId,
      foo: "bar2",
    },
    {
      sessionToken: createDocResult.sessionToken,
    },
  );

  assertEquals(replaceDocResult.didReplace, true);

  const deleteDocResult = await deleteDocument(
    cryptoKey,
    testCosmosUrl,
    tempDb,
    tempCol,
    "_central",
    newDocId,
    {
      sessionToken: replaceDocResult.sessionToken,
    },
  );

  assertEquals(deleteDocResult.didDelete, true);

  const deleteColResult = await deleteCollection(
    cryptoKey,
    testCosmosUrl,
    tempDb,
    tempCol,
  );

  assertEquals(deleteColResult.didDelete, true);

  const deleteDbResult = await deleteDatabase(cryptoKey, testCosmosUrl, tempDb);

  assertEquals(deleteDbResult.didDelete, true);
});
