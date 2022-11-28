import { encodeBase64 } from "../deps.ts";

/**
 * The type of signature being used.
 */
const COSMOS_SIGN_TYPE = "master";

/**
 * The version of the signing process being used.
 */
const COSMOS_SIGN_VERSION = "1.0";

/**
 * The version of the API.
 */
const COSMOS_API_VERSION = "2018-12-31";

/**
 * The Cosmos resources that can be queried.
 */
type CosmosResourceType =
  | "dbs"
  | "colls"
  | "docs"
  | "sprocs"
  | "udfs"
  | "triggers"
  | "users"
  | "permissions"
  | "attachments"
  | "media"
  | "conflicts"
  | "pkranges"
  | "offers"
  | "";

/**
 * The Http method being invoked on a Cosmos resource.
 */
type CosmosMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

/**
 * The properties required to generate the authorization headers.
 */
export interface GenerateCosmosReqHeadersProps {
  /**
   * A CryptoKey based on the Cosmos master key.
   */
  key: CryptoKey;

  /**
   * The Http method being invoked.
   */
  method: CosmosMethod;

  /**
   * The type of resource being queried.
   */
  resourceType: CosmosResourceType;

  /**
   * A resource link string.  For getting or updating a specific resource
   * it should point directly to that resource, e.g. dbs/mydb/colls/mycol/docs/doc1.
   * For listing resources, it should point to the parent, e.g. dbs/mydb to access
   * a list of collections.
   * For creating databases this value can be omitted.
   * Full description here:
   * https://docs.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources#constructkeytoken.
   */
  resourceLink?: string;

  /**
   * The date and time that the request is being made.
   * If not supplied, then the current date and time will be used.
   */
  date?: Date;
}

/**
 * The authorization headers that are generated.
 */
export interface GenerateCosmosReqHeadersResult {
  /**
   * The "Authorization" header.
   */
  authorizationHeader: string;

  /**
   * The "x-ms-date" header.
   */
  xMsDateHeader: string;

  /**
   * The "x-ms-version" header.
   */
  xMsVersion: string;
}

/**
 * Returns a set of headers that need to be appended to a request.
 * Based on the access control instructions here:
 * https://docs.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources.
 * @param props The property bag required to build the authorization headers.
 */
export async function generateCosmosReqHeaders(
  props: GenerateCosmosReqHeadersProps,
): Promise<GenerateCosmosReqHeadersResult> {
  const date = props.date || new Date();
  const resourceLink = props.resourceLink || "";
  const encoder = new TextEncoder();

  const text = props.method.toLowerCase() +
    "\n" +
    props.resourceType.toLowerCase() +
    "\n" +
    resourceLink +
    "\n" +
    date.toUTCString().toLowerCase() +
    "\n\n"; // additional new line required at end

  const data = encoder.encode(text);

  const signature = await crypto.subtle.sign("HMAC", props.key, data);

  const authorizationHeader = "type=" + COSMOS_SIGN_TYPE +
    "&ver=" + COSMOS_SIGN_VERSION +
    "&sig=" + encodeURIComponent(encodeBase64(signature));

  return {
    authorizationHeader,
    xMsDateHeader: date.toUTCString(),
    xMsVersion: COSMOS_API_VERSION,
  };
}
