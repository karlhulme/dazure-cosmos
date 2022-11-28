import { decodeBase64 } from "../deps.ts";

/**
 * Returns a CryptoKey based on the cosmos string key.
 * @param cosmosKey A cosmos master key string.
 */
export async function convertCosmosKeyToCryptoKey(
  cosmosKey: string,
): Promise<CryptoKey> {
  const keyBuffer = decodeBase64(cosmosKey);

  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    keyBuffer,
    { name: "HMAC", hash: "SHA-256" },
    true,
    ["sign"],
  );

  return cryptoKey;
}
