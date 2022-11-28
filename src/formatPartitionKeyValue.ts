/**
 * Formats the given value into a partition key array header value.
 * @param value A string or number value.
 */
export function formatPartitionKeyValue(value: string | number) {
  if (typeof value === "string") {
    return `["${value}"]`;
  } else {
    return `[${value}]`;
  }
}
