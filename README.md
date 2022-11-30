# dazure-cosmos

A set of functions for interacting with Azure Cosmos DB.

Support for:

- Importing a Cosmos key.
- Listing, creating and deleting databases.
- Listing, creating and deleting collections.
- Retrieving, replacing and deleting individual documents.
- Querying a container using the gateway.
- Querying multiple physicl containers using pkranges.

## Todo

- Complete test coverage
- Complete comments

## Environment variables

You will need to set the following environment variables to run the tests:

- **COSMOS_URL** The url to the Cosmos db instance, e.g.
  https://myapp.documents.azure.net
- **COSMOS_KEY** The value of the shared access key.

## Commands

Run `deno task test` to test and format.
