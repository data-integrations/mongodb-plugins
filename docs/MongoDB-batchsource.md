# MongoDB Batch Source


Description
-----------
Reads documents from a MongoDB collection and converts each document into a StructuredRecord with the help
of a specified schema. The user can optionally provide input query.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MongoDB is running on. Enter the hostname only. The connection string is constructed by the plugin.

**Port:** Port that MongoDB is listening to.

**Database:** MongoDB database name.

**Collection:** Name of the database collection to read from.

**Output Schema:** Specifies the schema of the documents.

**Input Query:** Optionally filter the input collection with a query. This query must be represented in JSON format
and use the [MongoDB extended JSON format] to represent non-native JSON data types.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Authentication Connection String:** Optional MongoDB connection string to connect to the 'config' database of a
sharded cluster. It can be omitted if username and password do not differ from the previously provided ones or if
'config' database does not require authentication.

**On Record Error:** Specifies how to handle error in record processing. An error will be thrown if failed to parse
value according to a provided schema.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. See
[Connection String Options] for a full description of these arguments.

[MongoDB extended JSON format]:
http://docs.mongodb.org/manual/reference/mongodb-extended-json/

[Connection String Options]:
https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options


Data Types Mapping
----------

    | MongoDB Data Type              | CDAP Schema Data Type | Comment                                            |
    | ------------------------------ | --------------------- | -------------------------------------------------- |
    | Boolean                        | boolean               |                                                    |
    | Binary data                    | bytes                 |                                                    |
    | ObjectId                       | bytes                 |                                                    |
    | Double                         | double                |                                                    |
    | Decimal128                     | decimal               |                                                    |
    | 32-bit integer                 | int                   |                                                    |
    | 64-bit integer                 | long                  |                                                    |
    | String                         | string                |                                                    |
    | Symbol                         | string                |                                                    |
    | Date                           | timestamp             |                                                    |
    | Array                          | array                 |                                                    |
    | Object                         | record, map           | Map keys must be a non-nullable string.            |
    | Regular Expression             |                       | Is not supported.                                  |
    | DBPointer                      |                       | Is not supported.                                  |
    | JavaScript                     |                       | Is not supported.                                  |
    | JavaScript (with scope)        |                       | Is not supported.                                  |
    | Timestamp                      |                       | Is not supported.                                  |
    |                                |                       | Special type for internal MongoDB use.             |
    | Min key                        |                       | Is not supported.                                  |
    | Max key                        |                       | Is not supported.                                  |
