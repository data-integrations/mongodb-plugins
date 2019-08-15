# MongoDB Batch Sink


Description
-----------
This sink writes to a MongoDB collection.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MongoDB is running on.

**Port:** Port that MongoDB is listening to.

**Database:** MongoDB database name.

**Collection:** Name of the database collection to write to.

**ID Field:** Which input field should be used as the document identifier. Identifier is expected to be a
[12-byte value] or it's 24-byte [hexadecimal string] representation. If none is given, an identifier will be
automatically generated.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. See
[Connection String Options] for a full description of these arguments.

[Connection String Options]:
https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options

[12-byte value]:
https://docs.mongodb.com/manual/reference/bson-types/#objectid

[hexadecimal string]:
https://docs.mongodb.com/manual/reference/method/ObjectId/#specify-a-hexadecimal-string

Data Types Mapping
----------
	
    | CDAP Schema Data Type | MongoDB Data Type     | Comment                                            |
    | --------------------- | --------------------- | -------------------------------------------------- |
    | boolean               | Boolean               |                                                    |
    | bytes                 | Binary data, ObjectId | 12-byte value of the identifier field is mapped to |
    |                       |                       | ObjectId.                                          |
    | string                | String, ObjectId      | 24-byte hexadecimal string value of the identifier |
    |                       |                       | field is mapped to ObjectId.                       |
    | date                  | Date                  |                                                    |
    | double                | Double                |                                                    |
    | decimal               | Decimal128            |                                                    |
    | float                 | Double                |                                                    |
    | int                   | 32-bit integer        |                                                    |
    | long                  | 64-bit integer        |                                                    |
    | time                  | String                | Time string in the following format: HH:mm:ss.SSS  |
    | timestamp             | Date                  |                                                    |
    | array                 | Array                 |                                                    |
    | record                | Object                |                                                    |
    | enum                  | String                |                                                    |
    | map                   | Object                |                                                    |
    | union                 |                       | Depends on the actual value. For example, if it's  |
    |                       |                       | a union ["string","int","long"] and the value is   |
    |                       |                       | actually a long, the MongoDB document will have    |
    |                       |                       | the field as a 64-bit integer. If a different      |
    |                       |                       | record comes in with the value as a string, the    |
    |                       |                       | MongoDB document will end up with a String for     |
    |                       |                       | that field.                                        |
