/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;

/**
 * MongoDB constants.
 */
public class MongoDBConstants {

  private MongoDBConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * MongoDB plugin name.
   */
  public static final String PLUGIN_NAME = "MongoDB";

  /**
   * Configuration property name used to specify host that MongoDB is running on.
   */
  public static final String HOST = "host";

  /**
   * Configuration property name used to specify port that MongoDB is listening to.
   */
  public static final String PORT = "port";

  /**
   * Configuration property name used to specify MongoDB database name.
   */
  public static final String DATABASE = "database";

  /**
   * Configuration property name used to specify name of the database collection.
   */
  public static final String COLLECTION = "collection";

  /**
   * Configuration property name used to specify the schema of the documents.
   */
  public static final String SCHEMA = "schema";

  /**
   * Configuration property name used to specify query to filter the input collection. This query must be represented
   * in JSON format and use the
   * <a href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">MongoDB extended JSON format</a>
   * to represent non-native JSON data types.
   */
  public static final String INPUT_QUERY = "inputQuery";

  /**
   * Configuration property name used to specify how to handle error in record processing. An error will be thrown if
   * failed to parse value according to a provided schema..
   */
  public static final String ON_ERROR = "on-error";

  /**
   * Configuration property name used to specify user identity for connecting to the specified database.
   */
  public static final String USER = "user";

  /**
   * Configuration property name used to specify password to use to connect to the specified database.
   */
  public static final String PASSWORD = "password";

  /**
   * Configuration property name used to specify auxiliary MongoDB connection string to authenticate against when
   * constructing splits.
   */
  public static final String AUTH_CONNECTION_STRING = "authConnectionString";

  /**
   * Configuration property name used to specify a list of arbitrary string key/value pairs as connection arguments.
   */
  public static final String CONNECTION_ARGUMENTS = "connectionArguments";

  /**
   * Configuration property name used to specify which of the incoming fields should be used as an object identifier.
   */
  public static final String ID_FIELD = "idField";

  /**
   * Default identifier field name.
   */
  public static final String DEFAULT_ID_FIELD_NAME = "_id";
}
