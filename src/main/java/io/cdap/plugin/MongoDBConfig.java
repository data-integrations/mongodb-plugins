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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that MongoDB Source and Sink can re-use.
 */
public class MongoDBConfig extends PluginConfig {

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Name(MongoDBConstants.HOST)
  @Description("Host that MongoDB is running on.")
  @Macro
  private String host;

  @Name(MongoDBConstants.PORT)
  @Description("Port that MongoDB is listening to.")
  @Macro
  @Nullable
  private Integer port;

  @Name(MongoDBConstants.DATABASE)
  @Description("MongoDB database name.")
  @Macro
  private String database;

  @Name(MongoDBConstants.COLLECTION)
  @Description("Name of the database collection.")
  @Macro
  private String collection;

  @Name(MongoDBConstants.USER)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Macro
  @Nullable
  private String user;

  @Name(MongoDBConstants.PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Macro
  @Nullable
  private String password;

  @Name(MongoDBConstants.CONNECT_USING_SRV_STRING)
  @Description("Toggle to determine whether to use an SRV connection string for MongoDB. It can be " +
    "enabled if the MongoDB deployment supports SRV DNS records for connection resolution.")
  @Macro
  @Nullable
  private boolean connectUsingSRVString;

  @Name(MongoDBConstants.CONNECTION_ARGUMENTS)
  @Description("A list of arbitrary string key/value pairs as connection arguments.")
  @Macro
  @Nullable
  private String connectionArguments;

  public MongoDBConfig(String referenceName, String host, int port, String database, String collection, String user,
                       String password, boolean connectUsingSRVString, String connectionArguments) {
    this.referenceName = referenceName;
    this.host = host;
    this.port = port;
    this.database = database;
    this.collection = collection;
    this.user = user;
    this.password = password;
    this.connectUsingSRVString = connectUsingSRVString;
    this.connectionArguments = connectionArguments;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getHost() {
    return host;
  }

  @Nullable
  public Integer getPort() {
    return port;
  }

  public String getDatabase() {
    return database;
  }

  public String getCollection() {
    return collection;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public boolean connectUsingSRVString() {
    return connectUsingSRVString;
  }

  @Nullable
  public String getConnectionArguments() {
    return connectionArguments;
  }

  /**
   * Validates {@link MongoDBConfig} instance.
   */
  public void validate() {
    if (Strings.isNullOrEmpty(referenceName)) {
      throw new InvalidConfigPropertyException("Reference name must be specified", Constants.Reference.REFERENCE_NAME);
    } else {
      try {
        IdUtils.validateId(referenceName);
      } catch (IllegalArgumentException e) {
        // InvalidConfigPropertyException should be thrown instead of IllegalArgumentException
        throw new InvalidConfigPropertyException("Invalid reference name", e, Constants.Reference.REFERENCE_NAME);
      }
    }
    if (!containsMacro(MongoDBConstants.HOST) && Strings.isNullOrEmpty(host)) {
      throw new InvalidConfigPropertyException("Host must be specified", MongoDBConstants.HOST);
    }
    if ((!containsMacro(MongoDBConstants.CONNECT_USING_SRV_STRING) && !connectUsingSRVString) &&
      !containsMacro(MongoDBConstants.PORT)) {
      if (port < 1) {
        throw new InvalidConfigPropertyException("Port number must be greater than 0", MongoDBConstants.PORT);
      }
    }
    if (!containsMacro(MongoDBConstants.DATABASE) && Strings.isNullOrEmpty(database)) {
      throw new InvalidConfigPropertyException("Database name must be specified", MongoDBConstants.DATABASE);
    }
    if (!containsMacro(MongoDBConstants.COLLECTION) && Strings.isNullOrEmpty(collection)) {
      throw new InvalidConfigPropertyException("Collection name must be specified", MongoDBConstants.COLLECTION);
    }
  }

  /**
   * Constructs a connection string such as: "mongodb://admin:password@localhost:27017/admin.analytics?key=value;"
   * using host, port, username, password, database, collection and optional connection properties.
   * If SRV is enabled, the connection string will use the "mongodb+srv://" protocol instead of "mongodb://".
   * In the case when username or password is not provided, the connection string will not contain credentials:
   * "mongodb://localhost:27017/admin.analytics?key=value;"
   * When SRV is not used, the port will be included in the connection string.
   *
   * @return connection string.
   */
  public String getConnectionString() {
    StringBuilder connectionStringBuilder = new StringBuilder();
    if (connectUsingSRVString()) {
      connectionStringBuilder.append("mongodb+srv://");
    } else {
      connectionStringBuilder.append("mongodb://");
    }
    if (!Strings.isNullOrEmpty(user) || !Strings.isNullOrEmpty(password)) {
      connectionStringBuilder.append(user).append(":").append(password).append("@");
    }
    connectionStringBuilder.append(host);
    if (!connectUsingSRVString()) {
      connectionStringBuilder.append(":").append(port);
    }
    connectionStringBuilder.append("/").append(database).append(".").append(collection);
    if (!Strings.isNullOrEmpty(connectionArguments)) {
      connectionStringBuilder.append("?").append(connectionArguments);
    }
    return connectionStringBuilder.toString();
  }

  /**
   * Validates given input/output schema according the the specified supported types. Fields of types
   * {@link Schema.Type#RECORD}, {@link Schema.Type#ARRAY}, {@link Schema.Type#MAP} will be validated recursively.
   *
   * @param schema                schema to validate.
   * @param supportedLogicalTypes set of supported logical types.
   * @param supportedTypes        set of supported types.
   * @throws IllegalArgumentException in the case when schema is invalid.
   */
  public void validateSchema(Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                             Set<Schema.Type> supportedTypes) {

    Preconditions.checkNotNull(supportedLogicalTypes, "Supported logical types can not be null");
    Preconditions.checkNotNull(supportedTypes, "Supported types can not be null");
    if (schema == null) {
      throw new IllegalArgumentException("Schema must be specified");
    }
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    validateRecordSchema(null, nonNullableSchema, supportedLogicalTypes, supportedTypes);
  }

  private void validateRecordSchema(@Nullable String fieldName, Schema schema,
                                    Set<Schema.LogicalType> supportedLogicalTypes, Set<Schema.Type> supportedTypes) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("Schema must contain fields");
    }
    for (Schema.Field field : fields) {
      // Use full field name for nested records to construct meaningful errors messages.
      // Full field names will be in the following format: 'record_field_name.nested_record_field_name'
      String fullFieldName = fieldName != null ? String.format("%s.%s", fieldName, field.getName()) : field.getName();
      validateFieldSchema(fullFieldName, field.getSchema(), supportedLogicalTypes, supportedTypes);
    }
  }

  private void validateFieldSchema(String fieldName, Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                                   Set<Schema.Type> supportedTypes) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.Type type = nonNullableSchema.getType();
    switch (type) {
      case RECORD:
        validateRecordSchema(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
        break;
      case ARRAY:
        validateArraySchema(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
        break;
      case MAP:
        validateMapSchema(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
        break;
      default:
        validateSchemaType(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
    }
  }

  private void validateMapSchema(String fieldName, Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                                 Set<Schema.Type> supportedTypes) {
    Schema keySchema = schema.getMapSchema().getKey();
    if (keySchema.isNullable()) {
      throw new IllegalArgumentException(String.format(
        "Map keys must be a non-nullable string. Please change field '%s' to be a non-nullable string.", fieldName));
    }
    if (keySchema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException(String.format(
        "Map keys must be a non-nullable string. Please change field '%s' to be a non-nullable string.", fieldName));
    }
    validateFieldSchema(fieldName, schema.getMapSchema().getValue(), supportedLogicalTypes, supportedTypes);
  }

  private void validateArraySchema(String fieldName, Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                                   Set<Schema.Type> supportedTypes) {
    Schema componentSchema = schema.getComponentSchema().isNullable() ? schema.getComponentSchema().getNonNullable()
      : schema.getComponentSchema();
    validateFieldSchema(fieldName, componentSchema, supportedLogicalTypes, supportedTypes);
  }

  private void validateSchemaType(String fieldName, Schema fieldSchema, Set<Schema.LogicalType> supportedLogicalTypes,
                                  Set<Schema.Type> supportedTypes) {
    Schema.Type type = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (supportedTypes.contains(type) || supportedLogicalTypes.contains(logicalType)) {
      return;
    }

    String supportedTypeNames = Stream.concat(supportedTypes.stream(), supportedLogicalTypes.stream())
      .map(Enum::name)
      .map(String::toLowerCase)
      .collect(Collectors.joining(", "));

    String actualTypeName = logicalType != null ? logicalType.name().toLowerCase() : type.name().toLowerCase();
    throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s.",
                                                     fieldName, actualTypeName, supportedTypeNames));
  }
}
