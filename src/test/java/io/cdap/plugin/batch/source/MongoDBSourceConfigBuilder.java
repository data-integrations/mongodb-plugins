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
package io.cdap.plugin.batch.source;

import io.cdap.plugin.ErrorHandling;
import io.cdap.plugin.batch.MongoDBConfigBuilder;

/**
 * Builder class that provides handy methods to construct {@link MongoDBBatchSource.MongoDBSourceConfig} for testing.
 */
public class MongoDBSourceConfigBuilder extends MongoDBConfigBuilder<MongoDBSourceConfigBuilder> {

  private String schema;
  private String inputQuery;
  private String onError;
  private String authConnectionString;

  public static MongoDBSourceConfigBuilder builder() {
    return new MongoDBSourceConfigBuilder();
  }

  public static MongoDBSourceConfigBuilder builder(MongoDBBatchSource.MongoDBSourceConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setHost(original.getHost())
      .setPort(original.getPort())
      .setDatabase(original.getDatabase())
      .setCollection(original.getCollection())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setConnectionArguments(original.getConnectionArguments())
      .setSchema(original.getSchema())
      .setOnError(original.getErrorHandling())
      .setInputQuery(original.getInputQuery())
      .setAuthConnectionString(original.getAuthConnectionString());
  }

  public MongoDBSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public MongoDBSourceConfigBuilder setOnError(ErrorHandling onError) {
    this.onError = onError.getDisplayName();
    return this;
  }

  public MongoDBSourceConfigBuilder setInputQuery(String inputQuery) {
    this.inputQuery = inputQuery;
    return this;
  }

  public MongoDBSourceConfigBuilder setAuthConnectionString(String authConnectionString) {
    this.authConnectionString = authConnectionString;
    return this;
  }

  public MongoDBBatchSource.MongoDBSourceConfig build() {
    return new MongoDBBatchSource.MongoDBSourceConfig(referenceName, host, port, database, collection, user, password,
                                                      connectUsingSRVString, connectionArguments, schema, inputQuery,
                                                      onError, authConnectionString);
  }
}
