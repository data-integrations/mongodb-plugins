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
package io.cdap.plugin.batch.sink;

import io.cdap.plugin.batch.MongoDBConfigBuilder;
import io.cdap.plugin.batch.source.MongoDBBatchSource;

/**
 * Builder class that provides handy methods to construct {@link MongoDBBatchSource.MongoDBSourceConfig} for testing.
 */
public class MongoDBSinkConfigBuilder extends MongoDBConfigBuilder<MongoDBSinkConfigBuilder> {

  private String idField;

  public static MongoDBSinkConfigBuilder builder() {
    return new MongoDBSinkConfigBuilder();
  }

  public static MongoDBSinkConfigBuilder builder(MongoDBBatchSink.MongoDBSinkConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setHost(original.getHost())
      .setPort(original.getPort())
      .setDatabase(original.getDatabase())
      .setCollection(original.getCollection())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setConnectionArguments(original.getConnectionArguments())
      .setIdField(original.getIdField());
  }

  public MongoDBSinkConfigBuilder setIdField(String idField) {
    this.idField = idField;
    return this;
  }

  public MongoDBBatchSink.MongoDBSinkConfig build() {
    return new MongoDBBatchSink.MongoDBSinkConfig(referenceName, host, port, database, collection, user, password,
                                                  connectionArguments, idField);
  }
}
