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
package io.cdap.plugin.batch;

import io.cdap.plugin.MongoDBConfig;

/**
 * Builder class that provides handy methods to construct {@link MongoDBConfig} for testing.
 */
public class MongoDBConfigBuilder<T extends MongoDBConfigBuilder> {

  protected String referenceName;
  protected String host;
  protected int port;
  protected String database;
  protected String collection;
  protected String user;
  protected String password;
  protected boolean connectUsingSRVString;
  protected String connectionArguments;

  public static MongoDBConfigBuilder builder() {
    return new MongoDBConfigBuilder();
  }

  public static MongoDBConfigBuilder builder(MongoDBConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setHost(original.getHost())
      .setPort(original.getPort())
      .setDatabase(original.getDatabase())
      .setCollection(original.getCollection())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setConnectionArguments(original.getConnectionArguments());
  }

  public T setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return (T) this;
  }

  public T setHost(String host) {
    this.host = host;
    return (T) this;
  }

  public T setPort(Integer port) {
    this.port = port;
    return (T) this;
  }

  public T setDatabase(String database) {
    this.database = database;
    return (T) this;
  }

  public T setCollection(String collection) {
    this.collection = collection;
    return (T) this;
  }

  public T setUser(String user) {
    this.user = user;
    return (T) this;
  }

  public T setPassword(String password) {
    this.password = password;
    return (T) this;
  }

  public T setConnectUsingSRVString(boolean connectUsingSRVString) {
    this.connectUsingSRVString = connectUsingSRVString;
    return (T) this;
  }

  public T setConnectionArguments(String connectionArguments) {
    this.connectionArguments = connectionArguments;
    return (T) this;
  }

  public MongoDBConfig build() {
    return new MongoDBConfig(referenceName, host, port, database, collection, user, password,
                             connectUsingSRVString, connectionArguments);
  }
}
