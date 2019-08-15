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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * {@link MongoDBBatchSink} test.
 */
public class MongoDBBatchSinkTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final MongoDBBatchSink.MongoDBSinkConfig VALID_CONFIG = MongoDBSinkConfigBuilder.builder()
    .setReferenceName("MongoDBSink")
    .setHost("localhost")
    .setPort(27017)
    .setDatabase("admin")
    .setCollection("analytics")
    .setUser("admin")
    .setPassword("password")
    .setConnectionArguments("key=value;")
    .build();

  private static final MongoDBBatchSink SINK = new MongoDBBatchSink(VALID_CONFIG);

  @Test
  public void testInvalidInputSchema() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("_id", Schema.of(Schema.Type.NULL)));

    thrown.expect(InvalidStageException.class);
    SINK.configurePipeline(new MockPipelineConfigurer(schema));
  }

  @Test
  public void testInvalidRecordInputSchema() {
    Schema nestedRecord = Schema.recordOf("nested", Schema.Field.of("invalid", Schema.of(Schema.Type.NULL)));
    Schema schema = Schema.recordOf("schema", Schema.Field.of("nested", nestedRecord));

    thrown.expect(InvalidStageException.class);
    SINK.configurePipeline(new MockPipelineConfigurer(schema));
  }

  @Test
  public void testNullableMapKeyInputSchema() {
    Schema mapSchema = Schema.mapOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)), Schema.of(Schema.Type.STRING));
    Schema schema = Schema.recordOf("schema", Schema.Field.of("map", mapSchema));

    thrown.expect(InvalidStageException.class);
    SINK.configurePipeline(new MockPipelineConfigurer(schema));
  }

  @Test
  public void testInvalidMapKeyInputSchema() {
    Schema mapSchema = Schema.mapOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING));
    Schema schema = Schema.recordOf("schema", Schema.Field.of("map", mapSchema));

    thrown.expect(InvalidStageException.class);
    SINK.configurePipeline(new MockPipelineConfigurer(schema));
  }

  @Test
  public void testInvalidMapValueInputSchema() {
    Schema mapSchema = Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL));
    Schema schema = Schema.recordOf("schema", Schema.Field.of("map", mapSchema));

    thrown.expect(InvalidStageException.class);
    SINK.configurePipeline(new MockPipelineConfigurer(schema));
  }

  @Test
  public void testInvalidArrayInputSchema() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.NULL))));

    thrown.expect(InvalidStageException.class);
    SINK.configurePipeline(new MockPipelineConfigurer(schema));
  }

  @Test
  public void testIdFieldIsNotInTheSchema() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    MongoDBBatchSink.MongoDBSinkConfig config = MongoDBSinkConfigBuilder.builder(VALID_CONFIG)
      .setIdField("not_in_the_schema")
      .build();
    MongoDBBatchSink sink = new MongoDBBatchSink(config);

    thrown.expect(InvalidStageException.class);
    sink.configurePipeline(new MockPipelineConfigurer(schema));
  }

  @Test
  public void testIdFieldConflictsWithDefaultId() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("_id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    MongoDBBatchSink.MongoDBSinkConfig config = MongoDBSinkConfigBuilder.builder(VALID_CONFIG)
      .setIdField("name")
      .build();
    MongoDBBatchSink sink = new MongoDBBatchSink(config);

    thrown.expect(InvalidStageException.class);
    sink.configurePipeline(new MockPipelineConfigurer(schema));
  }
}
