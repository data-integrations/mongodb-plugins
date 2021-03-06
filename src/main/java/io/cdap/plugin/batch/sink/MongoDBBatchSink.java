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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.MongoDBConfig;
import io.cdap.plugin.MongoDBConstants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} that writes data to MongoDB.
 * This {@link MongoDBBatchSink} takes a {@link StructuredRecord} in,
 * converts it to {@link BSONWritable}, and writes it to MongoDB.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MongoDBConstants.PLUGIN_NAME)
@Description("MongoDB Batch Sink writes to a MongoDB collection.")
public class MongoDBBatchSink extends ReferenceBatchSink<StructuredRecord, NullWritable, BSONWritable> {

  private final MongoDBSinkConfig config;
  private RecordToBSONWritableTransformer transformer;
  private static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.ARRAY, Schema.Type.BOOLEAN,
                                                                                 Schema.Type.BYTES, Schema.Type.STRING,
                                                                                 Schema.Type.DOUBLE, Schema.Type.FLOAT,
                                                                                 Schema.Type.INT, Schema.Type.LONG,
                                                                                 Schema.Type.RECORD, Schema.Type.ENUM,
                                                                                 Schema.Type.MAP, Schema.Type.UNION);

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DATE, Schema.LogicalType.DECIMAL, Schema.LogicalType.TIME_MILLIS, Schema.LogicalType.TIME_MICROS,
    Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS);

  public MongoDBBatchSink(MongoDBSinkConfig config) {
    super(new ReferencePluginConfig(config.getReferenceName()));
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      validateInputSchema(inputSchema);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.validate();
    Configuration conf = new Configuration();
    String path = conf.get(
      "mapreduce.task.tmp.dir",
      conf.get(
        "mapred.child.tmp",
        conf.get("hadoop.tmp.dir", System.getProperty("java.io.tmpdir")))) + "/" + UUID.randomUUID().toString();

    emitLineage(context);
    context.addOutput(Output.of(config.getReferenceName(), new MongoDBOutputFormatProvider(config, path)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    transformer = new RecordToBSONWritableTransformer(config.idField);
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<NullWritable, BSONWritable>> emitter) {
    BSONWritable bsonWritable = transformer.transform(record);
    emitter.emit(new KeyValue<>(NullWritable.get(), bsonWritable));
  }

  private void validateInputSchema(Schema inputSchema) {
    try {
      config.validateSchema(inputSchema, SUPPORTED_LOGICAL_TYPES, SUPPORTED_SIMPLE_TYPES);
    } catch (IllegalArgumentException e) {
      throw new InvalidStageException("Invalid input schema", e);
    }
  }

  private void emitLineage(BatchSinkContext context) {
    if (Objects.nonNull(context.getInputSchema())) {
      LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
      lineageRecorder.createExternalDataset(context.getInputSchema());
      List<Schema.Field> fields = context.getInputSchema().getFields();
      if (fields != null && !fields.isEmpty()) {
        lineageRecorder.recordWrite("Write",
                                    String.format("Wrote to '%s' MongoDB collection.", config.getCollection()),
                                    fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
      }
    }
  }

  private static class MongoDBOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    MongoDBOutputFormatProvider(MongoDBConfig config, String path) {
      this.conf = new HashMap<>();
      conf.put("mongo.output.uri", config.getConnectionString());
      conf.put("mapreduce.task.tmp.dir", path);
    }

    @Override
    public String getOutputFormatClassName() {
      return MongoOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * Config class for {@link MongoDBBatchSink}.
   */
  public static class MongoDBSinkConfig extends MongoDBConfig {

    @Name(MongoDBConstants.ID_FIELD)
    @Nullable
    @Description("Allows to specify which of the incoming fields should be used as an document identifier. " +
      "Identifier will be generated if no value is specified.")
    @Macro
    private String idField;

    public MongoDBSinkConfig(String referenceName, String host, int port, String database, String collection,
                             String user, String password, String connectionArguments, String idField) {
      super(referenceName, host, port, database, collection, user, password, connectionArguments);
      this.idField = idField;
    }

    @Nullable
    public String getIdField() {
      return idField;
    }

    @Override
    public void validateSchema(Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                               Set<Schema.Type> supportedTypes) {
      super.validateSchema(schema, supportedLogicalTypes, supportedTypes);
      if (!containsMacro(MongoDBConstants.ID_FIELD) && !Strings.isNullOrEmpty(idField)) {
        if (schema.getField(idField) == null) {
          throw new InvalidConfigPropertyException(String.format("Schema does not contain identifier field '%s'.",
                                                                 idField), MongoDBConstants.ID_FIELD);
        }
        if (!MongoDBConstants.DEFAULT_ID_FIELD_NAME.equals(idField) &&
          schema.getField(MongoDBConstants.DEFAULT_ID_FIELD_NAME) != null) {
          throw new InvalidConfigPropertyException(
            String.format("Schema already contains identifier field '%s'.", MongoDBConstants.DEFAULT_ID_FIELD_NAME),
            MongoDBConstants.ID_FIELD);
        }
      }
    }
  }
}
