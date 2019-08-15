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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.splitter.MongoSplitter;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.ErrorHandling;
import io.cdap.plugin.MongoDBConstants;
import io.cdap.plugin.batch.sink.MongoDBBatchSink;
import io.cdap.plugin.batch.source.MongoDBBatchSource;
import io.cdap.plugin.common.Constants;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Unit Tests for {@link MongoDBBatchSource} and {@link MongoDBBatchSink}.
 */
public class MongoDBTest extends HydratorTestBase {
  private static final String VERSION = "3.2.0";
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion(VERSION);

  private static final ArtifactId BATCH_APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", VERSION);
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  private static final ArtifactRange BATCH_ARTIFACT_RANGE = new ArtifactRange(NamespaceId.DEFAULT.getNamespace(),
                                                                              "data-pipeline",
                                                                              CURRENT_VERSION, true,
                                                                              CURRENT_VERSION, true);

  private static final String MONGO_DB = "cdap";
  private static final String MONGO_SINK_COLLECTIONS = "copy";
  private static final String SOURCE_COLLECTIONS = "source";

  private static final ZonedDateTime DATE_TIME = ZonedDateTime.now(ZoneOffset.UTC);
  private static final Schema NESTED_SCHEMA = Schema.recordOf("", Schema.Field.of("inner_field",
                                                                                  Schema.of(Schema.Type.STRING)));
  private static final Schema SCHEMA = Schema.recordOf(
    "document",
    Schema.Field.of("_id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("int32", Schema.of(Schema.Type.INT)),
    Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
    Schema.Field.of("object", NESTED_SCHEMA),
    Schema.Field.of("object-to-map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("binary", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("date", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
    Schema.Field.of("null", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("decimal", Schema.decimalOf(20, 10))
  );

  private static final List<StructuredRecord> TEST_RECORDS = Arrays.asList(
    StructuredRecord.builder(SCHEMA)
      .set("_id", "5d079ee6d078c94008e4bb3a")
      .set("string", "AAPL")
      .set("int32", Integer.MIN_VALUE)
      .set("double", Double.MIN_VALUE)
      .set("array", Arrays.asList("a1", "a2"))
      .set("object", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .set("object-to-map", ImmutableMap.builder().put("key", "value").build())
      .set("binary", "binary data".getBytes())
      .set("boolean", false)
      .setTimestamp("date", DATE_TIME)
      .set("long", Long.MIN_VALUE)
      .setDecimal("decimal", new BigDecimal("987654321.1234567890"))
      .build(),

    StructuredRecord.builder(SCHEMA)
      .set("_id", "5d079ee6d078c94008e4bb37")
      .set("string", "AAPL")
      .set("int32", 10)
      .set("double", 23.23)
      .set("array", Arrays.asList("a1", "a2"))
      .set("object", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .set("object-to-map", ImmutableMap.builder().put("key", "value").build())
      .set("binary", "binary data".getBytes())
      .set("boolean", false)
      .setTimestamp("date", DATE_TIME)
      .set("long", Long.MAX_VALUE)
      .setDecimal("decimal", new BigDecimal("0.1234567890"))
      .build()
  );

  // Correspond to the TEST_RECORDS
  private static final List<BsonDocument> TEST_DOCUMENTS = Arrays.asList(
    new BsonDocument()
      .append("_id", new BsonObjectId(new ObjectId("5d079ee6d078c94008e4bb3a")))
      .append("string", new BsonString("AAPL"))
      .append("int32", new BsonInt32(Integer.MIN_VALUE))
      .append("double", new BsonDouble(Double.MIN_VALUE))
      .append("array", new BsonArray(Arrays.asList(new BsonString("a1"), new BsonString("a2"))))
      .append("object", new BsonDocument().append("inner_field", new BsonString("val")))
      .append("object-to-map", new BsonDocument().append("key", new BsonString("value")))
      .append("binary", new BsonBinary("binary data".getBytes()))
      .append("boolean", new BsonBoolean(false))
      .append("date", new BsonDateTime(DATE_TIME.toInstant().toEpochMilli()))
      .append("null", new BsonNull())
      .append("long", new BsonInt64(Long.MIN_VALUE))
      .append("decimal", new BsonDecimal128(Decimal128.parse("987654321.1234567890"))),

    new BsonDocument()
      .append("_id", new BsonObjectId(new ObjectId("5d079ee6d078c94008e4bb37")))
      .append("string", new BsonString("AAPL"))
      .append("int32", new BsonInt32(10))
      .append("double", new BsonDouble(23.23))
      .append("array", new BsonArray(Arrays.asList(new BsonString("a1"), new BsonString("a2"))))
      .append("object", new BsonDocument().append("inner_field", new BsonString("val")))
      .append("object-to-map", new BsonDocument().append("key", new BsonString("value")))
      .append("binary", new BsonBinary("binary data".getBytes()))
      .append("boolean", new BsonBoolean(false))
      .append("date", new BsonDateTime(DATE_TIME.toInstant().toEpochMilli()))
      .append("null", new BsonNull())
      .append("long", new BsonInt64(Long.MAX_VALUE))
      .append("decimal", new BsonDecimal128(Decimal128.parse("0.1234567890")))
  );

  private static final Schema ALL_SINK_DATATYPES_SCHEMA = Schema.recordOf(
    "document",
    ImmutableList.<Schema.Field>builder()
      .addAll(SCHEMA.getFields())
      .addAll(Arrays.asList(
        Schema.Field.of("date-field", Schema.of(Schema.LogicalType.DATE)),
        Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
        Schema.Field.of("time", Schema.of(Schema.LogicalType.TIME_MILLIS)),
        Schema.Field.of("enum", Schema.enumWith("first", "second")),
        Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.STRING), NESTED_SCHEMA))
      )).build()
  );

  private static final List<StructuredRecord> ALL_SINK_DATATYPES_RECORDS = Arrays.asList(
    StructuredRecord.builder(ALL_SINK_DATATYPES_SCHEMA)
      .set("_id", "5d079ee6d078c94008e4bb4a")
      .set("string", "AAPL")
      .set("int32", Integer.MIN_VALUE)
      .set("double", Double.MIN_VALUE)
      .set("array", Arrays.asList("a1", "a2"))
      .set("object", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .set("object-to-map", ImmutableMap.builder().put("key", "value").build())
      .set("binary", "binary data".getBytes())
      .set("boolean", false)
      .setTimestamp("date", DATE_TIME)
      .set("long", Long.MIN_VALUE)
      .setDecimal("decimal", new BigDecimal("987654321.1234567890"))
      .setDate("date-field", DATE_TIME.toLocalDate())
      .set("float", Float.MIN_VALUE)
      .setTime("time", DATE_TIME.toLocalTime())
      .set("enum", "first")
      .set("union", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .build(),

    StructuredRecord.builder(ALL_SINK_DATATYPES_SCHEMA)
      .set("_id", "5d079ee6d078c94008e4bb47")
      .set("string", "AAPL")
      .set("int32", 10)
      .set("double", 23.23)
      .set("array", Arrays.asList("a1", "a2"))
      .set("object", StructuredRecord.builder(NESTED_SCHEMA).set("inner_field", "val").build())
      .set("object-to-map", ImmutableMap.builder().put("key", "value").build())
      .set("binary", "binary data".getBytes())
      .set("boolean", false)
      .setTimestamp("date", DATE_TIME)
      .set("long", Long.MAX_VALUE)
      .setDecimal("decimal", new BigDecimal("0.1234567890"))
      .setDate("date-field", DATE_TIME.toLocalDate())
      .set("float", Float.MIN_VALUE)
      .setTime("time", DATE_TIME.toLocalTime())
      .set("enum", "second")
      .set("union", "string")
      .build()
  );

  private static final BiConsumer<StructuredRecord, BsonDocument> COMPARE_COMMON = (expected, actual) -> {
    Assert.assertEquals(expected.get("string"), actual.getString("string").getValue());
    Assert.assertEquals((int) expected.get("int32"), actual.getInt32("int32").getValue());
    Assert.assertEquals(expected.<Double>get("double"), actual.getDouble("double").getValue(), 0.00001);
    Assert.assertArrayEquals(expected.get("binary"), actual.getBinary("binary").getData());
    Assert.assertEquals((long) expected.get("long"), actual.getInt64("long").getValue());
    Assert.assertEquals(expected.get("boolean"), actual.getBoolean("boolean").getValue());

    Assert.assertEquals(expected.get("array"),
                        actual.getArray("array").getValues().stream()
                          .map(BsonValue::asString)
                          .map(BsonString::getValue)
                          .collect(Collectors.toList()));

    Assert.assertEquals(expected.<StructuredRecord>get("object").get("inner_field"),
                        actual.getDocument("object").getString("inner_field").getValue());

    Map<String, String> actualMap = actual.getDocument("object-to-map").entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asString().getValue()));

    Assert.assertEquals(expected.get("object-to-map"), actualMap);

    Assert.assertEquals(expected.getTimestamp("date").toInstant().toEpochMilli(),
                        actual.getDateTime("date").getValue());

    Assert.assertEquals(expected.getDecimal("decimal"),
                        actual.getDecimal128("decimal").getValue().bigDecimalValue());
  };

  private static final BiConsumer<StructuredRecord, BsonDocument> COMPARE_SINK_DATA_TYPES = (expected, actual) -> {
    Assert.assertEquals(expected.getDate("date-field").atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(),
                        actual.getDateTime("date-field").getValue());
    Assert.assertEquals(expected.<Float>get("float"), actual.getDouble("float").getValue(), 0.00001);
    Assert.assertEquals(expected.get("enum"), actual.getString("enum").getValue());
    Assert.assertEquals(expected.getTime("time").toString(), actual.getString("time").getValue());
    // If actual value is a record
    if (expected.get("union") instanceof StructuredRecord) {
      Assert.assertEquals(expected.<StructuredRecord>get("union").get("inner_field"),
                          actual.getDocument("union").getString("inner_field").getValue());
    }
    // If actual value is a string
    if (expected.get("union") instanceof String) {
      Assert.assertEquals(expected.get("union"), actual.getString("union").getValue());
    }
  };

  private static final MongodStarter starter = MongodStarter.getDefaultInstance();
  private MongodExecutable mongodExecutable;
  private MongodProcess mongod;
  private MongoClient mongoClient;
  private int mongoPort;

  @BeforeClass
  public static void setup() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    Set<ArtifactRange> parents = ImmutableSet.of(BATCH_ARTIFACT_RANGE);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("mongo-plugins", "1.0.0"), parents,
                      MongoDBBatchSource.class, MongoInputFormat.class, MongoSplitter.class, MongoInputSplit.class,
                      MongoDBBatchSink.class);
  }

  @Before
  public void beforeTest() throws Exception {
    // Start an embedded mongodb server
    mongoPort = Network.getFreeServerPort();
    IMongodConfig mongodConfig = new MongodConfigBuilder()
      .version(Version.Main.V4_0)
      .net(new Net("localhost", mongoPort, Network.localhostIsIPv6()))
      .build();
    mongodExecutable = starter.prepare(mongodConfig);
    mongod = mongodExecutable.start();

    mongoClient = new MongoClient("localhost", mongoPort);
    mongoClient.dropDatabase(MONGO_DB);
    MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB);
    MongoIterable<String> collections = mongoDatabase.listCollectionNames();
    Assert.assertFalse(collections.iterator().hasNext());
    MongoDatabase db = mongoClient.getDatabase(MONGO_DB);

    mongoDatabase.createCollection(SOURCE_COLLECTIONS);
    MongoCollection allDataTypesCollection = db.getCollection(SOURCE_COLLECTIONS, BsonDocument.class);
    allDataTypesCollection.insertMany(TEST_DOCUMENTS);
  }

  @After
  public void afterTest() throws Exception {
    if (mongod != null) {
      mongod.stop();
    }
    if (mongodExecutable != null) {
      mongodExecutable.stop();
    }
  }

  @Test
  public void testMongoDBSink() throws Exception {
    String inputDatasetName = "input-batchsinktest";
    String secondCollectionName = MONGO_SINK_COLLECTIONS + "second";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    ETLStage sink1 = new ETLStage("MongoDB1", new ETLPlugin(
      "MongoDB",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .putAll(getCommonPluginProperties())
        .put(MongoDBConstants.ID_FIELD, "_id")
        .put(MongoDBConstants.COLLECTION, MONGO_SINK_COLLECTIONS)
        .put(Constants.Reference.REFERENCE_NAME, "MongoTestDBSink1").build(),
      null));
    ETLStage sink2 = new ETLStage("MongoDB2", new ETLPlugin(
      "MongoDB",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .putAll(getCommonPluginProperties())
        .put(MongoDBConstants.ID_FIELD, "_id")
        .put(MongoDBConstants.COLLECTION, secondCollectionName)
        .put(Constants.Reference.REFERENCE_NAME, "MongoTestDBSink2").build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink1)
      .addStage(sink2)
      .addConnection(source.getName(), sink1.getName())
      .addConnection(source.getName(), sink2.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MongoSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);


    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, ALL_SINK_DATATYPES_RECORDS);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    verifyMongoSinkData(MONGO_SINK_COLLECTIONS, ALL_SINK_DATATYPES_RECORDS,
                        Arrays.asList(COMPARE_COMMON, COMPARE_SINK_DATA_TYPES));
    verifyMongoSinkData(secondCollectionName, ALL_SINK_DATATYPES_RECORDS,
                        Arrays.asList(COMPARE_COMMON, COMPARE_SINK_DATA_TYPES));
  }

  @Test
  public void testMongoToMongo() throws Exception {
    ETLStage source = new ETLStage("MongoDBSource", new ETLPlugin(
      "MongoDB",
      BatchSource.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .putAll(getCommonPluginProperties())
        .put(MongoDBConstants.COLLECTION, SOURCE_COLLECTIONS)
        .put(MongoDBConstants.SCHEMA, SCHEMA.toString())
        .put(Constants.Reference.REFERENCE_NAME, "MongoMongoTest").build(),
      null));

    ETLStage sink = new ETLStage("MongoDBSink", new ETLPlugin(
      "MongoDB",
      BatchSink.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .putAll(getCommonPluginProperties())
        .put(MongoDBConstants.ID_FIELD, "_id")
        .put(MongoDBConstants.COLLECTION, MONGO_SINK_COLLECTIONS)
        .put(Constants.Reference.REFERENCE_NAME, "MongoToMongoTest").build(),
      null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MongoToMongoTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    verifyMongoSinkData(MONGO_SINK_COLLECTIONS, TEST_RECORDS, Collections.singletonList(COMPARE_COMMON));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testMongoDBSourceSupportedDataTypes() throws Exception {
    ETLStage source = new ETLStage("MongoDB", new ETLPlugin(
      "MongoDB",
      BatchSource.PLUGIN_TYPE,
      new ImmutableMap.Builder<String, String>()
        .putAll(getCommonPluginProperties())
        .put(MongoDBConstants.COLLECTION, SOURCE_COLLECTIONS)
        .put(MongoDBConstants.SCHEMA, SCHEMA.toString())
        .put(Constants.Reference.REFERENCE_NAME, "AllDataTypesMongoTest").build(),
      null));
    String outputDatasetName = "all-data-types-output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MongoAllDataTypesSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, outputRecords.size());
    for (BsonDocument expected : TEST_DOCUMENTS) {
      Optional<StructuredRecord> actualOptional = outputRecords.stream()
        .filter(r -> expected.getObjectId("_id").getValue().toHexString().equals(r.get("_id")))
        .findAny();

      Assert.assertTrue(String.format("Output must contain record for document with ID: '%s'",
                                      expected.getObjectId("_id").getValue().toHexString()),
                        actualOptional.isPresent());
      StructuredRecord actual = actualOptional.get();

      Assert.assertNull(actual.get("null"));
      Assert.assertEquals(expected.getString("string").getValue(), actual.get("string"));
      Assert.assertEquals(expected.getInt32("int32").getValue(), (int) actual.get("int32"));
      Assert.assertEquals(expected.getDouble("double").getValue(), actual.get("double"), 0.00001);
      Assert.assertArrayEquals(expected.getBinary("binary").getData(), Bytes.getBytes(actual.get("binary")));
      Assert.assertEquals(expected.getInt64("long").getValue(), (long) actual.get("long"));
      Assert.assertEquals(expected.getBoolean("boolean").getValue(), actual.get("boolean"));

      Assert.assertEquals(expected.getArray("array").getValues().stream()
                            .map(BsonValue::asString)
                            .map(BsonString::getValue)
                            .collect(Collectors.toList()), actual.get("array"));

      Assert.assertEquals(expected.getDocument("object").getString("inner_field").getValue(),
                          actual.<StructuredRecord>get("object").get("inner_field"));

      Map<String, String> expectedMap = expected.getDocument("object-to-map").entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asString().getValue()));

      Assert.assertEquals(expectedMap, actual.get("object-to-map"));

      Assert.assertEquals(expected.getDateTime("date").getValue(),
                          actual.getTimestamp("date").toInstant().toEpochMilli());

      Assert.assertEquals(expected.getDecimal128("decimal").getValue().bigDecimalValue(),
                          actual.getDecimal("decimal"));
    }
  }

  private void verifyMongoSinkData(String collectionName, List<StructuredRecord> expectedRecords,
                                   List<BiConsumer<StructuredRecord, BsonDocument>> testActions) throws Exception {
    MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB);
    MongoCollection<BsonDocument> documents = mongoDatabase.getCollection(collectionName, BsonDocument.class);
    Assert.assertEquals(expectedRecords.size(), documents.count());

    for (StructuredRecord expected : expectedRecords) {
      BsonObjectId expectedId = new BsonObjectId(new ObjectId(expected.<String>get("_id")));
      Iterator<BsonDocument> actualIterator = documents.find(new BsonDocument("_id", expectedId)).iterator();
      Assert.assertTrue(actualIterator.hasNext());
      BsonDocument actual = actualIterator.next();
      // Perform supplied test actions
      testActions.forEach(test -> test.accept(expected, actual));
    }
  }

  private Map<String, String> getCommonPluginProperties() {
    return new ImmutableMap.Builder<String, String>()
      .put(MongoDBConstants.HOST, "localhost")
      .put(MongoDBConstants.PORT, String.valueOf(mongoPort))
      .put(MongoDBConstants.DATABASE, MONGO_DB)
      .put(MongoDBConstants.ON_ERROR, ErrorHandling.FAIL_PIPELINE.getDisplayName())
      .build();
  }
}
