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

import com.google.common.collect.ImmutableMap;
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.BSONObject;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link RecordToBSONWritableTransformer} test.
 */
public class RecordToBSONWritableTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final RecordToBSONWritableTransformer TRANSFORMER = new RecordToBSONWritableTransformer();

  @Test
  public void testTransform() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("array_field",
                                                    Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("int_field", 15)
      .set("long_field", 10L)
      .set("double_field", 10.5D)
      .set("float_field", 15.5F)
      .set("string_field", "string_value")
      .set("boolean_field", true)
      .set("bytes_field", "test_blob".getBytes())
      .set("null_field", null)
      .set("array_field", Arrays.asList(1L, null, 2L, null, 3L))
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertEquals(inputRecord.get("int_field"), bsonObject.get("int_field"));
    Assert.assertEquals(inputRecord.get("long_field"), bsonObject.get("long_field"));
    Assert.assertEquals(inputRecord.get("double_field"), bsonObject.get("double_field"));
    Assert.assertEquals(inputRecord.get("float_field"), bsonObject.get("float_field"));
    Assert.assertEquals(inputRecord.get("string_field"), bsonObject.get("string_field"));
    Assert.assertEquals(inputRecord.get("boolean_field"), bsonObject.get("boolean_field"));
    Assert.assertEquals(inputRecord.get("bytes_field"), bsonObject.get("bytes_field"));
    Assert.assertNull(bsonObject.get("null_field"));
    Assert.assertEquals(inputRecord.get("array_field"), bsonObject.get("array_field"));
  }

  @Test
  public void testTransformNestedMapsSimpleTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("nested_string_maps", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                  Schema.of(Schema.Type.STRING)))),
                                    Schema.Field.of("nested_int_maps", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                  Schema.of(Schema.Type.INT)))),
                                    Schema.Field.of("nested_bytes_maps", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                                  Schema.of(Schema.Type.BYTES))))
    );

    Map<String, Map<String, String>> stringMap = ImmutableMap.<String, Map<String, String>>builder()
      .put("nested_map1", ImmutableMap.<String, String>builder().put("k1", "v1").build())
      .put("nested_map2", ImmutableMap.<String, String>builder().put("k2", "v2").build())
      .put("nested_map3", ImmutableMap.<String, String>builder().put("k3", "v3").build())
      .build();

    Map<String, Map<String, Integer>> intMap = ImmutableMap.<String, Map<String, Integer>>builder()
      .put("nested_map1", ImmutableMap.<String, Integer>builder().put("k1", 1).build())
      .put("nested_map2", ImmutableMap.<String, Integer>builder().put("k2", 2).build())
      .put("nested_map3", ImmutableMap.<String, Integer>builder().put("k3", 3).build())
      .build();

    Map<String, Map<String, byte[]>> bytesMap = ImmutableMap.<String, Map<String, byte[]>>builder()
      .put("nested_map1", ImmutableMap.<String, byte[]>builder().put("k1", "v1".getBytes()).build())
      .put("nested_map2", ImmutableMap.<String, byte[]>builder().put("k2", "v2".getBytes()).build())
      .put("nested_map3", ImmutableMap.<String, byte[]>builder().put("k3", "v3".getBytes()).build())
      .build();

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("nested_string_maps", stringMap)
      .set("nested_int_maps", intMap)
      .set("nested_bytes_maps", bytesMap)
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertEquals(inputRecord.get("nested_string_maps"), bsonObject.get("nested_string_maps"));
    Assert.assertEquals(inputRecord.get("nested_int_maps"), bsonObject.get("nested_int_maps"));
    Assert.assertEquals(inputRecord.get("nested_bytes_maps"), bsonObject.get("nested_bytes_maps"));
  }

  @Test
  public void testTransformComplexNestedMaps() {

    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_decimal_field", Schema.decimalOf(4, 2)));

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("map_field", Schema.nullableOf(
                                      Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                   Schema.mapOf(Schema.of(Schema.Type.STRING), nestedRecordSchema)))));

    StructuredRecord nestedRecord1 = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some value")
      .setDecimal("nested_decimal_field", new BigDecimal("12.34"))
      .build();

    StructuredRecord nestedRecord2 = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some value")
      .setDecimal("nested_decimal_field", new BigDecimal("10.00"))
      .build();

    StructuredRecord nestedRecord3 = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some value")
      .setDecimal("nested_decimal_field", new BigDecimal("10.01"))
      .build();

    Map<String, Map<String, StructuredRecord>> map = ImmutableMap.<String, Map<String, StructuredRecord>>builder()
      .put("nested_map1", ImmutableMap.<String, StructuredRecord>builder().put("k1", nestedRecord1).build())
      .put("nested_map2", ImmutableMap.<String, StructuredRecord>builder().put("k2", nestedRecord2).build())
      .put("nested_map3", ImmutableMap.<String, StructuredRecord>builder().put("k3", nestedRecord3).build())
      .build();

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("map_field", map)
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    // Nested records must be transformed to a BSONObject as a regular ones
    BSONObject actualNestedMap1 = (BSONObject) ((BSONObject) bsonObject.get("map_field")).get("nested_map1");
    Assert.assertEquals(TRANSFORMER.transform(nestedRecord1).getDoc(), actualNestedMap1.get("k1"));

    BSONObject actualNestedMap2 = (BSONObject) ((BSONObject) bsonObject.get("map_field")).get("nested_map2");
    Assert.assertEquals(TRANSFORMER.transform(nestedRecord2).getDoc(), actualNestedMap2.get("k2"));

    BSONObject actualNestedMap3 = (BSONObject) ((BSONObject) bsonObject.get("map_field")).get("nested_map3");
    Assert.assertEquals(TRANSFORMER.transform(nestedRecord3).getDoc(), actualNestedMap3.get("k3"));
  }

  @Test
  public void testTransformNestedArraysSimpleTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("nested_string_array", Schema.arrayOf(
                                      Schema.arrayOf(Schema.of(Schema.Type.STRING)))),
                                    Schema.Field.of("nested_int_array", Schema.arrayOf(
                                      Schema.arrayOf(Schema.of(Schema.Type.INT)))),
                                    Schema.Field.of("nested_bytes_array", Schema.arrayOf(
                                      Schema.arrayOf(Schema.of(Schema.Type.BYTES))))
    );

    List<List<String>> stringArray = Arrays.asList(
      Arrays.asList("1", "2", "3"),
      Arrays.asList("1", "2", "3"),
      Arrays.asList("1", "2", "3")
    );

    List<List<Integer>> intArray = Arrays.asList(
      Arrays.asList(1, 2, 3),
      Arrays.asList(1, 2, 3),
      Arrays.asList(1, 2, 3)
    );

    List<List<byte[]>> bytesArray = Arrays.asList(
      Arrays.asList("1".getBytes(), "2".getBytes(), "3".getBytes()),
      Arrays.asList("1".getBytes(), "2".getBytes(), "3".getBytes()),
      Arrays.asList("1".getBytes(), "2".getBytes(), "3".getBytes())
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("nested_string_array", stringArray)
      .set("nested_int_array", intArray)
      .set("nested_bytes_array", bytesArray)
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertEquals(inputRecord.get("nested_string_array"), bsonObject.get("nested_string_array"));
    Assert.assertEquals(inputRecord.get("nested_int_array"), bsonObject.get("nested_int_array"));
    Assert.assertEquals(inputRecord.get("nested_bytes_array"), bsonObject.get("nested_bytes_array"));
  }

  @Test
  public void testTransformComplexNestedArrays() {

    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_decimal_field", Schema.decimalOf(4, 2)));
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("nested_array_of_maps", Schema.arrayOf(
                                      Schema.arrayOf(Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                  Schema.of(Schema.Type.STRING))))),
                                    Schema.Field.of("nested_array_of_records", Schema.arrayOf(
                                      Schema.arrayOf(nestedRecordSchema)))
    );

    List<List<Map<String, String>>> arrayOfMaps = Arrays.asList(
      Collections.singletonList(ImmutableMap.<String, String>builder().put("k1", "v1").build()),
      Collections.singletonList(ImmutableMap.<String, String>builder().put("k2", "v2").build()),
      Collections.singletonList(ImmutableMap.<String, String>builder().put("k3", "v3").build())
    );

    List<List<StructuredRecord>> arrayOfRecords = Arrays.asList(
      Collections.singletonList(
        StructuredRecord.builder(nestedRecordSchema)
          .set("nested_string_field", "some value")
          .setDecimal("nested_decimal_field", new BigDecimal("12.34"))
          .build()
      ),

      Collections.singletonList(
        StructuredRecord.builder(nestedRecordSchema)
          .set("nested_string_field", "some value")
          .setDecimal("nested_decimal_field", new BigDecimal("10.00"))
          .build()
      ),

      Collections.singletonList(
        StructuredRecord.builder(nestedRecordSchema)
          .set("nested_string_field", "some value")
          .setDecimal("nested_decimal_field", new BigDecimal("10.01"))
          .build()
      )
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("nested_array_of_maps", arrayOfMaps)
      .set("nested_array_of_records", arrayOfRecords)
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertEquals(inputRecord.get("nested_array_of_maps"), bsonObject.get("nested_array_of_maps"));

    // Nested records must be transformed to a BSONObject as a regular ones
    List actualArrayOfRecords = (List) bsonObject.get("nested_array_of_records");
    Assert.assertEquals(TRANSFORMER.transform(arrayOfRecords.get(0).get(0)).getDoc(),
                        ((List) actualArrayOfRecords.get(0)).get(0));

    Assert.assertEquals(TRANSFORMER.transform(arrayOfRecords.get(1).get(0)).getDoc(),
                        ((List) actualArrayOfRecords.get(1)).get(0));

    Assert.assertEquals(TRANSFORMER.transform(arrayOfRecords.get(2).get(0)).getDoc(),
                        ((List) actualArrayOfRecords.get(2)).get(0));
  }

  @Test
  public void testTransformStringIdField() {
    Schema nestedRecordSchema = Schema.recordOf("nested",
                                                Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("nested_record", nestedRecordSchema));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "5d431557d62a513457e791f4")
      .set("nested_record", StructuredRecord.builder(nestedRecordSchema)
        .set("string_field", "5d431557d62a513457e791f4")
        .build())
      .build();

    BSONWritable bsonWritable = new RecordToBSONWritableTransformer("string_field").transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertEquals(new ObjectId("5d431557d62a513457e791f4"), bsonObject.get("_id"));
    Assert.assertFalse(bsonObject.containsField("string_field"));

    BSONObject nestedActual = (BSONObject) bsonObject.get("nested_record");
    // Nested records must not be affected
    Assert.assertFalse(nestedActual.containsField("_id"));
  }

  @Test
  public void testTransformBytesIdField() {
    Schema nestedRecordSchema = Schema.recordOf("nested",
                                                Schema.Field.of("bytes_field", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("nested_record", nestedRecordSchema));

    ByteBuffer byteBuffer = ByteBuffer.allocate(12);
    new ObjectId("5d431557d62a513457e791f4").putToByteBuffer(byteBuffer);

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("bytes_field", byteBuffer.array())
      .set("nested_record", StructuredRecord.builder(nestedRecordSchema)
        .set("bytes_field", "5d431557d62a513457e791f4")
        .build())
      .build();

    BSONWritable bsonWritable = new RecordToBSONWritableTransformer("bytes_field").transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertEquals(new ObjectId("5d431557d62a513457e791f4"), bsonObject.get("_id"));
    Assert.assertFalse(bsonObject.containsField("bytes_field"));

    BSONObject nestedActual = (BSONObject) bsonObject.get("nested_record");
    // Nested records must not be affected
    Assert.assertFalse(nestedActual.containsField("_id"));
  }

  @Test
  public void testTransformUnionBytes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("union_field", Schema.unionOf(
                                      Schema.of(Schema.Type.STRING),
                                      Schema.of(Schema.Type.BYTES)
                                    )));


    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("union_field", ByteBuffer.wrap("bytes".getBytes()))
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    // ByteBuffer value must be transformed to byte array, since it can not be written directly via BSONWritable
    Assert.assertTrue(bsonObject.get("union_field") instanceof byte[]);
    Assert.assertArrayEquals("bytes".getBytes(), (byte[]) bsonObject.get("union_field"));
  }

  @Test
  public void testTransformUnionRecord() {
    Schema recordSchema = Schema.recordOf("nested", Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)));
    Schema schema = Schema.recordOf("schema", Schema.Field.of("union_field", Schema.unionOf(
      Schema.of(Schema.Type.STRING), recordSchema)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("union_field", StructuredRecord.builder(recordSchema)
        .set("bytes_field", ByteBuffer.wrap("bytes".getBytes()))
        .build())
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertTrue(bsonObject.containsField("union_field"));
    // Record must be transformed to BSONObject
    Assert.assertTrue(bsonObject.get("union_field") instanceof BSONObject);

    // ByteBuffer value must be transformed to byte array, since it can not be written directly via BSONWritable
    BSONObject recordActual = (BSONObject) bsonObject.get("union_field");
    Assert.assertTrue(recordActual.get("bytes_field") instanceof byte[]);
    Assert.assertArrayEquals("bytes".getBytes(), (byte[]) recordActual.get("bytes_field"));
  }

  @Test
  public void testTransformEmpty() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("decimal", Schema.nullableOf(Schema.decimalOf(6, 4))),
                                    Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("array_field",
                                                    Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.LONG)))),
                                    Schema.Field.of("timestamp_millis",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                                    Schema.Field.of("timestamp_micros",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("time_millis",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("time_micros",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS)))
    );

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();

    Assert.assertNotNull(bsonObject);
    Assert.assertNull(inputRecord.get("int_field"));
    Assert.assertNull(inputRecord.get("long_field"));
    Assert.assertNull(inputRecord.get("double_field"));
    Assert.assertNull(inputRecord.get("float_field"));
    Assert.assertNull(inputRecord.get("string_field"));
    Assert.assertNull(inputRecord.get("boolean_field"));
    Assert.assertNull(inputRecord.get("bytes_field"));
    Assert.assertNull(bsonObject.get("null_field"));
    Assert.assertNull(inputRecord.get("array_field"));
    Assert.assertNull(inputRecord.get("decimal"));
    Assert.assertNull(inputRecord.get("timestamp_millis"));
    Assert.assertNull(inputRecord.get("timestamp_micros"));
    Assert.assertNull(inputRecord.get("time_millis"));
    Assert.assertNull(inputRecord.get("time_micros"));
    Assert.assertNull(inputRecord.get("date"));
  }


  @Test
  public void testTransformInvalidStringIdField() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)));
    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "not a valid identifier")
      .build();

    thrown.expect(UnexpectedFormatException.class);
    new RecordToBSONWritableTransformer("string_field").transform(inputRecord);
  }

  @Test
  public void testTransformArrays() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("list_field", Schema.arrayOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("set_field", Schema.arrayOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("array_field", Schema.arrayOf(Schema.of(Schema.Type.LONG))));

    List<Long> expected = Arrays.asList(1L, 2L, null, 3L);
    Set<Long> expectedSet = new HashSet<>(expected);
    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("set_field", expectedSet)
      .set("list_field", expected)
      .set("array_field", expected.toArray())
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();
    Assert.assertNotNull(bsonObject);
    Assert.assertEquals(expectedSet, new HashSet<>((List<Long>) bsonObject.get("set_field")));
    Assert.assertEquals(expected, bsonObject.get("list_field"));
    Assert.assertEquals(expected, bsonObject.get("array_field"));
  }

  @Test
  public void testTransformLogicalTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("decimal", Schema.decimalOf(4, 2)),
                                    Schema.Field.of("timestamp_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("timestamp_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                    Schema.Field.of("time_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .setDecimal("decimal", new BigDecimal("10.01"))
      .setTimestamp("timestamp_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("timestamp_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTime("time_millis", LocalTime.now(ZoneOffset.UTC))
      .setTime("time_micros", LocalTime.now(ZoneOffset.UTC))
      .setDate("date", LocalDate.now(ZoneOffset.UTC))
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();
    Assert.assertNotNull(bsonObject);
    BsonDocument bsonDocument = BsonDocument.parse(bsonObject.toString());
    Assert.assertEquals(inputRecord.getDecimal("decimal"),
                        bsonDocument.getDecimal128("decimal").getValue().bigDecimalValue());
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_millis").toInstant().toEpochMilli(),
                        bsonDocument.getDateTime("timestamp_millis").getValue());
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_micros").toInstant().toEpochMilli(),
                        bsonDocument.getDateTime("timestamp_micros").getValue());
    Assert.assertEquals(inputRecord.getTime("time_millis").toString(),
                        bsonDocument.getString("time_millis").getValue());
    Assert.assertEquals(inputRecord.getTime("time_micros").toString(),
                        bsonDocument.getString("time_micros").getValue());
    Assert.assertEquals(inputRecord.getDate("date").atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli(),
                        bsonDocument.getDateTime("date").getValue());
  }

  @Test
  public void testTransformNestedLogicalTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("decimal_array", Schema.arrayOf(Schema.decimalOf(4, 2))),
                                    Schema.Field.of("timestamp_millis_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                                    Schema.Field.of("timestamp_micros_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("time_millis_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("time_micros_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
                                    Schema.Field.of("date_array",
                                                    Schema.arrayOf(Schema.of(Schema.LogicalType.DATE)))
    );

    // Set values of logical types on temporary record to get their physical representation
    Schema tmpSchema = Schema.recordOf("schema",
                                       Schema.Field.of("decimal", Schema.decimalOf(4, 2)),
                                       Schema.Field.of("timestamp_millis",
                                                       Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                       Schema.Field.of("timestamp_micros",
                                                       Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                       Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                       Schema.Field.of("time_micros", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                       Schema.Field.of("date", Schema.of(Schema.LogicalType.DATE)));

    StructuredRecord tmpRecord = StructuredRecord.builder(tmpSchema)
      .setDecimal("decimal", new BigDecimal("10.01"))
      .setTimestamp("timestamp_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("timestamp_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTime("time_millis", LocalTime.now(ZoneOffset.UTC))
      .setTime("time_micros", LocalTime.now(ZoneOffset.UTC))
      .setDate("date", LocalDate.now(ZoneOffset.UTC))
      .build();

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("decimal_array", new byte[][]{tmpRecord.get("decimal")})
      .set("timestamp_millis_array", Collections.singleton(tmpRecord.get("timestamp_millis")))
      .set("timestamp_micros_array", Collections.singleton(tmpRecord.get("timestamp_micros")))
      .set("time_millis_array", Collections.singleton(tmpRecord.get("time_millis")))
      .set("time_micros_array", Collections.singleton(tmpRecord.get("time_micros")))
      .set("date_array", Collections.singleton(tmpRecord.get("date")))
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();
    Assert.assertNotNull(bsonObject);
    BsonDocument bsonDocument = BsonDocument.parse(bsonObject.toString());
    Assert.assertEquals(tmpRecord.getDecimal("decimal"),
                        bsonDocument.getArray("decimal_array").get(0).asDecimal128().getValue().bigDecimalValue());
    Assert.assertEquals(tmpRecord.getTimestamp("timestamp_millis").toInstant().toEpochMilli(),
                        bsonDocument.getArray("timestamp_millis_array").get(0).asDateTime().getValue());
    Assert.assertEquals(tmpRecord.getTimestamp("timestamp_micros").toInstant().toEpochMilli(),
                        bsonDocument.getArray("timestamp_micros_array").get(0).asDateTime().getValue());
    Assert.assertEquals(tmpRecord.getTime("time_millis").toString(),
                        bsonDocument.getArray("time_millis_array").get(0).asString().getValue());
    Assert.assertEquals(tmpRecord.getTime("time_micros").toString(),
                        bsonDocument.getArray("time_micros_array").get(0).asString().getValue());
    Assert.assertEquals(tmpRecord.getDate("date").atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli(),
                        bsonDocument.getArray("date_array").get(0).asDateTime().getValue());
  }

  @Test
  public void testTransformUnionLogicalTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("decimal", Schema.unionOf(
                                      Schema.decimalOf(4, 2),
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                                    Schema.Field.of("timestamp_millis", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS),
                                      Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("timestamp_micros", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MICROS),
                                      Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("time_millis", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIME_MILLIS),
                                      Schema.of(Schema.LogicalType.TIME_MICROS))),
                                    Schema.Field.of("time_micros", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.TIME_MICROS),
                                      Schema.of(Schema.LogicalType.TIME_MILLIS))),
                                    Schema.Field.of("date", Schema.unionOf(
                                      Schema.of(Schema.LogicalType.DATE),
                                      Schema.of(Schema.LogicalType.TIME_MICROS))));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .setDecimal("decimal", new BigDecimal("10.01"))
      .setTimestamp("timestamp_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("timestamp_micros", ZonedDateTime.now(ZoneOffset.UTC))
      .setTime("time_millis", LocalTime.now(ZoneOffset.UTC))
      .setTime("time_micros", LocalTime.now(ZoneOffset.UTC))
      .setDate("date", LocalDate.now(ZoneOffset.UTC))
      .build();

    BSONWritable bsonWritable = TRANSFORMER.transform(inputRecord);
    BSONObject bsonObject = bsonWritable.getDoc();
    Assert.assertNotNull(bsonObject);
    BsonDocument bsonDocument = BsonDocument.parse(bsonObject.toString());
    Assert.assertEquals(inputRecord.getDecimal("decimal"),
                        bsonDocument.getDecimal128("decimal").getValue().bigDecimalValue());
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_millis").toInstant().toEpochMilli(),
                        bsonDocument.getDateTime("timestamp_millis").getValue());
    Assert.assertEquals(inputRecord.getTimestamp("timestamp_micros").toInstant().toEpochMilli(),
                        bsonDocument.getDateTime("timestamp_micros").getValue());
    Assert.assertEquals(inputRecord.getTime("time_millis").toString(),
                        bsonDocument.getString("time_millis").getValue());
    Assert.assertEquals(inputRecord.getTime("time_micros").toString(),
                        bsonDocument.getString("time_micros").getValue());
    Assert.assertEquals(inputRecord.getDate("date").atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli(),
                        bsonDocument.getDateTime("date").getValue());
  }
}
