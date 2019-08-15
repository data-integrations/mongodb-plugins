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

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObjectBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

/**
 * {@link BSONObjectToRecordTransformer} test.
 */
public class BSONObjectToRecordTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransform() {
    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_long_field", Schema.of(Schema.Type.LONG)));

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("array_field",
                                                    Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))),
                                    Schema.Field.of("nested_object", nestedRecordSchema),
                                    Schema.Field.of("object_to_map", Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));

    StructuredRecord nestedRecord = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some")
      .set("nested_long_field", Long.MAX_VALUE)
      .build();

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("int_field", 15)
      .set("long_field", 10L)
      .set("double_field", 10.5D)
      .set("string_field", "string_value")
      .set("boolean_field", true)
      .set("bytes_field", "test_blob".getBytes())
      .set("null_field", null)
      .set("array_field", Arrays.asList(1L, null, 2L, null, 3L))
      .set("nested_object", nestedRecord)
      .set("object_to_map", ImmutableMap.<String, String>builder()
        .put("key", "value")
        .build()
      )
      .build();

    BasicBSONList bsonList = new BasicBSONList();
    bsonList.addAll(expected.get("array_field"));

    BSONObject nestedBsonObject = BasicDBObjectBuilder.start()
      .add("nested_string_field", nestedRecord.get("nested_string_field"))
      .add("nested_long_field", nestedRecord.get("nested_long_field"))
      .get();

    BSONObject mapObject = BasicDBObjectBuilder.start()
      .add("key", "value")
      .get();

    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("int_field", expected.get("int_field"))
      .add("long_field", expected.get("long_field"))
      .add("double_field", expected.get("double_field"))
      .add("float_field", expected.get("float_field"))
      .add("string_field", expected.get("string_field"))
      .add("boolean_field", expected.get("boolean_field"))
      .add("bytes_field", expected.get("bytes_field"))
      .add("null_field", expected.get("null_field"))
      .add("array_field", bsonList)
      .add("nested_object", nestedBsonObject)
      .add("object_to_map", mapObject)
      .get();

    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(bsonObject);

    Assert.assertEquals(expected, transformed);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformEmptyObject() {
    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_long_field", Schema.of(Schema.Type.LONG)));

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("array_field", Schema.nullableOf(
                                      Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
                                    Schema.Field.of("nested_object", Schema.nullableOf(nestedRecordSchema)),
                                    Schema.Field.of("object_to_map", Schema.nullableOf(
                                      Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))));

    BSONObject bsonObject = BasicDBObjectBuilder.start().get();
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(bsonObject);
    Assert.assertNull(transformed.get("int_field"));
    Assert.assertNull(transformed.get("long_field"));
    Assert.assertNull(transformed.get("double_field"));
    Assert.assertNull(transformed.get("string_field"));
    Assert.assertNull(transformed.get("boolean_field"));
    Assert.assertNull(transformed.get("bytes_field"));
    Assert.assertNull(bsonObject.get("null_field"));
    Assert.assertNull(transformed.get("array_field"));
    Assert.assertNull(transformed.get("nested_object"));
    Assert.assertNull(transformed.get("object_to_map"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformComplexMap() {
    Schema nestedRecordSchema = Schema.recordOf("nested_object",
                                                Schema.Field.of("nested_string_field", Schema.of(Schema.Type.STRING)),
                                                Schema.Field.of("nested_long_field", Schema.of(Schema.Type.LONG)));

    Schema schema = Schema.recordOf("schema", Schema.Field.of("object_to_map", Schema.mapOf(
      Schema.of(Schema.Type.STRING), nestedRecordSchema)));

    StructuredRecord nestedRecord = StructuredRecord.builder(nestedRecordSchema)
      .set("nested_string_field", "some")
      .set("nested_long_field", Long.MAX_VALUE)
      .build();

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("object_to_map", ImmutableMap.<String, StructuredRecord>builder()
        .put("key", nestedRecord)
        .build()
      )
      .build();

    BSONObject nestedBsonObject = BasicDBObjectBuilder.start()
      .add("nested_string_field", nestedRecord.get("nested_string_field"))
      .add("nested_long_field", nestedRecord.get("nested_long_field"))
      .get();
    BSONObject mapObject = BasicDBObjectBuilder.start()
      .add("key", nestedBsonObject)
      .get();
    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("object_to_map", mapObject)
      .get();

    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(bsonObject);

    Assert.assertEquals(expected, transformed);
  }

  @Test
  public void testTransformUnexpectedFormat() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("union_field", Schema.unionOf(
      Schema.of(Schema.Type.LONG),
      Schema.of(Schema.Type.STRING)))
    );

    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("union_field", 2019L)
      .get();

    thrown.expect(UnexpectedFormatException.class);
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(bsonObject);
  }

  @Test
  public void testTransformInvalidMapValue() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.mapOf(
      Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))));
    BSONObject invalidObject = BasicDBObjectBuilder.start()
      .add("field", "value")
      .get();

    thrown.expect(UnexpectedFormatException.class);
    thrown.expectMessage("is expected to be of type");
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(invalidObject);
  }

  @Test
  public void testTransformInvalidStringValue() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.of(Schema.Type.STRING)));
    BSONObject invalidObject = BasicDBObjectBuilder.start()
      .add("field", 1)
      .get();

    thrown.expect(UnexpectedFormatException.class);
    thrown.expectMessage("is expected to be of type");
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(invalidObject);
  }

  @Test
  public void testTransformInvalidBytesValue() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.of(Schema.Type.BYTES)));
    // BSONObject contain bytes array for values of MongoDB Bytes data type
    BSONObject invalidObject = BasicDBObjectBuilder.start()
      .add("field", ByteBuffer.allocate(12))
      .get();

    thrown.expect(UnexpectedFormatException.class);
    thrown.expectMessage("is expected to be of type");
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(invalidObject);
  }

  @Test
  public void testTransformInvalidLongValue() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.of(Schema.Type.LONG)));
    // Integer is not valid Long value
    BSONObject invalidObject = BasicDBObjectBuilder.start()
      .add("field", 0)
      .get();

    thrown.expect(UnexpectedFormatException.class);
    thrown.expectMessage("is expected to be of type");
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(invalidObject);
  }

  @Test
  public void testTransformInvalidArrayValue() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.of(Schema.Type.LONG)));
    // Integer is not valid Long value
    BSONObject invalidObject = BasicDBObjectBuilder.start()
      .add("field", new HashMap<>())
      .get();

    thrown.expect(UnexpectedFormatException.class);
    thrown.expectMessage("is expected to be of type");
    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(invalidObject);
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformLogicalTypes() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("decimal_6_4", Schema.decimalOf(6, 4)),
                                    Schema.Field.of("decimal_34_4", Schema.decimalOf(34, 4)),
                                    Schema.Field.of("decimal_34_6", Schema.decimalOf(34, 6)),
                                    Schema.Field.of("timestamp_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("timestamp_micros", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
    );

    ZonedDateTime dateTime = ZonedDateTime.now(ZoneOffset.UTC);
    StructuredRecord expected = StructuredRecord.builder(schema)
      .setDecimal("decimal_6_4", new BigDecimal("10.1234"))
      .setDecimal("decimal_34_4", new BigDecimal("10.1234"))
      .setDecimal("decimal_34_6", new BigDecimal("10.123400"))
      .setTimestamp("timestamp_millis", dateTime)
      .setTimestamp("timestamp_micros", dateTime)
      .build();

    Date date = Date.from(dateTime.toInstant());
    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("decimal_6_4", new Decimal128(new BigDecimal("10.1234")))
      .add("decimal_34_4", new Decimal128(new BigDecimal("10.1234")))
      .add("decimal_34_6", new Decimal128(new BigDecimal("10.1234")))
      .add("timestamp_millis", date)
      .add("timestamp_micros", date)
      .get();

    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(bsonObject);

    Assert.assertNotNull(transformed);
    Assert.assertEquals(expected.getDecimal("decimal_6_4"), transformed.getDecimal("decimal_6_4"));
    Assert.assertEquals(expected.getDecimal("decimal_34_4"), transformed.getDecimal("decimal_34_4"));
    Assert.assertEquals(expected.getDecimal("decimal_34_6"), transformed.getDecimal("decimal_34_6"));
    Assert.assertEquals(expected.getTimestamp("timestamp_millis"), transformed.getTimestamp("timestamp_millis"));
    Assert.assertEquals(expected.getTimestamp("timestamp_micros"), transformed.getTimestamp("timestamp_micros"));
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformDecimalDoesNotRound() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("decimal_34_2", Schema.decimalOf(34, 2)));
    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("decimal_34_2", new Decimal128(new BigDecimal("10.1234")))
      .get();

    thrown.expectMessage("has scale '4' which is not equal to schema scale '2'");
    thrown.expect(UnexpectedFormatException.class);

    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    transformer.transform(bsonObject);
  }

  @Test
  public void testTransformBytesId() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("_id", Schema.of(Schema.Type.BYTES)));

    ObjectId expectedId = new ObjectId("5d55315ce63eaf3886d57760");
    BSONObject bsonObject = BasicDBObjectBuilder.start()
      .add("_id", expectedId)
      .get();

    BSONObjectToRecordTransformer transformer = new BSONObjectToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(bsonObject);
    Assert.assertNotNull(transformed);

    Object transformedIdValue = transformed.get("_id");
    Assert.assertNotNull(transformedIdValue);
    ObjectId transformedId = transformedIdValue instanceof ByteBuffer ? new ObjectId((ByteBuffer) transformedIdValue)
      : new ObjectId((byte[]) transformedIdValue);

    Assert.assertEquals(expectedId, transformedId);
  }
}
