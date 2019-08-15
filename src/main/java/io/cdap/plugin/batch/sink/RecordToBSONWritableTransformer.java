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

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.MongoDBConstants;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Transforms {@link StructuredRecord} to {@link BSONWritable}.
 */
public class RecordToBSONWritableTransformer {

  private String idFieldName;

  public RecordToBSONWritableTransformer() {
  }

  /**
   * @param idFieldName specifies which of the incoming fields should be used as an document identifier. Identifier will
   *                    be generated if no value is specified.
   */
  public RecordToBSONWritableTransformer(@Nullable String idFieldName) {
    this.idFieldName = idFieldName;
  }

  /**
   * Transforms given {@link StructuredRecord} to {@link BSONWritable}.
   *
   * @param record structured record to be transformed.
   * @return {@link BSONWritable} that corresponds to the given {@link StructuredRecord}.
   */
  public BSONWritable transform(StructuredRecord record) {
    return new BSONWritable(extractRecord(null, record, idFieldName));
  }

  private DBObject extractRecord(@Nullable String fieldName, @Nullable StructuredRecord record,
                                 @Nullable String idFieldName) {
    if (record == null) {
      // Return 'null' value as it is
      return null;
    }
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(), "Schema fields cannot be empty");
    BasicDBObjectBuilder bsonBuilder = BasicDBObjectBuilder.start();
    for (Schema.Field field : fields) {
      // Use full field name for nested records to construct meaningful errors messages.
      // Full field names will be in the following format: 'record_field_name.nested_record_field_name'
      String fullFieldName = fieldName != null ? String.format("%s.%s", fieldName, field.getName()) : field.getName();
      Schema nonNullableSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable()
        : field.getSchema();

      if (field.getName().equals(idFieldName)) {
        ObjectId objectId = extractObjectId(fullFieldName, record.get(field.getName()), nonNullableSchema);
        bsonBuilder.add(MongoDBConstants.DEFAULT_ID_FIELD_NAME, objectId);
        continue;
      }

      bsonBuilder.add(field.getName(), extractValue(fullFieldName, record.get(field.getName()), nonNullableSchema));
    }
    return bsonBuilder.get();
  }

  private ObjectId extractObjectId(String fieldName, Object value, Schema schema) {
    Schema.Type type = schema.getType();
    try {
      if (type == Schema.Type.BYTES) {
        return value instanceof ByteBuffer ? new ObjectId((ByteBuffer) value) : new ObjectId((byte[]) value);
      } else if (type == Schema.Type.STRING) {
        return new ObjectId((String) value);
      }
      throw new UnexpectedFormatException(String.format("Identified field '%s' is expected to be of type " +
                                                          "'bytes' or 'string', but found a '%s'.", fieldName, type));
    } catch (IllegalArgumentException e) {
      throw new UnexpectedFormatException(String.format("Invalid value '%s' of the identifier field '%s'.", value,
                                                        fieldName), e);
    }
  }

  private Object extractValue(String fieldName, Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.getLogicalType();
    // Get values of logical types properly
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
          return new Date((long) value);
        case TIMESTAMP_MICROS:
          return new Date(TimeUnit.MICROSECONDS.toMillis((long) value));
        case TIME_MILLIS:
          LocalTime time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Integer) value)));
          return time.toString();
        case TIME_MICROS:
          LocalTime localTime = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
          return localTime.toString();
        case DATE:
          long epochDay = ((Integer) value).longValue();
          LocalDate localDate = LocalDate.ofEpochDay(epochDay);
          return Date.from(localDate.atStartOfDay(ZoneId.ofOffset("UTC", ZoneOffset.UTC)).toInstant());
        case DECIMAL:
          int scale = schema.getScale();
          BigDecimal decimal = value instanceof ByteBuffer
            ? new BigDecimal(new BigInteger(Bytes.toBytes((ByteBuffer) value)), scale)
            : new BigDecimal(new BigInteger((byte[]) value), scale);

          return new Decimal128(decimal);
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.name().toLowerCase()));
      }
    }

    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case BOOLEAN:
        return (Boolean) value;
      case INT:
        return (Integer) value;
      case DOUBLE:
        return (Double) value;
      case FLOAT:
        return (Float) value;
      case BYTES:
        if (value instanceof ByteBuffer) {
          return Bytes.getBytes((ByteBuffer) value);
        } else {
          return (byte[]) value;
        }
      case LONG:
        return (Long) value;
      case STRING:
        return (String) value;
      case MAP:
        return extractMap(fieldName, (Map<String, ?>) value, schema);
      case ARRAY:
        return extractArray(fieldName, value, schema);
      case RECORD:
        return extractRecord(fieldName, (StructuredRecord) value, null);
      case UNION:
        return extractUnion(fieldName, value, schema);
      case ENUM:
        String enumValue = (String) value;
        if (!Objects.requireNonNull(schema.getEnumValues()).contains(enumValue)) {
          throw new UnexpectedFormatException(
            String.format("Value '%s' of the field '%s' is not enum value. Enum values are: '%s'", enumValue, fieldName,
                          schema.getEnumValues()));
        }

        return enumValue;
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                          fieldLogicalType.name().toLowerCase()));
    }
  }

  private DBObject extractMap(String fieldName, Map<String, ?> map, Schema schema) {
    if (map == null) {
      // Return 'null' value as it is
      return null;
    }
    Map.Entry<Schema, Schema> mapSchema = Objects.requireNonNull(schema.getMapSchema());

    BasicDBObjectBuilder bsonBuilder = BasicDBObjectBuilder.start();
    Schema valueSchema = mapSchema.getValue();
    Schema nonNullableSchema = mapSchema.getValue().isNullable() ? valueSchema.getNonNullable() : valueSchema;
    map.forEach((k, v) -> bsonBuilder.add(k, extractValue(fieldName, v, nonNullableSchema)));

    return bsonBuilder.get();
  }

  private List extractArray(String fieldName, Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }

    Schema componentSchema = schema.getComponentSchema();
    Schema nonNullableSchema = componentSchema.isNullable() ? componentSchema.getNonNullable() : componentSchema;
    List<Object> extracted = new ArrayList<>();
    if (value.getClass().isArray()) {
      int length = Array.getLength(value);
      for (int i = 0; i < length; i++) {
        Object arrayElement = Array.get(value, i);
        extracted.add(extractValue(fieldName, arrayElement, nonNullableSchema));
      }
      return extracted;
    }
    // An 'array' field can be a java.util.Collection or an array.
    Collection values = (Collection) value;
    for (Object obj : values) {
      extracted.add(extractValue(fieldName, obj, nonNullableSchema));
    }
    return extracted;
  }

  private Object extractUnion(String fieldName, Object value, Schema schema) {
    if (value == null) {
      // Return 'null' value as it is
      return null;
    }
    for (Schema s : schema.getUnionSchemas()) {
      try {
        return extractValue(fieldName, value, s);
      } catch (Exception e) {
        // expected if this schema is not the correct one for the value
      }
    }
    // Should never happen
    throw new InvalidStageException(
      String.format("None of the union schemas '%s' of the field '%s' matches the value '%s'.",
                    schema.getUnionSchemas(), fieldName, value));
  }
}
