/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jcloud.util;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.avro.Schema.Type.*;


/**
 * avro util.
 */
@Slf4j
public class AvroRecordUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordUtil.class);

    private static final Map<byte[], Schema> SCHEMA_CACHES = new ConcurrentHashMap<>();

    public static Schema getAvroSchema(
            Record<GenericRecord> record) {
        final Message<GenericRecord> message = record.getMessage()
                .orElseThrow(() -> new RuntimeException("Message not exist in record"));

        return SCHEMA_CACHES.computeIfAbsent(message.getSchemaVersion(), (schemaVersion) -> {
            org.apache.pulsar.client.api.Schema<GenericRecord> schema = extractPulsarSchema(message);
            return convertToAvroSchema(schema);
        });
    }

    public static org.apache.pulsar.client.api.Schema<GenericRecord> getPulsarSchema(
            Record<GenericRecord> record) {

        org.apache.pulsar.client.api.Schema<GenericRecord> schema = record.getSchema();
        if (record.getValue().getSchemaVersion() == null && record.getValue().getSchemaType() != SchemaType.BYTES) {
            org.apache.pulsar.client.api.Schema<GenericRecord> internalSchema =
                    getPulsarInternalSchema(record.getMessage().orElse(null));
            if (internalSchema != null) {
                schema = recoverGenericSchemaFromInternalSchema(internalSchema);
                log.debug("Recover generic schema from internal schema: {}", schema);
            }
        }
        if (schema != null) {
            return schema;
        }
        // Pulsar version < 2.7.0
        final Message<GenericRecord> message = record.getMessage()
                .orElseThrow(() -> new RuntimeException("Message not exist in record, Please check if Source is "
                        + "PulsarSource."));
        return extractPulsarSchema(message);
    }

    public static org.apache.pulsar.client.api.Schema<GenericRecord>
    recoverGenericSchemaFromInternalSchema(org.apache.pulsar.client.api.Schema<GenericRecord> schema) {
        switch (schema.getSchemaInfo().getType()) {
            case PROTOBUF_NATIVE:
                return (GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(schema.getSchemaInfo());
            case AVRO:
                return (GenericAvroSchema) GenericAvroSchema.of(schema.getSchemaInfo());
            case JSON:
                return (GenericJsonSchema) GenericJsonSchema.of(schema.getSchemaInfo());
            default:
                return null;
        }
    }

    public static org.apache.pulsar.client.api.Schema<GenericRecord> getPulsarInternalSchema(
            Message<GenericRecord> message) {
        org.apache.pulsar.client.api.Schema<GenericRecord> schema = null;
        if (message != null) {
            if (message instanceof MessageImpl) {
                MessageImpl impl = (MessageImpl) message;
                schema = impl.getSchemaInternal();
            } else if (message instanceof TopicMessageImpl) {
                TopicMessageImpl impl = (TopicMessageImpl) message;
                schema = impl.getSchemaInternal();
            }
        }
        return schema;
    }

    public static org.apache.pulsar.client.api.Schema<GenericRecord> extractPulsarSchema(
            Message<GenericRecord> message) {
        try {
            if (message.getReaderSchema().isPresent()) {
                return (org.apache.pulsar.client.api.Schema<GenericRecord>) message.getReaderSchema().get();
            }
            //There is no good way to handle `PulsarRecord#getSchema` in the pulsar function,
            // first read the schema information in the Message through reflection.
            // You can replace this method when the schema is available in the Record.
            final ClassLoader pulsarFunctionClassLoader = message.getClass().getClassLoader();
            Message<GenericRecord> rawMessage = message;
            if (message.getClass().getCanonicalName().endsWith("TopicMessageImpl")){
                final Class<?> classTopicMessageImpl =
                        pulsarFunctionClassLoader.loadClass("org.apache.pulsar.client.impl.TopicMessageImpl");
                final Method getMessage = classTopicMessageImpl.getDeclaredMethod("getMessage");
                @SuppressWarnings("unchecked") final Message<GenericRecord> invoke =
                        (Message<GenericRecord>) getMessage.invoke(message);
                rawMessage = invoke;
            }
            final Class<?> classMessageImpl =
                    pulsarFunctionClassLoader.loadClass("org.apache.pulsar.client.impl.MessageImpl");
            final Method getSchema = classMessageImpl.getDeclaredMethod("getSchema");
            @SuppressWarnings("unchecked")
            org.apache.pulsar.client.api.Schema<GenericRecord> schema =
                    (org.apache.pulsar.client.api.Schema<GenericRecord>) getSchema.invoke(rawMessage);
            return schema;
        } catch (Throwable e) {
            LOGGER.error("getPulsarSchema error", e);
            throw new RuntimeException("getPulsarSchema error", e);
        }
    }

    @SneakyThrows
    public static Schema convertToAvroSchema(org.apache.pulsar.client.api.Schema<?> pulsarSchema) {
        SchemaInfo schemaInfo = pulsarSchema.getSchemaInfo();
        if (schemaInfo.getType() == SchemaType.PROTOBUF_NATIVE) {
            Descriptors.Descriptor descriptor = ProtobufNativeSchemaUtils.deserialize(schemaInfo.getSchema());
            ProtobufData model = ProtobufData.get();
            return model.getSchema(descriptor);
        } else if (schemaInfo.getType() == SchemaType.KEY_VALUE) {
            final KeyValueSchema<?, ?> kvSchema = (KeyValueSchema<?, ?>) pulsarSchema;
            final String keySchemaDef = kvSchema.getKeySchema().getSchemaInfo().getSchemaDefinition();
            final String valueSchemaDef = kvSchema.getValueSchema().getSchemaInfo().getSchemaDefinition();

            final Schema avroSchema = Schema.createRecord("KVSchema", null, null, false,
                    Arrays.asList(
                            // namespace+name must be different in the KVSchema
                            new Schema.Field("key", parseAvroSchema(keySchemaDef, "_key")),
                            new Schema.Field("value", Schema.createUnion(
                                    Schema.create(Schema.Type.NULL), parseAvroSchema(valueSchemaDef, "_value")))
                    ));
            return avroSchema;
        } else {
            if (SchemaType.isPrimitiveType(schemaInfo.getType())){
                throw new UnsupportedOperationException(
                        "do not support non-structured schema type" + schemaInfo.getType());
            }
            String rootAvroSchemaString = schemaInfo.getSchemaDefinition();
            if (StringUtils.isEmpty(rootAvroSchemaString)) {
                throw new IllegalArgumentException("schema definition is empty");
            }
            return parseAvroSchema(rootAvroSchemaString, null);
        }
    }

    private static Schema parseAvroSchema(String jsonSchema, String namespaceSuffix) {
        final Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        Schema schema = parser.parse(jsonSchema);
        List<Schema.Field> fields = schema.getFields()
                .stream()
                .map(f -> new Schema.Field(f, f.schema()))
                .collect(Collectors.toList());
        if (namespaceSuffix == null) {
            namespaceSuffix = "";
        }
        final String namespace = schema.getNamespace() == null ? namespaceSuffix : schema.getNamespace() + namespaceSuffix;
        return Schema.createRecord(schema.getName(), null, namespace, false, fields);
    }

    public static org.apache.avro.generic.GenericRecord convertGenericRecord(DynamicMessage recordValue,
                                                                             Schema rootAvroSchema) {
        org.apache.avro.generic.GenericRecord recordHolder = new GenericData.Record(rootAvroSchema);
        for (Schema.Field field : rootAvroSchema.getFields()) {
            String fieldName = field.name();
            Object valueField = recordValue.getField(recordValue.getDescriptorForType().findFieldByName(fieldName));
            if (valueField instanceof DynamicMessage) {
                Schema subSchema = field.schema();
                if (field.schema().isUnion()) {
                    subSchema = field.schema().getTypes().stream()
                            .filter(schema -> schema.getType().equals(Schema.Type.RECORD))
                            .findFirst()
                            .get();
                }
                valueField = convertGenericRecord((DynamicMessage) valueField, subSchema);
            }
            recordHolder.put(fieldName, valueField);
        }
        log.debug("convert DynamicMessage to GenericRecord: {}", recordHolder);
        return recordHolder;
    }

    public static org.apache.avro.generic.GenericRecord convertGenericRecord(GenericRecord recordValue,
                                                                             Schema rootAvroSchema) {
        if (recordValue.getSchemaType() == SchemaType.KEY_VALUE) {
            KeyValue<GenericRecord, GenericRecord> keyValue =
                    (KeyValue<GenericRecord, GenericRecord>) recordValue.getNativeObject();
            org.apache.avro.generic.GenericRecord recordHolder = new GenericData.Record(rootAvroSchema);
            GenericRecord keyObject = keyValue.getKey();
            if (keyObject != null) {
                Schema keySchema = rootAvroSchema.getField("key").schema();
                recordHolder.put("key", convertGenericRecord(keyObject, keySchema));
            }
            GenericRecord valueObject = keyValue.getValue();
            if (valueObject != null) {
                Schema valueSchema = rootAvroSchema.getField("value").schema();
                recordHolder.put("value", convertGenericRecord(valueObject, valueSchema));
            }
            return recordHolder;
        }

        // handle nullable fields that are union[null,record]
        if (rootAvroSchema.isUnion()) {
            rootAvroSchema = rootAvroSchema.getTypes().stream()
                    .filter(schema -> schema.getType().equals(Schema.Type.RECORD))
                    .findFirst()
                    .get();
        }
        org.apache.avro.generic.GenericRecord recordHolder = new GenericData.Record(rootAvroSchema);
        for (org.apache.pulsar.client.api.schema.Field field : recordValue.getFields()) {
            Schema.Field field1 = rootAvroSchema.getField(field.getName());
            Object valueField = readValue(recordValue, field);
            if (valueField instanceof GenericRecord) {
                Schema subSchema = field1.schema();
                if (field1.schema().isUnion()) {
                    subSchema = field1.schema().getTypes().stream()
                            .filter(schema -> schema.getType().equals(Schema.Type.RECORD))
                            .findFirst()
                            .get();
                }
                valueField = convertGenericRecord((GenericRecord) valueField, subSchema);
            } else if (valueField instanceof DynamicMessage) {
                Schema subSchema = field1.schema();
                if (field1.schema().isUnion()) {
                    subSchema = field1.schema().getTypes().stream()
                            .filter(schema -> schema.getType().equals(Schema.Type.RECORD))
                            .findFirst()
                            .get();
                }
                valueField = convertGenericRecord((DynamicMessage) valueField, subSchema);
            }
            if (field1.schema().getType().equals(ENUM)) {
                valueField = new GenericData.EnumSymbol(field1.schema(), valueField.toString());
            } else if (field1.schema().getType().equals(ARRAY) && valueField instanceof List) {
                List<Object> list = ((List<?>) valueField).stream().map(v -> {
                    if (v instanceof GenericRecord) {
                        return convertGenericRecord((GenericRecord) v, field1.schema().getElementType());
                    } else if (v instanceof DynamicMessage) {
                        return convertGenericRecord((DynamicMessage) v, field1.schema().getElementType());
                    } else {
                        return v;
                    }
                }).collect(Collectors.toList());
                valueField = new GenericData.Array<>(field1.schema(), list);
            }
            recordHolder.put(field.getName(), valueField);
        }
        return recordHolder;
    }

    private static Object readValue(GenericRecord recordValue, org.apache.pulsar.client.api.schema.Field field) {
        if (recordValue == null || field == null) {
            return null;
        }
        //  If the field has no value, NullPointerException will be thrown, for GenericJsonRecord
        try {
            return recordValue.getField(field);
        } catch (NullPointerException ignore) {
            return null;
        }
    }

}
