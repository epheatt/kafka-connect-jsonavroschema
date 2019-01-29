package com.github.epheatt.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.StringConverterConfig;

import java.util.Map;
import java.util.HashMap;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonToAvroSchemaConverter extends JsonConverter {
    private static final Logger log = LoggerFactory.getLogger(JsonToAvroSchemaConverter.class);

    private SchemaRegistryClient schemaRegistry;
    private boolean isKey;
    private AvroData avroData;
    private String payloadPointer;

    private boolean enableSchemas = JsonConverterConfig.SCHEMAS_ENABLE_DEFAULT;
    private int cacheSize = JsonConverterConfig.SCHEMAS_CACHE_SIZE_DEFAULT;
    private Cache<Schema, ObjectNode> fromConnectSchemaCache;
    private Cache<JsonNode, Schema> toConnectSchemaCache;
    private final JsonSerializer serializer = new JsonSerializer();
    private final JsonDeserializer deserializer = new JsonDeserializer();

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
    private static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";

    public JsonToAvroSchemaConverter (SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    public JsonToAvroSchemaConverter () {

    }

    public void configure(Map<String, ?> configs) {
        AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);
        JsonConverterConfig jsonConverterConfig = new JsonConverterConfig(configs);

        enableSchemas = jsonConverterConfig.schemasEnabled();
        payloadPointer = (String) configs.getOrDefault("payload.pointer",null);
        cacheSize = jsonConverterConfig.schemaCacheSize();
        isKey = jsonConverterConfig.type() == ConverterType.KEY;
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
        avroData = new AvroData(new AvroDataConfig(configs));
        fromConnectSchemaCache = new SynchronizedCache<>(new LRUCache<Schema, ObjectNode>(cacheSize));
        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<JsonNode, Schema>(cacheSize));

        if (schemaRegistry == null) {
            schemaRegistry =
                    new CachedSchemaRegistryClient(avroConverterConfig.getSchemaRegistryUrls(),
                            avroConverterConfig.getMaxSchemasPerSubject(), configs);
        }
        super.configure(configs);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(StringConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {

        return super.fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        JsonNode jsonValue;

        // This handles a tombstone message
        if (value == null) {
            return SchemaAndValue.NULL;
        }

        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        if (enableSchemas) {
            ObjectNode envelope = mapper.createObjectNode();
            org.apache.avro.Schema schema = fetchAvroSchemaFromSchemaRegistry(topic);
            envelope.set(ENVELOPE_SCHEMA_FIELD_NAME, asJsonSchema(avroData.toConnectSchema(schema)));
            if (payloadPointer != null && !payloadPointer.isEmpty()) {
                jsonValue = jsonValue.at(payloadPointer);
            }
            envelope.set(ENVELOPE_PAYLOAD_FIELD_NAME, enrichJsonValueAsPerAvroSchema(schema, jsonValue));
            jsonValue = envelope;
        }

        try {
            return super.toConnectData(topic, serializer.serialize(topic, jsonValue));
        } catch (SerializationException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Fetch avro schema from schema registry.
     *
     * @param subject the subject
     * @return the schema
     */
    private org.apache.avro.Schema fetchAvroSchemaFromSchemaRegistry(String subject) {
        try {
            int schemaId = schemaRegistry.getLatestSchemaMetadata(subject).getId();
            return schemaRegistry.getBySubjectAndId(subject, schemaId);
        } catch (Exception e) {
            String errorMessage = "Error in fetching schema from registry for subject : " + subject;
            log.error(errorMessage, e);
            throw new RetriableException(errorMessage, e);
        }
    }

    /**
     * Enrich json value as per avro schema.
     *
     * @param valueSchema the value schema
     * @param value the value
     * @return the object
     */
    private ObjectNode enrichJsonValueAsPerAvroSchema(org.apache.avro.Schema valueSchema, Object value) {
        ObjectNode valueNode = mapper.convertValue(value, ObjectNode.class);
        ObjectNode updatedNode = mapper.createObjectNode();
        for (org.apache.avro.Schema.Field field : valueSchema.getFields()) {
            updateValueAsPerAvroSchema(field, field.schema(), valueNode, updatedNode, Boolean.FALSE);
        }
        return updatedNode;
    }

    /**
     * Update value as per avro schema.
     *
     * @param field the field
     * @param valueSchema the value schema
     * @param valueNode the value node
     * @param updatedNode the updated node
     * @param isUnionSubtype the is union subtype
     */
    private void updateValueAsPerAvroSchema(org.apache.avro.Schema.Field field, org.apache.avro.Schema valueSchema,
                                            ObjectNode valueNode, ObjectNode updatedNode, boolean isUnionSubtype) {
        String fieldName = field.name();
        switch (valueSchema.getType()) {
            case UNION:
                for (org.apache.avro.Schema unionSchema : field.schema().getTypes()) {
                    if (unionSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
                        continue;
                    } else {
                        updateValueAsPerAvroSchema(field, unionSchema, valueNode, updatedNode, Boolean.FALSE);//TODO: Check Default to pass Optional vs Boolean.TRUE
                    }
                }
                break;
            case ARRAY:
                updateValueAsPerArraySchema(field, valueSchema, valueNode, updatedNode, isUnionSubtype, fieldName);
                break;
            case RECORD:
                if (valueNode.get(fieldName) != null) {
                    ObjectNode recordNode = mapper.createObjectNode();
                    for (org.apache.avro.Schema.Field recordField : valueSchema.getFields()) {
                        updateValueAsPerAvroSchema(recordField, recordField.schema(),
                                (ObjectNode) valueNode.get(fieldName), recordNode, Boolean.FALSE);
                    }
                    if(isUnionSubtype) {
                        ObjectNode recordNamespaceNode = mapper.createObjectNode();
                        recordNamespaceNode.set(valueSchema.getFullName(), recordNode);
                        updatedNode.set(fieldName, recordNamespaceNode);
                    } else {
                        updatedNode.set(fieldName, recordNode);
                    }
                } else {
                    updatedNode.set(fieldName, null);
                }

                break;
            case STRING:
                JsonNode fieldValue = valueNode.get(fieldName);
                if (fieldValue != null) {
                    if(isUnionSubtype) {
                        ObjectNode innerNode = mapper.createObjectNode();
                        innerNode.put(Schema.Type.STRING.getName(), fieldValue.asText());
                        updatedNode.set(fieldName, innerNode);
                    } else {
                        updatedNode.put(fieldName, fieldValue.asText());
                    }

                } else {
                    updatedNode.set(fieldName, null);
                }

                break;
            case INT:
                JsonNode intValue = valueNode.get(fieldName);
                if (intValue != null) {
                    if(isUnionSubtype) {
                        ObjectNode innerNode = mapper.createObjectNode();
                        innerNode.put(org.apache.avro.Schema.Type.INT.getName(), intValue.asInt());
                        updatedNode.set(fieldName, innerNode);
                    } else {
                        updatedNode.put(fieldName, intValue.asInt());
                    }
                } else {
                    updatedNode.set(fieldName, null);
                }
                break;
            case LONG:
                JsonNode longValue = valueNode.get(field.name());
                if (longValue != null) {
                    long longVal;
                    if (TIMESTAMP_MILLIS.equals(valueSchema.getProp("logicalType"))) {
                        try {
                            longVal = Instant.parse(longValue.asText()).toEpochMilli();
                        } catch (DateTimeParseException e) {
                            log.error("Incorrect date string in record : " + longValue);
                            updatedNode.set(fieldName, null);
                            break;
                        }
                    } else {
                        longVal = longValue.asLong();
                    }
                    if(isUnionSubtype) {
                        ObjectNode innerNode = mapper.createObjectNode();
                        innerNode.put(org.apache.avro.Schema.Type.LONG.getName(), longVal);
                        updatedNode.set(fieldName, innerNode);
                    } else {
                        updatedNode.put(fieldName, longVal);
                    }
                } else {
                    updatedNode.set(fieldName, null);
                }
                break;
            case DOUBLE:
                JsonNode doubleValue = valueNode.get(field.name());
                if (doubleValue != null) {
                    if(isUnionSubtype) {
                        ObjectNode innerNode = mapper.createObjectNode();
                        innerNode.put(org.apache.avro.Schema.Type.DOUBLE.getName(), doubleValue.asDouble());
                        updatedNode.set(fieldName, innerNode);
                    } else {
                        updatedNode.put(fieldName, doubleValue.asDouble());
                    }
                } else {
                    updatedNode.set(fieldName, null);
                }
                break;
            case BOOLEAN:
                JsonNode boolValue = valueNode.get(fieldName);
                if (boolValue != null) {
                    if(isUnionSubtype) {
                        ObjectNode innerNode = mapper.createObjectNode();
                        innerNode.put(Schema.Type.BOOLEAN.getName(), boolValue.asBoolean());
                        updatedNode.set(fieldName, innerNode);
                    } else {
                        updatedNode.put(fieldName, boolValue.asBoolean());
                    }
                } else {
                    updatedNode.set(fieldName, null);
                }
                break;
            default:
                log.error(valueSchema.getName() + "not defined" + valueSchema.getType());
        }
    }

    /**
     * Update value as per array schema.
     *
     * @param field
     *            the field
     * @param valueSchema
     *            the value schema
     * @param valueNode
     *            the value node
     * @param updatedNode
     *            the updated node
     * @param isUnionSubtype
     *            the is union subtype
     * @param fieldName
     *            the field name
     */
    private void updateValueAsPerArraySchema(org.apache.avro.Schema.Field field, org.apache.avro.Schema valueSchema, ObjectNode valueNode,
                                             ObjectNode updatedNode, boolean isUnionSubtype, String fieldName) {
        ArrayNode arrayValue = (ArrayNode) valueNode.get(fieldName);
        ArrayNode updatedArrayNode = mapper.createArrayNode();
        org.apache.avro.Schema arrayElementType = valueSchema.getElementType();
        org.apache.avro.Schema unionSch = null;
        for (org.apache.avro.Schema unionSchema : arrayElementType.getTypes()) {
            if (unionSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
                continue;
            } else {
                unionSch = unionSchema;
            }
        }
        if (arrayValue != null) {
            for (JsonNode arrayElement : arrayValue) {
                if (!arrayElement.isNull()) {
                    ObjectNode innerNode = mapper.createObjectNode();
                    if (unionSch.getType().getName().equals(org.apache.avro.Schema.Type.RECORD.name().toLowerCase())) {
                        ObjectNode recordValueNode = mapper.createObjectNode();
                        recordValueNode.set(field.name(), arrayElement);
                        updateValueAsPerAvroSchema(field, unionSch, recordValueNode, innerNode,
                                Boolean.FALSE);
                    } else {
                        innerNode.set(unionSch.getType().getName(), arrayElement);
                    }
                    updatedArrayNode.add(innerNode);
                } else {
                    updatedArrayNode.add(arrayElement);
                }
            }
            if (isUnionSubtype) {
                ObjectNode outerNode = mapper.createObjectNode();
                outerNode.set(Schema.Type.ARRAY.getName(), updatedArrayNode);
                updatedNode.set(fieldName, outerNode);
            } else {
                updatedNode.set(fieldName, updatedArrayNode);
            }
        } else {
            updatedNode.set(fieldName, null);
        }
    }

}
