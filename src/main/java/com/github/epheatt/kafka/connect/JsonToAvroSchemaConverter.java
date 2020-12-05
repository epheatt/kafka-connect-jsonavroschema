package com.github.epheatt.kafka.connect;


import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** <p>
 * This is a custom converter. It can be used to read kafka data which is in json string format. If schema is enable then it converts into embedded
 * schema json format. It uses schema registry to fetch the schema for topic as subject. It assumes schema registry is returning avro schema. It
 * enriches json data with respect to avro schema.
 * </p> */
public class JsonToAvroSchemaConverter extends JsonConverter {
    private static final Logger log = LoggerFactory.getLogger(JsonToAvroSchemaConverter.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    private static final boolean SCHEMAS_ENABLE_DEFAULT = true;
    private static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String TYPE_CONFIG = "converter.type";

    private static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
    private static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";
    private static final String ENVELOPE_PAYLOAD_POINTER = "payload.pointer";

    private SchemaRegistryClient schemaRegistry;
    private boolean isKey;
    private JsonToAvroSchemaConverterConfig conf;
    private AvroData avroData;
    private String payloadPointer;

    private boolean enableSchemas = SCHEMAS_ENABLE_DEFAULT;
    private int cacheSize = SCHEMAS_CACHE_SIZE_DEFAULT;
    private Cache<String, org.apache.avro.Schema> topicSchemaCache;
    private final JsonSerializer serializer = new JsonSerializer();
    private final JsonDeserializer deserializer = new JsonDeserializer();
    private final JsonConverter jsonConverter = new JsonConverter();

    public JsonToAvroSchemaConverter(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    public JsonToAvroSchemaConverter() {
        // no code is required here
    }

    public void configure(Map<String, ?> configs) {
        AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);

        Object enableConfigsVal = configs.get(SCHEMAS_ENABLE_CONFIG);
        if (enableConfigsVal != null)
            enableSchemas = enableConfigsVal.toString().equals("true");
        Object cacheSizeVal = configs.get(SCHEMAS_CACHE_SIZE_CONFIG);
        if (cacheSizeVal != null)
            cacheSize = Integer.parseInt((String) cacheSizeVal);
        Object typeVal = configs.get(TYPE_CONFIG);
        if (typeVal != null)
            isKey = typeVal.toString().equals(ConverterType.KEY.name());
        payloadPointer = (String) configs.getOrDefault(ENVELOPE_PAYLOAD_POINTER, null);
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
        super.configure(configs);
        jsonConverter.configure(configs);
        avroData = new AvroData(new AvroDataConfig(configs));
        conf = new JsonToAvroSchemaConverterConfig(configs);
        topicSchemaCache = new SynchronizedCache<>(new LRUCache<>(conf.getSchemaCacheSize()));

        if (schemaRegistry == null) {
            schemaRegistry = new CachedSchemaRegistryClient(avroConverterConfig.getSchemaRegistryUrls(),
                    avroConverterConfig.getMaxSchemasPerSubject());
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> configuration = new HashMap<>(configs);
        configuration.put(ConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.name().toLowerCase() : ConverterType.VALUE.name().toLowerCase());
        configure(configuration);
    }

    @Override
    public ConfigDef config() {
        return JsonToAvroSchemaConverterConfig.configDef();
    }

    /*
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return super.fromConnectData(topic, schema, value);
    }
    */

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
            envelope.set(ENVELOPE_PAYLOAD_FIELD_NAME, jsonValue);
            jsonValue = envelope;
        }

        try {
            return jsonConverter.toConnectData(topic, serializer.serialize(topic, jsonValue));
        } catch (SerializationException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /*
    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return toConnectData(topic, value);
    }
    */
    /** Fetch avro schema from schema registry.
     *
     * @param subject
     *            the subject
     * @return the schema */
    @SuppressWarnings("deprecation")
    private org.apache.avro.Schema fetchAvroSchemaFromSchemaRegistry(String subject) {
        org.apache.avro.Schema schema = topicSchemaCache.get(subject);
        if (schema == null) {
            try {
                int schemaId = schemaRegistry.getLatestSchemaMetadata(subject).getId();
                schema = schemaRegistry.getBySubjectAndId(subject, schemaId);
                topicSchemaCache.put(subject, schema);
            } catch (Exception e) {
                String errorMessage = "Error in fetching schema from registry for subject : " + subject;
                log.error(errorMessage, e);
                throw new RetriableException(errorMessage, e);
            }
        }
        return schema;
    }

}
