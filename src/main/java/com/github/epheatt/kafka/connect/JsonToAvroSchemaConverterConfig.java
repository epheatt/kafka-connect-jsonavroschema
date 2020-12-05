package com.github.epheatt.kafka.connect;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.storage.ConverterConfig;

/** This config class will contain additional converter config which is required for string to avro conversion. */
public class JsonToAvroSchemaConverterConfig extends ConverterConfig {

    private static final ConfigDef CONFIG;
    private static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMAS_CACHE_SIZE_DOC = "size of the schema cache.";
    private static final String ENVELOPE_PAYLOAD_POINTER = "payload.pointer";
    private static final String ENVELOPE_PAYLOAD_POINTER_DOC = "field name which contains actual payload to forward to connect.";
    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String SCHEMA_REGISTRY_URL_DOC = "Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas.";

    static {
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(SCHEMAS_CACHE_SIZE_CONFIG, Type.INT, SCHEMAS_CACHE_SIZE_DEFAULT, Importance.HIGH, SCHEMAS_CACHE_SIZE_DOC, null, -1,
                Width.MEDIUM, SCHEMAS_CACHE_SIZE_CONFIG);
        CONFIG.define(ENVELOPE_PAYLOAD_POINTER, Type.STRING, null, Importance.HIGH, ENVELOPE_PAYLOAD_POINTER_DOC, null, -1, Width.MEDIUM,
                ENVELOPE_PAYLOAD_POINTER);
        CONFIG.define(SCHEMA_REGISTRY_URL_CONFIG, Type.STRING, null, Importance.HIGH, SCHEMA_REGISTRY_URL_DOC, null, -1, Width.MEDIUM,
                SCHEMA_REGISTRY_URL_CONFIG);
    }

    /** @param props
     *            properties. */
    public JsonToAvroSchemaConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /** @return the config defination. */
    public static ConfigDef configDef() {
        return CONFIG;
    }

    public int getSchemaCacheSize() {
        return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }

    public String getPayloadPointer() {
        return getString(ENVELOPE_PAYLOAD_POINTER);
    }

    public String getSchemaRegistryUrls() {
        return getString(SCHEMA_REGISTRY_URL_CONFIG);
    }
}
