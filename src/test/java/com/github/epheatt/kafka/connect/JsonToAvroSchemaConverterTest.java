package com.github.epheatt.kafka.connect;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.connect.avro.AvroData;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonToAvroSchemaConverterTest {
    private static final String TOPIC = "topic";

    private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://fake-url");

    private final SchemaRegistryClient schemaRegistry;
    private final JsonToAvroSchemaConverter converter;

    public JsonToAvroSchemaConverterTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new JsonToAvroSchemaConverter(schemaRegistry);
    }

    @Before
    public void setUp() {
        converter.configure(SR_CONFIG, false);
    }

    @Test
    public void testConfigure() {
        converter.configure(SR_CONFIG, false);
        assertFalse(Whitebox.<Boolean>getInternalState(converter, "isKey"));
        //assertNotNull(Whitebox.getInternalState(
        //        Whitebox.<AbstractKafkaAvroSerDe>getInternalState(converter, "serializer"),
        //        "schemaRegistry"));
    }

    @Test
    public void testStructToConnect() throws IOException, RestClientException {
        String TEST_TOPIC = "testStructToConnect";
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record(TEST_TOPIC).fields()
                .requiredBoolean("field1")
                .requiredString("field2")
                .endRecord();
        schemaRegistry.register(TEST_TOPIC, avroSchema1);

        Schema expectedSchema = SchemaBuilder.struct().name(TEST_TOPIC).field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).build();
        Struct expected = new Struct(expectedSchema).put("field1", true).put("field2", "string");

        String msg = "{ \"field1\": true, \"field2\": \"string\" }";
        SchemaAndValue converted = converter.toConnectData(TEST_TOPIC, msg.getBytes());
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

    @Test
    public void testNestedStructToConnect() throws IOException, RestClientException {
        String TEST_TOPIC = "testNestedStructToConnect";
        org.apache.avro.Schema nestedSchema = org.apache.avro.SchemaBuilder
                .record("field3").fields()
                .requiredString("field4")
                .endRecord();
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record(TEST_TOPIC).fields()
                .requiredBoolean("field1")
                .requiredString("field2")
                .name("field3").type(nestedSchema).noDefault()
                .endRecord();
        schemaRegistry.register(TEST_TOPIC, avroSchema1);

        Schema field3Schema = SchemaBuilder
                .struct().name("field3")
                .field("field4",Schema.STRING_SCHEMA)
                .build();
        Schema expectedSchema = SchemaBuilder
                .struct().name(TEST_TOPIC)
                .field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", field3Schema)
                .build();
        Struct field3Expected = new Struct(field3Schema)
                .put("field4","val");
        Struct expected = new Struct(expectedSchema)
                .put("field1", true)
                .put("field2", "string")
                .put("field3", field3Expected);

        String msg = "{ \"field1\": true, \"field2\": \"string\", \"field3\": { \"field4\": \"val\"} }";
        SchemaAndValue converted = converter.toConnectData(TEST_TOPIC, msg.getBytes());
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

    @Test
    public void testTimestampToConnect() throws IOException, RestClientException {
        String TEST_TOPIC = "testTimestampToConnect";
        org.apache.avro.Schema timestampMilliType = org.apache.avro.LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record(TEST_TOPIC).fields()
                .requiredBoolean("field1")
                .requiredString("field2")
                .name("field3").type(timestampMilliType).noDefault()
                .endRecord();
        schemaRegistry.register(TEST_TOPIC, avroSchema1);

        Schema expectedSchema = SchemaBuilder.struct().name(TEST_TOPIC).field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).field("field3", Timestamp.SCHEMA).required().build();
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        java.util.Date reference = calendar.getTime();
        Struct expected = new Struct(expectedSchema).put("field1", true).put("field2", "string").put("field3",reference);

        String msg = "{ \"field1\": true, \"field2\": \"string\", \"field3\": \"1970-01-01T00:00:00.000Z\" }";
        SchemaAndValue converted = converter.toConnectData(TEST_TOPIC, msg.getBytes());
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

    @Test
    public void testPayloadPointer() throws IOException, RestClientException {
        HashMap<String, String> config = new HashMap<String, String>(2);
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://fake-url");
        config.put("payload.pointer", "/doc");
        converter.configure(config, false);

        String TEST_TOPIC = "testPayloadPointer";
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record(TEST_TOPIC).fields()
                .requiredBoolean("field1")
                .requiredString("field2")
                .endRecord();
        schemaRegistry.register(TEST_TOPIC, avroSchema1);

        Schema expectedSchema = SchemaBuilder.struct().name(TEST_TOPIC).field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).build();
        Struct expected = new Struct(expectedSchema).put("field1", true).put("field2", "string");

        String msg = "{ \"doc\": { \"field1\": true, \"field2\": \"string\" }, \"patch\": {\"op\": \"add\", \"path\": \"/field2\", \"value\": \"string\"} }";
        SchemaAndValue converted = converter.toConnectData(TEST_TOPIC, msg.getBytes());
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }
}
