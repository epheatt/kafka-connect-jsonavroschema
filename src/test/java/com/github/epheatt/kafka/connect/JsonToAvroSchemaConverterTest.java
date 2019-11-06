package com.github.epheatt.kafka.connect;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonSchema;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonToAvroSchemaConverterTest {
    private static final String TOPIC = "topic";

    private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://fake-url");

    private static final ObjectMapper mapper = new ObjectMapper();
    private final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    private final JsonToAvroSchemaConverter converter = new JsonToAvroSchemaConverter(schemaRegistry);
    private final AvroData avroData = new AvroData(new AvroDataConfig(SR_CONFIG));

    /*
    public JsonToAvroSchemaConverterTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new JsonToAvroSchemaConverter(schemaRegistry);
        avroData = new AvroData(new AvroDataConfig(SR_CONFIG));
    }
    */

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
    @Ignore
    public void testAvroSchemaToJsonSchema() {
        org.apache.avro.Schema avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        //avroStringSchema.addProp("length",
        //        org.codehaus.jackson.node.JsonNodeFactory.instance.numberNode(255));
        avroStringSchema.addProp("connect.name", "io.confluent.stringtype");
        avroStringSchema.addProp("connect.version",
                org.codehaus.jackson.node.JsonNodeFactory.instance.numberNode(2));
        avroStringSchema.addProp("default", "foo");
        //avroStringSchema.addProp("doc", "doc");
        avroStringSchema.addProp("connect.doc", "doc");
        avroStringSchema.addProp("connect.default", "foo");
        avroStringSchema.addProp("connect.name", "io.confluent.stringtype");
        Map<String, String> connectPropsMap = ImmutableMap.of(
                "foo","bar",
                "baz","baz",
                "length","255");
        org.codehaus.jackson.node.ObjectNode params = org.codehaus.jackson.node.JsonNodeFactory.instance.objectNode();
        avroStringSchema.addProp("connect.parameters", connectPropsMap);
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record(TOPIC).fields()
                .requiredBoolean("field1")
                .requiredString("field2")
                .name("field3").type(avroStringSchema).noDefault()
                .endRecord();
        Schema connectSchema1 = avroData.toConnectSchema(avroSchema1);
        ObjectNode jsonSchema1 = converter.asJsonSchema(connectSchema1);
        Schema connectSchema2 = converter.asConnectSchema(jsonSchema1);
        org.apache.avro.Schema avroSchema2 = avroData.fromConnectSchema(connectSchema2);


        Schema expectedSchema = SchemaBuilder
                .struct().name(TOPIC)
                .field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", SchemaBuilder.string().parameters(connectPropsMap))
                .build();

        ObjectNode jsonSchema2 = converter.asJsonSchema(expectedSchema);
        Schema connectSchema3 = converter.asConnectSchema(jsonSchema2);
        org.apache.avro.Schema avroSchema3 = avroData.fromConnectSchema(connectSchema3);


        assertEquals(avroSchema1,avroSchema2);
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
    public void testNestedStructWithNullToConnect() throws IOException, RestClientException {
        String TEST_TOPIC = "testNestedStructWithNullToConnect";

        org.apache.avro.Schema nestedField4Schema = org.apache.avro.SchemaBuilder
                .record("field4").fields()
                .optionalString("field5")
                .endRecord();

        org.apache.avro.Schema nestedSchema = org.apache.avro.SchemaBuilder
                .record("field3").fields()
                .name("field4").type().optional().type(nestedField4Schema)
                .endRecord();
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record(TEST_TOPIC).fields()
                .requiredBoolean("field1")
                .requiredString("field2")
                .name("field3").type(nestedSchema).noDefault()
                .endRecord();
        schemaRegistry.register(TEST_TOPIC, avroSchema1);

        Schema field4Schema = SchemaBuilder
                .struct().optional().name("field4")
                .field("field5",Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema field3Schema = SchemaBuilder
                .struct().name("field3")
                .field("field4", field4Schema)
                .build();
        Schema expectedSchema = SchemaBuilder
                .struct().name(TEST_TOPIC)
                .field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", field3Schema)
                .build();
        Struct field3Expected = new Struct(field3Schema)
                .put("field4", null);
        Struct expected = new Struct(expectedSchema)
                .put("field1", true)
                .put("field2", "string")
                .put("field3", field3Expected);

        String msg = "{ \"field1\": true, \"field2\": \"string\", \"field3\": { \"field4\":null} }";
        SchemaAndValue converted = converter.toConnectData(TEST_TOPIC, msg.getBytes());
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

    @Test
    public void testArrayToConnect() throws IOException, RestClientException {
        String TEST_TOPIC = "testArrayToConnect";
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record(TEST_TOPIC).fields()
                .requiredBoolean("field1")
                .requiredString("field2")
                .name("field3").type().nullable().array().items().nullable().stringType().noDefault()
                .endRecord();
        schemaRegistry.register(TEST_TOPIC, avroSchema1);

        Schema expectedSchema = SchemaBuilder
                .struct().name(TEST_TOPIC)
                .field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .build();
        Struct expected = new Struct(expectedSchema)
                .put("field1", true)
                .put("field2", "string")
                .put("field3", Arrays.asList("test1", null, "test2"));

        String msg = "{ \"field1\": true, \"field2\": \"string\", \"field3\": [\"test1\",null,\"test2\"] }";
        SchemaAndValue converted = converter.toConnectData(TEST_TOPIC, msg.getBytes());
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

	@Test
	public void testArrayWithStructToConnect() throws IOException, RestClientException {
		String TEST_TOPIC = "testArrayWithStructToConnect";
		org.apache.avro.Schema innerRecordSchema = org.apache.avro.SchemaBuilder
				.record("field3").fields()
				.optionalString("field4")
				.optionalLong("field5")
				.endRecord();

		org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
				.record(TEST_TOPIC).fields()
				.requiredBoolean("field1")
				.requiredString("field2")
				.name("field3").type().nullable().array().items().nullable().type(innerRecordSchema).noDefault()
				.endRecord();

		schemaRegistry.register(TEST_TOPIC, avroSchema);

		Schema expectedInnerRecordSchema = SchemaBuilder
		        .struct().optional().name("field3")
				.field("field4", Schema.OPTIONAL_STRING_SCHEMA)
				.field("field5", Schema.OPTIONAL_INT64_SCHEMA).build();

		Schema expectedSchema = SchemaBuilder
				.struct().name(TEST_TOPIC)
				.field("field1", Schema.BOOLEAN_SCHEMA)
				.field("field2", Schema.STRING_SCHEMA)
				.field("field3", SchemaBuilder.array(expectedInnerRecordSchema).optional().build())
				.build();

		Struct expected1 = new Struct(expectedInnerRecordSchema)
				.put("field4", "xyz")
				.put("field5", new Long(20));
		Struct expected2 = new Struct(expectedInnerRecordSchema)
				.put("field4", "abc")
				.put("field5", new Long(40));

		Struct expected = new Struct(expectedSchema)
				.put("field1", true)
				.put("field2", "string")
				.put("field3", Arrays.asList(expected1, expected2));

		String msg = "{ \"field1\": true, \"field2\": \"string\", \"field3\": [{\"field4\":\"xyz\",\"field5\":20},{\"field4\":\"abc\",\"field5\":40}] }";
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
