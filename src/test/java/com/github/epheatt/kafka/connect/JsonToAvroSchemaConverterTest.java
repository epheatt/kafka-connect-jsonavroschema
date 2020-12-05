package com.github.epheatt.kafka.connect;

import com.google.common.collect.ImmutableMap;
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
            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://fake-url");

    private static final ObjectMapper mapper = new ObjectMapper();
    private final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    private final JsonToAvroSchemaConverter converter = new JsonToAvroSchemaConverter(schemaRegistry);
    private final AvroData avroData = new AvroData(new AvroDataConfig(SR_CONFIG));

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
    public  void testAvroSchemaOptionalStruct() throws IOException, RestClientException {
        String TEST_TOPIC = "testAvroSchemaOptionalStruct";
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        //org.apache.avro.Schema avroSchema1 = parser.parse("{\"type\":\"record\",\"name\":\"Account\",\"namespace\":\"onep.mcafee\",\"fields\":[{\"name\":\"Id\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"IsDeleted\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"MasterRecordId\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"Name\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"Type\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"RecordTypeId\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"ParentId\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"BillingStreet\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"BillingCity\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"BillingState\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"BillingPostalCode\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"20\"}}],\"default\":null},{\"name\":\"BillingCountry\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"BillingLatitude\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"BillingLongitude\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"BillingGeocodeAccuracy\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"BillingAddress\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"BillingAddress0\",\"fields\":[{\"name\":\"GeocodeAccuracy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"State\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Street\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"PostalCode\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Country\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Latitude\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"City\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Longitude\",\"type\":[\"null\",\"double\"],\"default\":null}]}],\"default\":null},{\"name\":\"ShippingStreet\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ShippingCity\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"ShippingState\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"ShippingPostalCode\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"20\"}}],\"default\":null},{\"name\":\"ShippingCountry\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"ShippingLatitude\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ShippingLongitude\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ShippingGeocodeAccuracy\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"ShippingAddress\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"ShippingAddress1\",\"fields\":[{\"name\":\"GeocodeAccuracy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"State\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Street\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"PostalCode\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Country\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Latitude\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"City\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Longitude\",\"type\":[\"null\",\"double\"],\"default\":null}]}],\"default\":null},{\"name\":\"Phone\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"Fax\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"AccountNumber\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"Website\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"PhotoUrl\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"Sic\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"20\"}}],\"default\":null},{\"name\":\"Industry\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"AnnualRevenue\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"NumberOfEmployees\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"Ownership\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"TickerSymbol\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"20\"}}],\"default\":null},{\"name\":\"Description\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"Rating\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"Site\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"CurrencyIsoCode\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"3\"}}],\"default\":null},{\"name\":\"OwnerId\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"CreatedDate\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"CreatedById\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"LastModifiedDate\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"LastModifiedById\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SystemModstamp\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"LastActivityDate\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"LastViewedDate\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"LastReferencedDate\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"Jigsaw\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"20\"}}],\"default\":null},{\"name\":\"JigsawCompanyId\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"20\"}}],\"default\":null},{\"name\":\"CleanStatus\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"AccountSource\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"DunsNumber\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"9\"}}],\"default\":null},{\"name\":\"Tradestyle\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"NaicsCode\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"8\"}}],\"default\":null},{\"name\":\"NaicsDesc\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"120\"}}],\"default\":null},{\"name\":\"YearStarted\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"4\"}}],\"default\":null},{\"name\":\"SicDesc\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"ServiceSource1__CHL_Partner_Account_Type__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"ServiceSource1__CHL_Prevent_Portal_Download__c\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"ServiceSource1__CSM_Account_Has_Success_Plan__c\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"ServiceSource1__CSM_Average_SLA_Response_Time__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"50\"}}],\"default\":null},{\"name\":\"ServiceSource1__CSM_Health_Status__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"ServiceSource1__CSM_Last_Health_Status__c\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"ServiceSource1__CSM_Number_of_Cases__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ServiceSource1__CSM_Number_of_Closed_Cases__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ServiceSource1__CSM_Number_of_New_Cases__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ServiceSource1__CSM_Number_of_Open_High_Priority_Cases__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ServiceSource1__CSM_Number_of_Working_Cases__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ServiceSource1__CSM_Oldest_Open_Case__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"ServiceSource1__CSM_Overdue_Tasks_Count__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ServiceSourceQ__Active__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Checkbox_1__c\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Checkbox_2__c\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Currency_1__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Currency_2__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_DateTime_1__c\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"SSI_ZTH__Account_DateTime_2__c\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"SSI_ZTH__Account_ExternalID__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Loc_Address_Lookup_1__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Loc_Address_Lookup_2__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Loc_Address_Lookup_3__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Loc_Address_Lookup_4__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Location__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Lookup_1__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Lookup_2__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Lookup_3__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Number_1__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Number_2__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Number_3__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Number_4__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Picklist_1__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Picklist_2__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Picklist_3__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Picklist_4__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Picklist_5__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_10__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"100\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_1__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"100\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_2__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"100\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_3__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"100\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_4__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"100\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_5__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_6__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_7__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_8__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Text_9__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"100\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Client_Company_ID__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Data_Load_ID__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"40\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Data_Quality_Description__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"1300\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Data_Quality_Score__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Email__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"80\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Inactive__c\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"SSI_ZTH__Portal_Company_IDs__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"100\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Primary_Contact__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__SREV_Account_Type__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Sales_Center__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__SourceGUID__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"JBCXM__CustomerInfo__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"Business_Value__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"RLM_Tier__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"Upsell_Cross_sell_Potential_c__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"Upsell_Cross_sell_Product__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"X18_Digit_ID__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"1300\"}}],\"default\":null},{\"name\":\"ACV_tier__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"ServiceSource1__REN_Asperato_Customer_Id__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"20\"}}],\"default\":null},{\"name\":\"ServiceSource1__REN_Count_of_Assets__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"ServiceSource1__REN_Count_of_Opportunities__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_Date_1__c\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Date_2__c\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"SSI_ZTH__Account_ExternalID2__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Multi_Picklist_1__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"4099\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Multi_Picklist_2__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"4099\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Multi_Picklist_3__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"4099\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Multi_Picklist_4__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"4099\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Multi_Picklist_5__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"4099\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Multi_Picklist_6__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"4099\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Picklist_6__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Picklist_7__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_Po1__c\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"SSI_ZTH__Account_User_Lookup_1__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_User_Lookup_2__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_User_Lookup_3__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_User_Lookup_4__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_User_Lookup_5__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Account_User_Lookup_6__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"18\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Customer_Risk__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Customer_Start_Date__c\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"SSI_ZTH__External_ID__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"SSI_ZTH__Risk_date__c\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"SSI_ZTH__SFDC_18_digit_ID__c\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"1300\"}}],\"default\":null},{\"name\":\"extensions\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"extension\",\"namespace\":\"\",\"fields\":[{\"name\":\"master\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"master\",\"fields\":[{\"name\":\"location\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"location\",\"fields\":[{\"name\":\"value\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"primaryContact\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"primaryContact\",\"fields\":[{\"name\":\"value\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null}]}],\"default\":null}]}],\"default\":null},{\"name\":\"_id\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"24\"}}]},{\"name\":\"systemProperties\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"systemProperties\",\"namespace\":\"\",\"fields\":[{\"name\":\"archivedOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"revisionId\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"sfdcRevisionId\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"tenant\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"createdOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"createdBy\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"lastModifiedOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"lastModifiedBy\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"expiredOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"transId\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"string\"],\"default\":null}],\"default\":null}]}]}]}");
        org.apache.avro.Schema avroSchema1 = parser.parse("{\"type\":\"record\",\"name\":\"custom_field_values_users\",\"namespace\":\"mavenlink\",\"fields\":[{\"name\":\"subject_type\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"subject_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"value\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"account_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"custom_field_name\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"type\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"display_value\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"can_edit\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"created_at\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"updated_at\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"custom_field_id\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"setter_id\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"id\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"length\":\"255\"}}],\"default\":null},{\"name\":\"_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"systemProperties\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"systemProperties\",\"namespace\":\"\",\"fields\":[{\"name\":\"archivedOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"revisionId\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"sfdcRevisionId\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"tenant\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"createdOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"createdBy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"lastModifiedOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"lastModifiedBy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"expiredOn\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"transId\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"string\"],\"default\":null}],\"default\":null}]}],\"default\":null}]}");
        Schema connectSchema1 = avroData.toConnectSchema(avroSchema1);
        ObjectNode jsonSchema1 = converter.asJsonSchema(connectSchema1);
        int id = schemaRegistry.register(TEST_TOPIC, avroSchema1);
        org.apache.avro.Schema avroSchema2 = schemaRegistry.getById(id);
        //String msg = "{ \"Id\": \"foo\" }";
        String msg = "{\"subject_type\": \"user\",\"subject_id\": 10337205,\"account_id\": 5355095, \"custom_field_name\": \"User Portfolio\",\"type\": \"single\",\"display_value\": \"Technical\",\"can_edit\":true,\"created_at\": \"2018-07-31T12:48:39-07:00\",\"updated_at\": \"2019-03-06T08:34:06-08:00\",\"custom_field_id\": \"431355\",\"setter_id\": \"10287465\",\"id\":\"1\"}";
        SchemaAndValue converted = converter.toConnectData(TEST_TOPIC, msg.getBytes());
        String convertedBack = new String(converter.fromConnectData(TEST_TOPIC,converted.schema(),converted.value()));
        assertEquals(msg, convertedBack);
    }

    @Test
    @Ignore
    public void testAvroSchemaToJsonSchema() {
        org.apache.avro.Schema avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        //avroStringSchema.addProp("length",
        //        com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.numberNode(255));
        avroStringSchema.addProp("connect.name", "io.confluent.stringtype");
        avroStringSchema.addProp("connect.version",
                com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.numberNode(2));
        avroStringSchema.addProp("default", "foo");
        //avroStringSchema.addProp("doc", "doc");
        avroStringSchema.addProp("connect.doc", "doc");
        avroStringSchema.addProp("connect.default", "foo");
        avroStringSchema.addProp("connect.name", "io.confluent.stringtype");
        Map<String, String> connectPropsMap = ImmutableMap.of(
                "foo","bar",
                "baz","baz",
                "length","255");
        com.fasterxml.jackson.databind.node.ObjectNode params = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
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
    @Ignore
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
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://fake-url");
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
