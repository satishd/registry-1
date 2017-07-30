/*
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.ErrorMessage;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.Id;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.SchemaString;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.SchemaVersionEntry;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hortonworks.registries.schemaregistry.avro.ConfluentProtocolCompatibleTest.GENERIC_TEST_RECORD_SCHEMA;
import static com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.INVALID_SCHEMA_RESPONSE_STATUS;
import static com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.SUBJECT_NOT_FOUND_ERROR_CODE;

/**
 * Tests related to APIs exposed with {@link ConfluentSchemaRegistryCompatibleResource}
 */
public class ConfluentRegistryCompatibleResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentRegistryCompatibleResourceTest.class);

    @Rule
    public TestName testNameRule = new TestName();

    private WebTarget rootTarget;
    private LocalSchemaRegistryServer localSchemaRegistryServer;

    @Before
    public void setup() throws Exception {
        String configPath = new File(Resources.getResource("schema-registry-test.yaml").toURI()).getAbsolutePath();
        localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath);
        localSchemaRegistryServer.start();
        String rootUrl = String.format("http://localhost:%d/api/v1/confluent", localSchemaRegistryServer.getLocalPort());
        rootTarget = createRootTarget(rootUrl);
    }

    @After
    public void cleanup() throws Exception {
        if(localSchemaRegistryServer != null) {
            localSchemaRegistryServer.stop();
        }
    }

    @Test
    public void testConfluentSerDes() throws Exception {

        Schema schema = new Schema.Parser().parse(GENERIC_TEST_RECORD_SCHEMA);
        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();

        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, rootTarget.getUri().toString());

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
        kafkaAvroSerializer.configure(config, false);
        byte[] bytes = kafkaAvroSerializer.serialize("topic", record);

        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
        kafkaAvroDeserializer.configure(config, false);

        GenericRecord result = (GenericRecord) kafkaAvroDeserializer.deserialize("topic", bytes);
        LOG.info(result.toString());
    }

    @Test
    public void testConfluentApis() throws Exception {
        List<String> schemas = Arrays.stream(new String[]{"/device.avsc", "/device-compat.avsc", "/device-incompat.avsc"})
                                     .map(x -> {
                                         try {
                                             return fetchSchema(x);
                                         } catch (IOException e) {
                                             throw new RuntimeException(e);
                                         }
                                     })
                                     .collect(Collectors.toList());

        ObjectMapper objectMapper = new ObjectMapper();
        List<String> subjects = new ArrayList<>();
        int ct = 1;
        for (String schemaText : schemas) {
            String subjectName = testName() + ct++;
            subjects.add(subjectName);

            // check post schema version
            Long version = objectMapper.readValue(postSubjectSchema(subjectName, rootTarget, schemaText)
                                                          .readEntity(String.class), Id.class)
                                       .getId();

            // check get version api
            SchemaVersionEntry schemaVersionEntry = getVersion(rootTarget, subjectName, version.toString());

            Assert.assertEquals(subjectName, schemaVersionEntry.getName());
            Assert.assertEquals(version.intValue(), schemaVersionEntry.getVersion().intValue());
            Schema recvdSchema = new Schema.Parser().parse(schemaVersionEntry.getSchema());
            Schema regdSchema = new Schema.Parser().parse(schemaText);
            Assert.assertEquals(regdSchema, recvdSchema);
        }

        // check all registered subjects
        List<String> recvdSubjects = getAllSubjects(rootTarget);
        Assert.assertEquals(new HashSet<>(subjects), new HashSet<>(recvdSubjects));
    }

    private String testName() {
        return testNameRule.getMethodName();
    }

    private WebTarget createRootTarget(String rootUrl) {
        Client client = ClientBuilder.newBuilder()
                                     .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE)
                                     .build();
        client.register(MultiPartFeature.class);
        return client.target(rootUrl);
    }

    @Test
    public void testInValidSchemas() throws Exception {
        // add invalid schema
        Response invalidSchemaResponse = postSubjectSchema(testName(),
                                                           rootTarget,
                                                           fetchSchema("/device-unsupported-type.avsc"));
        Assert.assertEquals(INVALID_SCHEMA_RESPONSE_STATUS, invalidSchemaResponse.getStatus());

        ErrorMessage errorMessage = new ObjectMapper().readValue(invalidSchemaResponse.readEntity(String.class),
                                                                 ErrorMessage.class);
        Assert.assertEquals(ConfluentSchemaRegistryCompatibleResource.INVALID_SCHEMA_ERROR_CODE,
                            errorMessage.getErrorCode());
    }


    @Test
    public void testIncompatibleSchemas() throws Exception {
        String subject = testName();
        String response = postSubjectSchema(subject,
                                            rootTarget,
                                            fetchSchema("/device.avsc")).readEntity(String.class);
        ObjectMapper objectMapper = new ObjectMapper();
        long id = objectMapper.readValue(response, Id.class).getId();
        Assert.assertTrue(id > 0);

        // add incompatible schema
        Response incompatSchemaResponse = postSubjectSchema(subject, rootTarget,
                                                            fetchSchema("/device-incompat.avsc"));
        Assert.assertEquals(Response.Status.CONFLICT.getStatusCode(), incompatSchemaResponse.getStatus());

        ErrorMessage errorMessage = objectMapper.readValue(incompatSchemaResponse.readEntity(String.class),
                                                           ErrorMessage.class);
        Assert.assertEquals(ConfluentSchemaRegistryCompatibleResource.INCOMPATIBLE_SCHEMA_ERROR_CODE,
                            errorMessage.getErrorCode());
    }

    @Test
    public void testNonExistingSubject() throws Exception {
        // check non existing subject
        Response nonExistingSubjectResponse = rootTarget.path("/subjects/" + testName() + "/versions")
                                                        .request(MediaType.APPLICATION_JSON_TYPE)
                                                        .get();
        ErrorMessage errorMessage = new ObjectMapper().readValue(nonExistingSubjectResponse.readEntity(String.class),
                                                                 ErrorMessage.class);
        Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
                            nonExistingSubjectResponse.getStatus());

        Assert.assertEquals(SUBJECT_NOT_FOUND_ERROR_CODE, errorMessage.getErrorCode());
    }

    private List<String> getAllSubjects(WebTarget rootTarget) throws IOException {
        String subjectsResponse = rootTarget.path("/subjects").request().get(String.class);
        return new ObjectMapper().readValue(subjectsResponse, new TypeReference<List<String>>() {
        });
    }

    private SchemaVersionEntry getVersion(WebTarget rootTarget, String subjectName, String version) throws IOException {
        WebTarget versionsTarget = rootTarget.path("/subjects/" + subjectName + "/versions/");
        String versionsResponse = versionsTarget.path(version).request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        return new ObjectMapper().readValue(versionsResponse, SchemaVersionEntry.class);
    }

    private Response postSubjectSchema(String subjectName, WebTarget rootTarget, String schemaText) throws IOException {
        WebTarget subjectsTarget = rootTarget.path("/subjects/" + subjectName + "/versions");
        SchemaString schemaString = new SchemaString();
        schemaString.setSchema(schemaText);
        return subjectsTarget.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaString));
    }

    private String fetchSchema(String filePath) throws IOException {
        return IOUtils.toString(this.getClass().getResourceAsStream(filePath),
                                "UTF-8");
    }

}
