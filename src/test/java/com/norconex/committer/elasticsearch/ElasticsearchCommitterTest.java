/* Copyright 2013-2020 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.elasticsearch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.TimeIdGenerator;
import com.norconex.commons.lang.io.IOUtil;
import com.norconex.commons.lang.map.Properties;

@Testcontainers
class ElasticsearchCommitterTest {

    private static final Logger LOG = LoggerFactory.getLogger(
            ElasticsearchCommitterTest.class);

    private static final String TEST_ES_VERSION = "7.8.1";
    private static final String TEST_INDEX = "tests";
    private static final String TEST_ID = "1";
    private static final String TEST_CONTENT = "This is test content.";
    private static final String INDEX_ENDPOINT = "/" + TEST_INDEX + "/";
    private static final String CONTENT_FIELD =
            ElasticsearchCommitter.DEFAULT_ELASTICSEARCH_CONTENT_FIELD;

    @TempDir
    static File tempDir;

    @Container
    static ElasticsearchContainer container = new ElasticsearchContainer(
            new DockerImageName(
//                    "docker.elastic.co/elasticsearch/elasticsearch-oss",
                    "docker.elastic.co/elasticsearch/elasticsearch",
                    TEST_ES_VERSION).toString());


    private static RestClient restClient;
//    private static String node;

    @BeforeAll
    static void beforeAll() throws Exception {
//        elastic = EmbeddedElastic.builder()
//                .withElasticVersion(TEST_ES_VERSION)
//                .withSetting(PopularProperties.CLUSTER_NAME, "test_cluster")
//                .withStartTimeout(5, TimeUnit.MINUTES)
//                .build()
//                .start();
//
//        node = "http://localhost:" + elastic.getHttpPort();
//
//        restClient = RestClient.builder(HttpHost.create(node)).build();
        restClient = RestClient.builder(
                HttpHost.create(container.getHttpHostAddress())).build();
    }

    @BeforeEach
    void beforeEach() throws Exception {
//        restClient = RestClient.builder(
//                HttpHost.create(container.getHttpHostAddress())).build();

        if (restClient.performRequest(new Request("HEAD", "/" + TEST_INDEX))
                .getStatusLine().getStatusCode() == 200) {
            performRequest("DELETE", "/" + TEST_INDEX);
            performRequest("POST", "_flush");
            performRequest("PUT", "/" + TEST_INDEX);
        }

    }

//    @AfterEach
//    void afterEach() {
//        IOUtil.closeQuietly(restClient);
//    }


    @AfterAll
    static void afterAll() {
        IOUtil.closeQuietly(restClient);
//        if (elastic != null) {
//            elastic.stop();
//        }
    }

    @Test
    void testCommitAdd() throws Exception {
        withinCommitterSession(c -> {
            c.upsert(upsertRequest(TEST_ID, TEST_CONTENT));
        });

        JSONObject doc = getDocument(TEST_ID);
        assertTrue(isFound(doc), "Not found.");
        assertTrue(hasTestContent(doc), "Bad content.");
    }

    @Test
    void testCommitDelete() throws Exception {
        // Add a document directly to ES
        Request request = new Request(
                "PUT", INDEX_ENDPOINT + "_doc/" + TEST_ID);
        request.setJsonEntity(
                "{\"" + CONTENT_FIELD + "\":\"" + TEST_CONTENT + "\"}");
        restClient.performRequest(request);

        assertTrue(isFound(getDocument(TEST_ID)), "Not properly added.");

        // Queue it to be deleted
        withinCommitterSession(c -> {
            c.delete(new DeleteRequest(TEST_ID, new Properties()));
        });

        // Check that it's removed from ES
        assertFalse(isFound(getDocument(TEST_ID)), "Was not deleted.");
    }

    @Test
    void testRemoveQueuedFilesAfterAdd() throws Exception {
        // Add new doc to ES
        ElasticsearchCommitter esc = withinCommitterSession(c -> {
            c.upsert(upsertRequest(TEST_ID, null));
        });

        // After commit, make sure queue is emptied of all files
        assertTrue(listFiles(esc).isEmpty());
    }

    @Test
    void testRemoveQueuedFilesAfterDelete() throws Exception {
        // Delete doc from ES
        ElasticsearchCommitter esc = withinCommitterSession(c -> {
            c.delete(new DeleteRequest(TEST_ID, new Properties()));
        });

        // After commit, make sure queue is emptied of all files
        assertTrue(listFiles(esc).isEmpty());
    }

    @Test
    void testIdSourceFieldRemoval() throws Exception {
        // Force to use a reference field instead of the default
        // reference ID.
        String sourceIdField = "customId";
        Properties metadata = new Properties();
        String customIdValue = "ABC";
        metadata.set(sourceIdField, customIdValue);

        // Add new doc to ES with a difference id than the one we
        // assigned in source reference field. Set to keep that
        // field.
        withinCommitterSession(c -> {
            c.setSourceIdField(sourceIdField);
            c.upsert(upsertRequest(TEST_ID, TEST_CONTENT, metadata));
        });

        // Check that it's in ES using the custom ID
        JSONObject doc = getDocument(customIdValue);
        assertTrue(isFound(doc), "Not found.");
        assertTrue(hasTestContent(doc), "Bad content.");
        assertFalse(
                hasField(doc, sourceIdField), "sourceIdField was not removed.");
    }

    @Test
    void testCustomTargetContentField() throws Exception {
        String targetContentField = "customContent";
        Properties metadata = new Properties();
        metadata.set(targetContentField, TEST_CONTENT);

        // Add new doc to ES
        withinCommitterSession(c -> {
            c.setTargetContentField(targetContentField);
            c.upsert(upsertRequest(TEST_ID, TEST_CONTENT, metadata));
        });

        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID);
        assertTrue(isFound(doc), "Not found.");

        // Check content is available in custom content target field and
        // not in the default field
        assertEquals(TEST_CONTENT, getFieldValue(doc, targetContentField),
                "targetContentField was not saved.");
        assertFalse(hasField(doc, CONTENT_FIELD),
                "Default content field was saved.");
    }

    @Test
	void testMultiValueFields() throws Exception {
    	Properties metadata = new Properties();
        String fieldname = "multi";
		metadata.set(fieldname, "1", "2", "3");

        withinCommitterSession(c -> {
            c.upsert(upsertRequest(TEST_ID, null, metadata));
        });

        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID);
        assertTrue(isFound(doc), "Not found.");

        // Check multi values are still there
        assertEquals(3, getFieldValues(doc, fieldname).size(),
                "Multi-value not saved properly.");
	}

    @Test
    void testDotReplacement() throws Exception {
        Properties metadata = new Properties();
        String fieldNameDots = "with.dots.";
        String fieldNameNoDots = "with_dots_";
        String fieldValue = "some value";
        metadata.set(fieldNameDots, fieldValue);

        withinCommitterSession(c -> {
            c.setDotReplacement("_");
            c.upsert(upsertRequest(TEST_ID, null, metadata));
        });

        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID);

        assertTrue(isFound(doc), "Not found.");

        // Check the dots were replaced
        assertEquals(fieldValue, getFieldValue(doc, fieldNameNoDots),
                "Dots not replaced.");
        assertFalse(hasField(doc, fieldNameDots), "Dots still present.");
    }

    @Test
    void testErrorsFiltering() throws Exception {
        // Should only get errors returned.
        Properties metadata;

        // Commit first one to set the date format
        metadata = new Properties();
        metadata.set("date", "2014-01-01");
        withinCommitterSession(c -> {
            c.upsert(upsertRequest("good1", null, metadata));
        });

        // Commit a mixed batch with one wrong date format
        try {
            withinCommitterSession(c -> {
                Properties m;

                m = new Properties();
                m.set("date", "2014-01-02");
                c.upsert(upsertRequest("good2", null, m));

                m = new Properties();
                m.set("date", "5/30/2011");
                c.upsert(upsertRequest("bad1", null, m));

                m = new Properties();
                m.set("date", "2014-01-03");
                c.upsert(upsertRequest("good3", null, m));

                m = new Properties();
                m.set("date", "5/30/2012");
                c.upsert(upsertRequest("bad2", null, m));

                m = new Properties();
                m.set("date", "2014-01-04");
                c.upsert(upsertRequest("good4", null, m));
            });
            Assertions.fail("Failed to throw exception.");
        } catch (CommitterException e) {
            assertEquals(2, StringUtils.countMatches(
                    e.getMessage(), "\"error\":"), "Wrong error count.");
        }
    }

    private boolean hasTestContent(JSONObject doc) {
        return TEST_CONTENT.equals(getContentFieldValue(doc));
    }
    private boolean hasField(JSONObject doc, String fieldName) {
        return doc.getJSONObject("_source").has(fieldName);
    }
    private String getFieldValue(JSONObject doc, String fieldName) {
        return doc.getJSONObject("_source").getString(fieldName);
    }
    private List<String> getFieldValues(JSONObject doc, String fieldName) {
        List<String> values = new ArrayList<>();
        JSONArray array = doc.getJSONObject("_source").getJSONArray(fieldName);
        for (int i = 0; i < array.length(); i++) {
            values.add(array.getString(i));
        }
        return values;
    }
    private String getContentFieldValue(JSONObject doc) {
        return getFieldValue(doc, CONTENT_FIELD);
    }
    private boolean isFound(JSONObject doc) {
        return doc.getBoolean("found");
    }
    private JSONObject getDocument(String id) throws IOException {
        return performTypeRequest("GET", "_doc/" + id);
    }
    private JSONObject performTypeRequest(String method, String request)
            throws IOException {
        return performRequest(method, INDEX_ENDPOINT + request);
    }
    private JSONObject performRequest(String method, String endpoint)
            throws IOException {
        Response httpResponse;
        try {
            Request request = new Request(method, endpoint);
            httpResponse = restClient.performRequest(request);
        } catch (ResponseException e) {
            httpResponse = e.getResponse();
        }
        String response = IOUtils.toString(
                httpResponse.getEntity().getContent(), StandardCharsets.UTF_8);
        JSONObject json = new JSONObject(response);
        LOG.info("Response status: {}", httpResponse.getStatusLine());
        LOG.debug("Response body: {}", json);
        return json;
    }

    private UpsertRequest upsertRequest(String id, String content) {
        return upsertRequest(id, content, null);
    }
    private UpsertRequest upsertRequest(
            String id, String content, Properties metadata) {
        Properties p = metadata == null ? new Properties() : metadata;
        return new UpsertRequest(id, p, content == null
                ? new NullInputStream(0) : toInputStream(content, UTF_8));
    }

    private List<File> listFiles(ElasticsearchCommitter c) {
        return new ArrayList<>(FileUtils.listFiles(
                c.getCommitterContext().getWorkDir().toFile(), null, true));
    }

    protected ElasticsearchCommitter createESCommitter()
            throws CommitterException {

        CommitterContext ctx = CommitterContext.build()
                .setWorkDir(new File(tempDir,
                        "" + TimeIdGenerator.next()).toPath())
                .create();
        ElasticsearchCommitter committer = new ElasticsearchCommitter();
        committer.setNodes(container.getHttpHostAddress());
        committer.setIndexName(TEST_INDEX);
        committer.init(ctx);
        return committer;
    }


    protected ElasticsearchCommitter withinCommitterSession(CommitterConsumer c)
            throws CommitterException {
        ElasticsearchCommitter committer = createESCommitter();
        try {
            c.accept(committer);
        } catch (CommitterException e) {
            throw e;
        } catch (Exception e) {
            throw new CommitterException(e);
        }
        committer.close();
        return committer;
    }

    @FunctionalInterface
    protected interface CommitterConsumer {
        void accept(ElasticsearchCommitter c) throws Exception;
    }

}
