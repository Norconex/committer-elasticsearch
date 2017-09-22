/* Copyright 2013-2017 Norconex Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.committer.core.CommitterException;
import com.norconex.commons.lang.map.Properties;

import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

public class ElasticsearchCommitterTest {

    private static final Logger LOG = 
            LogManager.getLogger(ElasticsearchCommitterTest.class);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String TEST_ES_VERSION = "5.4.0";
    private static final String TEST_INDEX = "tests";
    private static final String TEST_TYPE = "test";
    private static final String TEST_ID = "1";
    private static final String TEST_CONTENT = "This is test content.";
    private static final String INDEX_ENDPOINT = "/" + TEST_INDEX + "/";
    private static final String TYPE_ENDPOINT = 
            INDEX_ENDPOINT + TEST_TYPE + "/";
    private static final String CONTENT_FIELD = 
            ElasticsearchCommitter.DEFAULT_ES_CONTENT_FIELD;
    
    private static EmbeddedElastic elastic;
    private static RestClient restClient;
    private static String node;

    private ElasticsearchCommitter committer;
    private File queue;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        elastic = EmbeddedElastic.builder()
                .withElasticVersion(TEST_ES_VERSION)
                .withSetting(PopularProperties.CLUSTER_NAME, "test_cluster")
                .build()
                .start();

        node = "http://localhost:" + elastic.getHttpPort();

        restClient = RestClient.builder(HttpHost.create(node)).build();
    }
    
    @Before
    public void setup() throws Exception {
        queue = tempFolder.newFolder("queue");
        committer = new ElasticsearchCommitter();
        committer.setQueueDir(queue.toString());
        committer.setNodes(node);
        committer.setIndexName(TEST_INDEX);
        committer.setTypeName(TEST_TYPE);
    }
    
    @After
    public void tearDown() throws IOException {
        LOG.debug("Deleting test index");
        //performIndexRequest("POST", "_flush");
        performRequest("DELETE", "/" + TEST_INDEX);
        performRequest("POST", "_flush");
        committer.close();
    }

    @AfterClass
    public static void afterClass() {
        IOUtils.closeQuietly(restClient);
        if (elastic != null) {
            elastic.stop();
        }
    }
    
    @Test
    public void testCommitAdd() throws Exception {
        // Add new doc to ES
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, new Properties());
            committer.commit();
        }
        JSONObject doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
    }

    @Test
    public void testCommitDelete() throws Exception {
        // Add a document directly to ES
        StringEntity requestEntity = new StringEntity(
                "{\"" + CONTENT_FIELD + "\":\"" + TEST_CONTENT + "\"}\n",
                ContentType.APPLICATION_JSON);
        restClient.performRequest("PUT", TYPE_ENDPOINT + TEST_ID, 
                Collections.emptyMap(), requestEntity);
        assertTrue("Not properly added.", isFound(getDocument(TEST_ID)));
        
        // Queue it to be deleted
        committer.remove(TEST_ID, new Properties());
        committer.commit();

        // Check that it's removed from ES
        assertFalse("Was not deleted.", isFound(getDocument(TEST_ID)));
    }
    
    @Test
    public void testRemoveQueuedFilesAfterAdd() throws Exception {
        // Add new doc to ES
        committer.add(TEST_ID, new NullInputStream(0), new Properties());
        committer.commit();

        // After commit, make sure queue is emptied of all files
        assertTrue(FileUtils.listFiles(queue, null, true).isEmpty());
    }

    @Test
    public void testRemoveQueuedFilesAfterDelete() throws Exception {
        // Add new doc to ES
        committer.remove(TEST_ID, new Properties());
        committer.commit();

        // After commit, make sure queue is emptied of all files
        assertTrue(FileUtils.listFiles(queue, null, true).isEmpty());
    }

    @Test
    public void testSetSourceReferenceField() throws Exception {
        // Force to use a reference field instead of the default
        // reference ID.
        String sourceReferenceField = "customId";
        committer.setSourceReferenceField(sourceReferenceField);
        Properties metadata = new Properties();
        String customIdValue = "ABC";
        metadata.setString(sourceReferenceField, customIdValue);

        // Add new doc to ES with a difference id than the one we
        // assigned in source reference field
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, metadata);
            committer.commit();
        }

        // Check that it's in ES using the custom ID
        JSONObject doc = getDocument(customIdValue); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        assertFalse("sourceReferenceField was saved.", 
                doc.getJSONObject("_source").has(sourceReferenceField));
    }
    
    @Test
    public void testKeepIdSourceField() throws Exception {
        // Force to use a reference field instead of the default
        // reference ID.
        String sourceReferenceField = "customId";
        committer.setSourceReferenceField(sourceReferenceField);
        Properties metadata = new Properties();
        String customIdValue = "ABC";
        metadata.setString(sourceReferenceField, customIdValue);

        // Add new doc to ES with a difference id than the one we
        // assigned in source reference field. Set to keep that 
        // field.
        committer.setKeepSourceReferenceField(true);
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, metadata);
            committer.commit();
        }

        // Check that it's in ES using the custom ID
        JSONObject doc = getDocument(customIdValue); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        assertTrue("sourceReferenceField was not saved.", 
                doc.getJSONObject("_source").has(sourceReferenceField));
    }
    
    @Test
    public void testCustomsourceContentField() throws Exception {
        
        // Set content from metadata
        String sourceContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(sourceContentField, TEST_CONTENT);
        
        // Add new doc to ES. Set a null input stream, because content
        // will be taken from metadata. 
        committer.setSourceContentField(sourceContentField);
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        
        // Check custom source field is removed (default behavior)
        assertFalse("sourceContentField was saved.", 
                doc.getJSONObject("_source").has(sourceContentField));
    }
    
    @Test
    public void testKeepCustomsourceContentField() throws Exception {
        // Set content from metadata
        String sourceContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(sourceContentField, TEST_CONTENT);
        
        // Add new doc to ES. Set a null input stream, because content
        // will be taken from metadata. Set to keep the source metadata
        // field.
        committer.setSourceContentField(sourceContentField);
        committer.setKeepSourceContentField(true);
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        
        // Check custom source field is kept
        assertTrue("sourceContentField was not saved.", 
                doc.getJSONObject("_source").has(sourceContentField));
    }
    
    @Test
    public void testCustomtargetContentField() throws Exception {
        String targetContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(targetContentField, TEST_CONTENT);
        
        // Add new doc to ES
        committer.setTargetContentField(targetContentField);
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, metadata);
            committer.commit();
        }
        
        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        
        // Check content is available in custom content target field and
        // not in the default field
        assertEquals("targetContentField was not saved.", TEST_CONTENT, 
                doc.getJSONObject("_source").getString(targetContentField));
        assertFalse("Default content field was saved.", 
                doc.getJSONObject("_source").has(CONTENT_FIELD));
    }
    
    @Test
	public void testMultiValueFields() throws Exception {
    	Properties metadata = new Properties();
        String fieldname = "multi";
		metadata.setString(fieldname, "1", "2", "3");
        
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        
        // Check multi values are still there
        assertEquals("Multi-value not saved properly.", 3, doc.getJSONObject(
                "_source").getJSONArray(fieldname).length());
	}

    @Test
    public void testDotReplacement() throws Exception {
        Properties metadata = new Properties();
        String fieldNameDots = "with.dots.";
        String fieldNameNoDots = "with_dots_";
        String fieldValue = "some value";
        metadata.setString(fieldNameDots, fieldValue);

        committer.setDotReplacement("_");
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in ES
        JSONObject doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        
        // Check the dots were replaced
        assertEquals("Dots not replaced.", fieldValue, doc.getJSONObject(
                "_source").getString(fieldNameNoDots));
        assertFalse("Dots still present.", doc.getJSONObject(
                "_source").has(fieldNameDots));
    }

    @Test
    public void testErrorsFiltering() throws Exception {
        // Should only get errors returned.
        Properties metadata;

        // Commit first one to set the date format
        metadata = new Properties();
        metadata.setString("date", "2014-01-01");
        committer.add("good1", new NullInputStream(0), metadata);
        committer.commit();

        // Commit a mixed batch with one wrong date format
        metadata = new Properties();
        metadata.setString("date", "2014-01-02");
        committer.add("good2", new NullInputStream(0), metadata);

        metadata = new Properties();
        metadata.setString("date", "5/30/2011");
        committer.add("bad1", new NullInputStream(0), metadata);

        metadata = new Properties();
        metadata.setString("date", "2014-01-03");
        committer.add("good3", new NullInputStream(0), metadata);
        
        metadata = new Properties();
        metadata.setString("date", "5/30/2012");
        committer.add("bad2", new NullInputStream(0), metadata);

        metadata = new Properties();
        metadata.setString("date", "2014-01-04");
        committer.add("good4", new NullInputStream(0), metadata);

        try {
            committer.commit();
            fail("Failed to throw exception.");
        } catch (CommitterException e) {
            assertEquals("Wrong error count.", 2, StringUtils.countMatches(
                    e.getMessage(), "\"error\":"));
        }
    }

    
    private boolean hasTestContent(JSONObject json) throws IOException {
        return TEST_CONTENT.equals(getContent(json));
    }
    private String getContent(JSONObject json) throws IOException {
        return json.getJSONObject("_source").getString(CONTENT_FIELD);
    }
    private boolean isFound(JSONObject json) throws IOException {
        return json.getBoolean("found");
    }
    private JSONObject getDocument(String id) throws IOException {
        return performTypeRequest("GET", id);
    }
    private JSONObject performTypeRequest(String method, String request)
            throws IOException {
        return performRequest(method, TYPE_ENDPOINT + URLEncoder.encode(
                request, StandardCharsets.UTF_8.toString()));
    }
    private JSONObject performRequest(String method, String endpoint)
            throws IOException {
        Response httpResponse;
        try {
            httpResponse = restClient.performRequest(method, endpoint);
        } catch (ResponseException e) {
            httpResponse = e.getResponse();
        }
        String response = IOUtils.toString(
                httpResponse.getEntity().getContent(), StandardCharsets.UTF_8);
        LOG.debug("RESPONSE: " + response);
        return new JSONObject(response);
    }
    private InputStream getContentStream() throws IOException {
        return IOUtils.toInputStream(TEST_CONTENT, StandardCharsets.UTF_8);
    }
}
