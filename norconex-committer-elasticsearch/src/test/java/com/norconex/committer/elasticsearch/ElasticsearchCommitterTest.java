/* Copyright 2013-2014 Norconex Inc.
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

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.CharEncoding;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.commons.lang.config.ConfigurationUtil;
import com.norconex.commons.lang.map.Properties;


public class ElasticsearchCommitterTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ElasticsearchCommitter committer;

    private Client client;
    
    private Node node;

    private String indexName = "crawl";

    private String typeName = "page";

    private File queue;

    @Before
    public void setup() throws Exception {

        Settings settings = Settings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put("path.home", tempFolder.newFolder().toString())
                .build();
        
        // Create a local client
        node = nodeBuilder().local(true).settings(settings).node();
        client = node.client();

        committer = new ElasticsearchCommitter(new IClientFactory() {
            @Override
            public Client createClient(ElasticsearchCommitter committer) {
                return client;
            }
        });

        committer.setIndexName(indexName);
        committer.setTypeName(typeName);
        committer.setTargetContentField(
                ElasticsearchCommitter.DEFAULT_ES_CONTENT_FIELD);

        queue = tempFolder.newFolder("queue");
        committer.setQueueDir(queue.toString());
    }
    
    @After
    public void tearDown() throws Exception {
        node.close();
    }

    @Test
    public void testCommitAdd() throws Exception {
        String content = "hello world!";
        InputStream is = IOUtils.toInputStream(content, CharEncoding.UTF_8);

        // Add new doc to ES
        String id = "1";
        committer.add(id,  is, new Properties());
        committer.commit();

        IOUtils.closeQuietly(is);
        
        // Check that it's in ES
        GetResponse response = client.prepareGet(indexName, typeName, id)
                .execute().actionGet();
        assertTrue(response.isExists());
        // Check content
        
        Map<String, Object> responseMap = response.getSource();
        assertEquals(content, ((List<?>) responseMap.get(
                ElasticsearchCommitter.DEFAULT_ES_CONTENT_FIELD)).get(0));
    }

    @Test
    public void testCommitDelete() throws Exception {

        // Add a document directly to ES
        IndexRequestBuilder request = client.prepareIndex(indexName, typeName);
        String id = "1";
        request.setId(id);
        request.setSource("content", "hello world!");
        request.execute();

        // Queue it to be deleted
        committer.remove(id, new Properties());
        committer.commit();

        // Check that it's removed from ES
        GetResponse response = client.prepareGet(indexName, typeName, id)
                .execute().actionGet();
        assertFalse(response.isExists());
    }

    @Test
    public void testRemoveQueuedFilesAfterAdd() throws Exception {

        // Add new doc to ES
        String id = "1";
        committer.add(id, new NullInputStream(0), new Properties());
        committer.commit();

        // After commit, make sure queue is emptied of all files
        assertTrue(FileUtils.listFiles(queue, null, true).isEmpty());
    }

    @Test
    public void testRemoveQueuedFilesAfterDelete() throws Exception {

        // Add new doc to ES
        String id = "1";
        committer.remove(id, new Properties());
        committer.commit();

        // After commit, make sure queue is emptied of all files
        assertTrue(FileUtils.listFiles(queue, null, true).isEmpty());
    }

    @Test
    public void testUnsupportedIdTargetField() throws Exception {

        String xml = 
                "<committer><targetReferenceField>newid</targetReferenceField></committer>";
        XMLConfiguration config = ConfigurationUtil.newXMLConfiguration(
                new StringReader(xml));
        try {
            committer.loadFromXml(config);
            fail("Expected exception because idTargetField is not supported");
        } catch (Exception e) {
            // Expected
        }
    }

    @Test
    public void testWriteRead() throws Exception {
        committer.setQueueDir("my-queue-dir");
        committer.setSourceContentField("sourceContentField");
        committer.setTargetContentField("targetContentField");
        committer.setSourceReferenceField("idField");
        committer.setKeepSourceContentField(true);
        committer.setKeepSourceReferenceField(false);
        committer.setQueueSize(10);
        committer.setCommitBatchSize(1);
        committer.setClusterName("my-cluster");
        committer.setIndexName("my-inxed");
        committer.setTypeName("my-type");

        ConfigurationUtil.assertWriteRead(committer);
    }
    
    @Test
    public void testSetSourceReferenceField() throws Exception {

        String content = "hello world!";
        InputStream is = IOUtils.toInputStream(content, CharEncoding.UTF_8);

        // Force to use a reference field instead of the default
        // reference ID.
        String sourceReferenceField = "customId";
        committer.setSourceReferenceField(sourceReferenceField);
        Properties metadata = new Properties();
        String customIdValue = "ABC";
        metadata.setString(sourceReferenceField, customIdValue);

        // Add new doc to ES with a difference id than the one we
        // assigned in source reference field
        committer.add("1",  is, metadata);
        committer.commit();

        IOUtils.closeQuietly(is);
        
        // Check that it's in ES using the custom ID
        GetResponse response = client.prepareGet(indexName, typeName, customIdValue)
                .execute().actionGet();
        assertTrue(response.isExists());
        
        // Check content
        Map<String, Object> responseMap = response.getSource();
        assertEquals(content, ((List<?>) responseMap.get(
                ElasticsearchCommitter.DEFAULT_ES_CONTENT_FIELD)).get(0));
        
        // Check custom id field is removed (default behavior)
        assertFalse(response.getSource().containsKey(
                sourceReferenceField));
    }
    
    @Test
    public void testKeepIdSourceField() throws Exception {

        String content = "hello world!";
        InputStream is = IOUtils.toInputStream(content, CharEncoding.UTF_8);

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
        committer.add("1",  is, metadata);
        committer.commit();

        IOUtils.closeQuietly(is);
        
        // Check that it's in ES using the custom ID
        GetResponse response = client.prepareGet(indexName, typeName, customIdValue)
                .execute().actionGet();
        assertTrue(response.isExists());
        
        // Check custom id field is NOT removed
        assertTrue(response.getSource().containsKey(
                sourceReferenceField));
    }
    
    @Test
    public void testCustomsourceContentField() throws Exception {
        
        // Set content from metadata
        String content = "hello world!";
        String sourceContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(sourceContentField, content);
        
        // Add new doc to ES. Set a null input stream, because content
        // will be taken from metadata. 
        String id = "1";
        committer.setSourceContentField(sourceContentField);
        committer.add(id,  new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in ES
        GetResponse response = client.prepareGet(indexName, typeName, id)
                .execute().actionGet();
        assertTrue(response.isExists());
        
        // Check content
        Map<String, Object> responseMap = response.getSource();
        assertEquals(content, ((List<?>) responseMap.get(
                ElasticsearchCommitter.DEFAULT_ES_CONTENT_FIELD)).get(0));
        
        // Check custom source field is removed (default behavior)
        assertFalse(response.getSource().containsKey(
                sourceContentField));
    }
    
    @Test
    public void testKeepCustomsourceContentField() throws Exception {
        
        // Set content from metadata
        String content = "hello world!";
        String sourceContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(sourceContentField, content);
        
        // Add new doc to ES. Set a null input stream, because content
        // will be taken from metadata. Set to keep the source metadata
        // field.
        String id = "1";
        committer.setSourceContentField(sourceContentField);
        committer.setKeepSourceContentField(true);
        committer.add(id,  new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in ES
        GetResponse response = client.prepareGet(indexName, typeName, id)
                .execute().actionGet();
        assertTrue(response.isExists());
        
        // Check custom source field is kept
        assertTrue(response.getSource().containsKey(
                sourceContentField));
    }
    
    @Test
    public void testCustomtargetContentField() throws Exception {

        String content = "hello world!";
        InputStream is = IOUtils.toInputStream(content, CharEncoding.UTF_8);
        
        String targetContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(targetContentField, content);
        
        // Add new doc to ES
        String id = "1";
        committer.setTargetContentField(targetContentField);
        committer.add(id, is, metadata);
        committer.commit();
        
        IOUtils.closeQuietly(is);
        
        // Check that it's in ES
        GetResponse response = client.prepareGet(indexName, typeName, id)
                .execute().actionGet();
        assertTrue(response.isExists());
        
        // Check content is available in custom content target field and
        // not in the default field
        Map<String, Object> responseMap = response.getSource();
        assertEquals(content, 
        		((List<?>) responseMap.get(targetContentField)).get(0));
        assertNull(responseMap.get(
                ElasticsearchCommitter.DEFAULT_ES_CONTENT_FIELD));
    }
    
    @Test
	public void testMultiValueFields() throws Exception {
		
    	Properties metadata = new Properties();
        String fieldname = "multi";
		metadata.setString(fieldname, "1", "2", "3");
        
        String id = "1";
        committer.add(id, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in ES
        GetResponse response = client.prepareGet(indexName, typeName, id)
                .execute().actionGet();
        assertTrue(response.isExists());
        
        // Check multi values are still there
        Map<String, Object> source = response.getSource();
        assertEquals(((List<?>) source.get(fieldname)).size(), 3);
	}

}
