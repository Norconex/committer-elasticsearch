/* Copyright 2017 Norconex Inc.
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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.ClassUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.encrypt.EncryptionKey;
import com.norconex.commons.lang.encrypt.EncryptionKey.Source;
import com.norconex.commons.lang.log.CountingConsoleAppender;

public class ElasticsearchCommitterConfigTest {

    @Test
    public void testWriteRead() throws Exception {
        ElasticsearchCommitter committer = new ElasticsearchCommitter();
        committer.setQueueDir("my-queue-dir");
        committer.setSourceContentField("sourceContentField");
        committer.setTargetContentField("targetContentField");
        committer.setSourceReferenceField("idField");
        committer.setKeepSourceContentField(true);
        committer.setKeepSourceReferenceField(false);
        committer.setQueueSize(10);
        committer.setCommitBatchSize(1);
        committer.setIndexName("my-inxed");
        committer.setTypeName("my-type");
        committer.setNodes("http://localhost:9200", "http://somewhere.com");
        committer.setDiscoverNodes(true);
        committer.setDotReplacement("_");
        committer.setIgnoreResponseErrors(true);
        committer.setUsername("username");
        committer.setPassword("password");
        committer.setPasswordKey(
                new EncryptionKey("encValue", Source.ENVIRONMENT));
        committer.setJsonFieldsPattern("jsonFieldPattern");
        committer.setConnectionTimeout(200);
        committer.setSocketTimeout(300);
        
        System.out.println("Writing/Reading this: " + committer);
        XMLConfigurationUtil.assertWriteRead(committer);
    }

    @Test
    public void testUnsupportedIdTargetField() throws Exception {
        ElasticsearchCommitter committer = new ElasticsearchCommitter();
        String xml = "<committer><targetReferenceField>"
                + "newid</targetReferenceField></committer>";
        XMLConfiguration config = XMLConfigurationUtil.newXMLConfiguration(
                new StringReader(xml));
        try {
            committer.loadFromXml(config);
            fail("Expected exception because idTargetField is not supported");
        } catch (Exception e) {
            // Expected
        }
    }
    
    @Test
    public void testValidation() throws IOException {
        CountingConsoleAppender appender = new CountingConsoleAppender();
        appender.startCountingFor(XMLConfigurationUtil.class, Level.WARN);
        try (Reader r = new InputStreamReader(getClass().getResourceAsStream(
                ClassUtils.getShortClassName(getClass()) + ".xml"))) {
            XMLConfigurationUtil.newInstance(r);
        } finally {
            appender.stopCountingFor(XMLConfigurationUtil.class);
        }
        Assert.assertEquals("Validation warnings/errors were found.", 
                0, appender.getCount());
    }
}
