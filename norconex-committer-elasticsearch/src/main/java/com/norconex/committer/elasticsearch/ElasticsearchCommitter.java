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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClient.FailureListener;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.ElasticsearchHostsSniffer;
import org.elasticsearch.client.sniff.HostsSniffer;
import org.elasticsearch.client.sniff.Sniffer;
import org.json.JSONArray;
import org.json.JSONObject;

import com.norconex.committer.core.AbstractCommitter;
import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.encrypt.EncryptionKey;
import com.norconex.commons.lang.encrypt.EncryptionUtil;
import com.norconex.commons.lang.time.DurationParser;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;

/**
 * <p>
 * Commits documents to Elasticsearch. Since version 4.0.0, this committer
 * relies on Elasticsearch REST API. If you wish to use the 
 * Elasticsearch Transport Client, use an older version (the Transport Client
 * will eventually be deprecated by Elastic).
 * </p>  
 * <p>
 * Despite being a subclass
 * of {@link AbstractMappedCommitter}, setting an <code>idTargetField</code>
 * is not supported (it is always "id").
 * </p>
 * <h3>Dots (.) in field names</h3>
 * <p>
 * Based on your Elasticsearch version, having dots in field names can have 
 * different consequences.  Some versions will not accept them and generate
 * errors, while Elasticsearch 5 and up supports them, but they are treated 
 * as objects, which may not always be what you want.
 * </p>
 * <p>
 * If your version of Elasticsearch does not handle dots the way you expect,
 * make sure you do not submit fields with dots. A good strategy
 * is to convert dots to another character (like underscore). 
 * This can be accomplished by setting a <code>dotReplacement</code>.
 * </p>
 * <p>
 * In addition, if you are using a Norconex Collector with the 
 * Norconex Importer, you can rename the problematic fields with
 * <a href="https://www.norconex.com/collectors/importer/latest/apidocs/com/norconex/importer/handler/tagger/impl/RenameTagger.html">
 * RenameTagger</a>. 
 * You can also make sure only the fields you are interested in are making
 * their way to Elasticsearch by using  
 * <a href="https://www.norconex.com/collectors/importer/latest/apidocs/com/norconex/importer/handler/tagger/impl/KeepOnlyTagger.html">
 * KeepOnlyTagger</a>.  If your dot represents a nested object, keep reading.
 * </p>
 * 
 * <h3>JSON Objects</h3>
 * <p>
 * <b>Since 4.1.0</b>, it is possible to provide a regular expression 
 * that will identify one or more fields containing a JSON object rather
 * than a regular string ({@link #setJsonFieldsPattern(String)}). For example,
 * this is a useful way to store nested objects.  While very flexible,
 * it can be challenging to come up with the JSON structure.  You may 
 * want to consider custom code to do so, or if you are using Norconex
 * Importer, one approach could be to use the 
 * <a href="https://www.norconex.com/collectors/importer/latest/apidocs/com/norconex/importer/handler/tagger/impl/ScriptTagger.html">
 * ScriptTagger</a>.
 * For this to work properly, make sure you define your Elasticsearch
 * field mappings on your index/type beforehand.
 * </p>
 * 
 * <h3>Authentication</h3>
 * <p>
 * Basic authentication is supported for password-protected clusters. 
 * The <code>password</code> can optionally be 
 * encrypted using {@link EncryptionUtil} (or command-line "encrypt.bat"
 * or "encrypt.sh").
 * In order for the password to be decrypted properly, you need
 * to specify the encryption key used to encrypt it. The key can be stored
 * in a few supported locations and a combination of
 * <code>passwordKey</code>
 * and <code>passwordKeySource</code> must be specified to properly
 * locate the key. The supported sources are:
 * </p>
 * <table border="1" summary="">
 *   <tr>
 *     <th><code>passwordKeySource</code></th>
 *     <th><code>passwordKey</code></th>
 *   </tr>
 *   <tr>
 *     <td><code>key</code></td>
 *     <td>The actual encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>file</code></td>
 *     <td>Path to a file containing the encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>environment</code></td>
 *     <td>Name of an environment variable containing the key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>property</code></td>
 *     <td>Name of a JVM system property containing the key.</td>
 *   </tr>
 * </table>
 * 
 * <h3>Timeouts</h3>
 * <p>
 * <b>Since 4.1.0</b>, it is possible to specify timeout values (in 
 * milliseconds), applied when data is sent to Elasticsearch.
 * </p>
 * 
 * <h3>XML configuration usage:</h3>
 * <pre>
 *  &lt;committer class="com.norconex.committer.elasticsearch.ElasticsearchCommitter"&gt;
 *      &lt;nodes&gt;
 *         (Comma-separated list of Elasticsearch node URLs. 
 *          Defaults to http://localhost:9200)
 *      &lt;/nodes&gt;
 *      &lt;indexName&gt;(Name of the index to use)&lt;/indexName&gt;
 *      &lt;typeName&gt;(Name of the type to use)&lt;/typeName&gt;
 *      &lt;ignoreResponseErrors&gt;[false|true]&lt;/ignoreResponseErrors&gt;
 *      &lt;discoverNodes&gt;[false|true]&lt;/discoverNodes&gt;
 *      &lt;dotReplacement&gt;
 *         (Optional value replacing dots in field names)
 *      &lt;/dotReplacement&gt;
 *      &lt;jsonFieldsPattern&gt;
 *         (Optional regular expression to identify fields containing JSON
 *          objects instead of regular strings)
 *      &lt;/jsonFieldsPattern&gt;
 *      &lt;connectionTimeout&gt;(milliseconds)&lt;/connectionTimeout&gt;
 *      &lt;socketTimeout&gt;(milliseconds)&lt;/socketTimeout&gt;
 *      &lt;maxRetryTimeout&gt;(milliseconds)&lt;/maxRetryTimeout&gt;
 *  
 *      &lt;!-- Use the following if authentication is required. --&gt;
 *      &lt;username&gt;(Optional user name)&lt;/username&gt;
 *      &lt;password&gt;(Optional user password)&lt;/password&gt;
 *      &lt;!-- Use the following if password is encrypted. --&gt;
 *      &lt;passwordKey&gt;(the encryption key or a reference to it)&lt;/passwordKey&gt;
 *      &lt;passwordKeySource&gt;[key|file|environment|property]&lt;/passwordKeySource&gt;
 *
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when 
 *         the default document reference is not used.  The reference value
 *         will be mapped to the Elasticsearch ID field. 
 *         Once re-mapped, this metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (Target repository field name for a document content/body.
 *          Default is "content".)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of documents to send to Elasticsearch at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay in milliseconds between retries)&lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * <p>
 * XML configuration entries expecting millisecond durations
 * can be provided in human-readable format (English only), as per 
 * {@link DurationParser} (e.g., "5 minutes and 30 seconds" or "5m30s").
 * </p>
 * 
 * <h4>Usage example:</h4>
 * <p>
 * The following example uses the minimum required settings, on the local host.  
 * </p> 
 * <pre>
 *  &lt;committer class="com.norconex.committer.elasticsearch.ElasticsearchCommitter"&gt;
 *      &lt;indexName&gt;some_index&lt;/indexName&gt;
 *      &lt;typeName&gt;some_type&lt;/typeName&gt;
 *  &lt;/committer&gt;
 * </pre>
 *  
 * @author Pascal Essiembre
 */
public class ElasticsearchCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = 
            LogManager.getLogger(ElasticsearchCommitter.class);

    public static final String DEFAULT_NODE = "http://localhost:9200"; 
    public static final String DEFAULT_ES_CONTENT_FIELD = "content";
    /** @since 4.1.0 */
    public static final int DEFAULT_CONNECTION_TIMEOUT = 1000;
    /** @since 4.1.0 */
    public static final int DEFAULT_SOCKET_TIMEOUT = 30000;
    /** @since 4.1.0 */
    public static final int DEFAULT_MAX_RETRY_TIMEOUT = 30000;

    private RestClient client;
    private Sniffer sniffer;
    private String[] nodes = new String[] {DEFAULT_NODE};
    private String indexName;
    private String typeName;
    private boolean ignoreResponseErrors;
    private boolean discoverNodes;
    private String username;
    private String password;
    private EncryptionKey passwordKey;
    private String dotReplacement;
    private String jsonFieldsPattern;
    private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;
    private int maxRetryTimeout = DEFAULT_MAX_RETRY_TIMEOUT;

    /**
     * Constructor.
     */
    public ElasticsearchCommitter() {
        super();
        setTargetContentField(DEFAULT_ES_CONTENT_FIELD);
    }
    /**
     * Gets Elasticsearch cluster node URLs. 
     * Defaults to "http://localhost:9200".
     * @return Elasticsearch nodes
     */
    public String[] getNodes() {
        return nodes;
    }
    /**
     * Sets cluster node URLs.
     * Node URLs with no port are assumed to be using port 80.
     * @param nodes Elasticsearch cluster nodes
     */
    public void setNodes(String... nodes) {
        this.nodes = nodes;
    }
    
	/**
     * Gets the index name.
     * @return index name
     */
    public String getIndexName() {
        return indexName;
    }
    /**
     * Sets the index name.
     * @param indexName the index name
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Gets the type name.
     * @return type name
     */
    public String getTypeName() {
        return typeName;
    }
    /**
     * Sets the type name.
     * @param typeName type name
     */
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets the regular expression matching fields that contains a JSON
     * object for its value (as opposed to a regular string). 
     * Default is <code>null</code>.
     * @return regular expression
     * @since 4.1.0
     */
    public String getJsonFieldsPattern() {
        return jsonFieldsPattern;
    }
    /**
     * Sets the regular expression matching fields that contains a JSON
     * object for its value (as opposed to a regular string).
     * @param jsonFieldsPattern regular expression
     * @since 4.1.0
     */
    public void setJsonFieldsPattern(String jsonFieldsPattern) {
        this.jsonFieldsPattern = jsonFieldsPattern;
    }

    /**
     * Whether to ignore response errors.  By default, an exception is 
     * thrown if the Elasticsearch response contains an error.  
     * When <code>true</code> the errors are logged instead.
     * @return <code>true</code> when ignoring response errors
     */
    public boolean isIgnoreResponseErrors() {
        return ignoreResponseErrors;
    }
    /**
     * Sets whether to ignore response errors.  
     * When <code>false</code>, an exception is 
     * thrown if the Elasticsearch response contains an error.  
     * When <code>true</code> the errors are logged instead.
     * @param ignoreResponseErrors <code>true</code> when ignoring response 
     *        errors
     */
    public void setIgnoreResponseErrors(boolean ignoreResponseErrors) {
        this.ignoreResponseErrors = ignoreResponseErrors;
    }

    /**
     * Whether automatic discovery of Elasticsearch cluster nodes should be
     * enabled.
     * @return <code>true</code> if enabled
     */
    public boolean isDiscoverNodes() {
        return discoverNodes;
    }
    /**
     * Sets whether automatic discovery of Elasticsearch cluster nodes should be
     * enabled. 
     * @param discoverNodes <code>true</code> if enabled
     */
    public void setDiscoverNodes(boolean discoverNodes) {
        this.discoverNodes = discoverNodes;
    }

    /**
     * Gets the username.
     * @return username the username
     */
    public String getUsername() {
        return username;
    }
    /**
     * Sets the username.
     * @param username the username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets the password.
     * @return password the password
     */
    public String getPassword() {
        return password;
    }
    /**
     * Sets the password.
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
    }    

    /**
     * Gets the password encryption key.
     * @return the password key or <code>null</code> if the password is not
     * encrypted.
     * @see EncryptionUtil
     */
    public EncryptionKey getPasswordKey() {
        return passwordKey;
    }
    /**
     * Sets the password encryption key. Only required when
     * the password is encrypted.
     * @param passwordKey password key
     * @see EncryptionUtil
     */
    public void setPasswordKey(EncryptionKey passwordKey) {
        this.passwordKey = passwordKey;
    }
    
    /**
     * Gets the character used to replace dots in field names. 
     * Default is <code>null</code> (does not replace dots). 
     * @return replacement character or <code>null</code>
     */
    public String getDotReplacement() {
        return dotReplacement;
    }
    /**
     * Sets the character used to replace dots in field names.
     * @param dotReplacement replacement character or <code>null</code>
     */
    public void setDotReplacement(String dotReplacement) {
        this.dotReplacement = dotReplacement;
    }
    
    /**
     * Gets Elasticsearch connection timeout.
     * @return milliseconds
     * @since 4.1.0
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }
    /**
     * Sets Elasticsearch connection timeout.
     * @param connectionTimeout milliseconds
     * @since 4.1.0
     */
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
    /**
     * Gets Elasticsearch socket timeout.
     * @return milliseconds
     * @since 4.1.0
     */
    public int getSocketTimeout() {
        return socketTimeout;
    }
    /**
     * Sets Elasticsearch socket timeout.
     * @param socketTimeout milliseconds
     * @since 4.1.0
     */
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
    /**
     * Gets Elasticsearch maximum amount of time to wait before retrying
     * a failing host.
     * return milliseconds
     * @since 4.1.0
     */
    public int getMaxRetryTimeout() {
        return maxRetryTimeout;
    }
    /**
     * Sets Elasticsearch maximum amount of time to wait before retrying
     * a failing host.
     * @param maxRetryTimeout milliseconds
     * @since 4.1.0
     */
    public void setMaxRetryTimeout(int maxRetryTimeout) {
        this.maxRetryTimeout = maxRetryTimeout;
    }
    
    @Override
    public void commit() {
        super.commit();
        closeIfDone();
    }
    
    //TODO The following is a super ugly hack to get around not having
    // a close() method (or equivalent) on the Committers yet.
    // So we check that the caller is not itself, which means it should
    // be the parent framework, which should in theory, call this only 
    // once. This is safe to do as the worst case scenario is that a new
    // client is re-created.
    // Remove this method once proper init/close is added to Committers
    private void closeIfDone() {
        StackTraceElement[] els = Thread.currentThread().getStackTrace();
        for (StackTraceElement el : els) {
            if (AbstractCommitter.class.getName().equals(el.getClassName())
                    && "commitIfReady".equals(el.getMethodName())) {
                return;
            }
        }
        close();
    }
    protected void close() {
        IOUtils.closeQuietly(sniffer);
        IOUtils.closeQuietly(client);
        client = null;
        sniffer = null;
        LOG.info("Elasticsearch RestClient closed.");
    }
    
    @Override
    protected void commitBatch(List<ICommitOperation> batch) {
        RestClient safeClient = nullSafeRestClient();
        
        StringBuilder json = new StringBuilder();
        
        LOG.info("Sending " + batch.size() 
                + " commit operations to Elasticsearch.");
        try {
            for (ICommitOperation op : batch) {
                if (op instanceof IAddOperation) {
                    appendAddOperation(json, (IAddOperation) op);
                } else if (op instanceof IDeleteOperation) {
                    appendDeleteOperation(json, (IDeleteOperation) op); 
                } else {
                    close();
                    throw new CommitterException("Unsupported operation:" + op);
                }
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("JSON POST:\n" + StringUtils.trim(json.toString()));
            }
            StringEntity requestEntity = new StringEntity(
                    json.toString(), ContentType.APPLICATION_JSON);
            Response response = safeClient.performRequest(
                    "POST", "/_bulk", Collections.emptyMap(), requestEntity);
            handleResponse(response);
            LOG.info("Done sending commit operations to Elasticsearch.");
        } catch (CommitterException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw new CommitterException(
                    "Could not commit JSON batch to Elasticsearch.", e);
        }
    }

    private void handleResponse(Response response) 
            throws IOException {
        HttpEntity respEntity = response.getEntity();
        if (respEntity != null) {
            String responseAsString = IOUtils.toString(
                    respEntity.getContent(), StandardCharsets.UTF_8);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Elasticsearch response:\n" + responseAsString);
            }
            
            // We have no need to parse the JSON if successful 
            // (saving on the parsing). We'll do it on errors only
            // to filter out successful ones and report only the errors
            if (StringUtils.substring(
                    responseAsString, 0, 100).contains("\"errors\":true")) {
                String error = extractResponseErrors(responseAsString);
                if (ignoreResponseErrors) {
                    LOG.error(error);
                } else {
                    close();
                    throw new CommitterException(error);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Elasticsearch response status:"
                    + response.getStatusLine());
        }
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            close();
            throw new CommitterException(
                  "Invalid HTTP response: " + response.getStatusLine());
        }
    }
    
    private String extractResponseErrors(String response) {
        StringBuilder error = new StringBuilder();
        JSONObject json = new JSONObject(response);
        JSONArray items = json.getJSONArray("items");
        
        for (int i = 0; i < items.length(); i++) {
            JSONObject index = items.getJSONObject(i).getJSONObject("index");
            if (index.has("error")) {
                if (error.length() > 0) {
                    error.append(",\n");
                }
                error.append(index.toString(4));
            }
        }
        error.append(']');
        error.insert(0, "Elasticsearch returned one or more errors:\n[");
        return error.toString();
    }
    
    private void appendAddOperation(StringBuilder json, IAddOperation add) {
        String id = add.getMetadata().getString(getSourceReferenceField());
        if (StringUtils.isBlank(id)) {
            id = add.getReference();
        }
        json.append("{\"index\":{");
        append(json, "_index", getIndexName());
        append(json.append(','), "_type", getTypeName());
        append(json.append(','), "_id", id);
        json.append("}}\n{");
        boolean first = true;
        for (Entry<String, List<String>> entry : add.getMetadata().entrySet()) {
            String field = entry.getKey();
            field = StringUtils.replace(field, ".", dotReplacement); 
            // Remove id from source unless specified to keep it
            if (!isKeepSourceReferenceField()
                    && field.equals(getSourceReferenceField())) {
                continue;
            }
            if (!first) {
                json.append(',');
            }
            append(json, field, entry.getValue());
            first = false;
        }
        json.append("}\n");
    }
    
    private void appendDeleteOperation(
            StringBuilder json, IDeleteOperation del) {
        json.append("{\"delete\":{");
        append(json, "_index", getIndexName());
        append(json.append(','), "_type", getTypeName());
        append(json.append(','), "_id", del.getReference());
        json.append("}}\n");
    }

    private void append(StringBuilder json, String field, List<String> values) {
        if (values.size() == 1) {
            append(json, field, values.get(0));
            return;
        }
        json.append('"')
            .append(StringEscapeUtils.escapeJson(field))
            .append("\":[");
        boolean first = true;
        for (String value : values) {
            if (!first) {
                json.append(',');
            }
            appendValue(json, field, value);
            first = false;
        }
        json.append(']');
    }
    
    private void append(StringBuilder json, String field, String value) {
        json.append('"')
            .append(StringEscapeUtils.escapeJson(field))
            .append("\":");
        appendValue(json, field, value);
    }
    
    private void appendValue(StringBuilder json, String field, String value) {
        if (getJsonFieldsPattern() != null 
                && getJsonFieldsPattern().matches(field)) {
            json.append(value);
        } else {
            json.append('"')
                .append(StringEscapeUtils.escapeJson(value))
                .append("\"");
        }
    }
    
    private synchronized RestClient nullSafeRestClient() {
        if (client == null) {
            if (StringUtils.isBlank(getIndexName())) {
                throw new CommitterException("Index name is undefined.");
            }
            if (StringUtils.isBlank(getTypeName())) {
                throw new CommitterException("Type name is undefined.");
            }
            client = createRestClient();
        }
        if (isDiscoverNodes() && sniffer == null) {
            sniffer = createSniffer(client);
        }
        return client;
    }
    
    protected RestClient createRestClient() {
        String[] elasticHosts = getNodes();
        HttpHost[] httpHosts = new HttpHost[elasticHosts.length];
        for (int i = 0; i < elasticHosts.length; i++) {
            httpHosts[i] = HttpHost.create(elasticHosts[i]);
        }
        
        RestClientBuilder builder = RestClient.builder(httpHosts);
        builder.setFailureListener(new FailureListener() {
            @Override
            public void onFailure(HttpHost host) {
                LOG.error("Failure occured on node: \"" + host
                        + "\". Check node logs.");
            }
        });
        builder.setRequestConfigCallback(rcb -> rcb
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout));
        builder.setMaxRetryTimeoutMillis(maxRetryTimeout);
        
        if (StringUtils.isNotBlank(getUsername())) {
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(
                            getUsername(), EncryptionUtil.decrypt(
                                    getPassword(), getPasswordKey())));
            builder.setHttpClientConfigCallback(
                    b -> b.setDefaultCredentialsProvider(credsProvider));
        }
        return builder.build();
    }

    protected Sniffer createSniffer(RestClient client) {
        // here we assume a cluster is either all https, or all https (no mix).
        if (ArrayUtils.isNotEmpty(nodes) && nodes[0].startsWith("https:")) {
            HostsSniffer hostsSniffer = new ElasticsearchHostsSniffer(client,
                    ElasticsearchHostsSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
                    ElasticsearchHostsSniffer.Scheme.HTTPS);
            return Sniffer.builder(client)
                    .setHostsSniffer(hostsSniffer).build();
        }
        return Sniffer.builder(client).build();
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
        EnhancedXMLStreamWriter w = new EnhancedXMLStreamWriter(writer);
        w.writeElementString("nodes", StringUtils.join(getNodes(), ','));
        w.writeElementString("indexName", getIndexName());
        w.writeElementString("typeName", getTypeName());
        w.writeElementBoolean("ignoreResponseErrors", isIgnoreResponseErrors());
        w.writeElementBoolean("discoverNodes", isDiscoverNodes());
        w.writeElementString("username", getUsername());
        w.writeElementString("password", getPassword());
        w.writeElementString("dotReplacement", getDotReplacement());
        w.writeElementString("jsonFieldsPattern", getJsonFieldsPattern());
        w.writeElementInteger("connectionTimeout", getConnectionTimeout());
        w.writeElementInteger("socketTimeout", getSocketTimeout());
        w.writeElementInteger("maxRetryTimeout", getMaxRetryTimeout());
        
        // Encrypted password:
        EncryptionKey key = getPasswordKey();
        if (key != null) {
            w.writeElementString("passwordKey", key.getValue());
            if (key.getSource() != null) {
                w.writeElementString("passwordKeySource",
                        key.getSource().name().toLowerCase());
            }
        }
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        if (StringUtils.isNotBlank(xml.getString("targetReferenceField"))) {
            close();
            throw new UnsupportedOperationException(
                    "targetReferenceField is not supported by "
                  + ElasticsearchCommitter.class.getSimpleName());
        }
        String targetContentField = xml.getString("targetContentField");
        if (StringUtils.isNotBlank(targetContentField)) {
            setTargetContentField(targetContentField);
        } else {
            setTargetContentField(DEFAULT_ES_CONTENT_FIELD);
        }
        
        if (xml.getString("clusterName", null) != null) {
            LOG.warn("\"clusterName\" is deprecated and will be ignored.");
        }
        if (xml.getString("clusterHosts", null) != null) {
            LOG.warn("\"clusterHosts\" has been replaced with \"nodes\". "
                    + "Please update your configuration file.");
        }
        setNodes(XMLConfigurationUtil.getCSVStringArray(
                xml, "nodes", getNodes()));
        setIndexName(xml.getString("indexName", getIndexName()));
        setTypeName(xml.getString("typeName", getTypeName()));
        setIgnoreResponseErrors(xml.getBoolean(
                "ignoreResponseErrors", isIgnoreResponseErrors()));
        setDiscoverNodes(xml.getBoolean("discoverNodes", isDiscoverNodes()));
        setUsername(xml.getString("username", getUsername()));
        setPassword(xml.getString("password", getPassword()));
        setDotReplacement(xml.getString("dotReplacement", getDotReplacement()));
        setJsonFieldsPattern(
                xml.getString("jsonFieldsPattern", getJsonFieldsPattern()));
        setConnectionTimeout((int) XMLConfigurationUtil.getDuration(
                xml, "connectionTimeout", getConnectionTimeout()));
        setSocketTimeout((int) XMLConfigurationUtil.getDuration(
                xml, "socketTimeout", getSocketTimeout()));
        setMaxRetryTimeout((int) XMLConfigurationUtil.getDuration(
                xml, "maxRetryTimeout", getMaxRetryTimeout()));
        
        // encrypted password:
        String xmlKey = xml.getString("passwordKey", null);
        String xmlSource = xml.getString("passwordKeySource", null);
        if (StringUtils.isNotBlank(xmlKey)) {
            EncryptionKey.Source source = null;
            if (StringUtils.isNotBlank(xmlSource)) {
                source = EncryptionKey.Source.valueOf(xmlSource.toUpperCase());
            }
            setPasswordKey(new EncryptionKey(xmlKey, source));
        }
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .appendSuper(super.hashCode())
                .append(nodes)
                .append(indexName)
                .append(typeName)
                .append(ignoreResponseErrors)
                .append(discoverNodes)
                .append(username)
                .append(password)
                .append(passwordKey)
                .append(dotReplacement)
                .append(jsonFieldsPattern)
                .append(connectionTimeout)
                .append(socketTimeout)
                .append(maxRetryTimeout)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ElasticsearchCommitter)) {
            return false;
        }
        ElasticsearchCommitter other = (ElasticsearchCommitter) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(nodes, other.nodes)
                .append(indexName, other.indexName)
                .append(typeName, other.typeName)
                .append(ignoreResponseErrors, other.ignoreResponseErrors)
                .append(discoverNodes, other.discoverNodes)
                .append(username, other.username)
                .append(password, other.password)
                .append(passwordKey, other.passwordKey)
                .append(dotReplacement, other.dotReplacement)
                .append(jsonFieldsPattern, other.jsonFieldsPattern)
                .append(connectionTimeout, other.connectionTimeout)
                .append(socketTimeout, other.socketTimeout)
                .append(maxRetryTimeout, other.maxRetryTimeout)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("nodes", nodes)
                .append("indexName", indexName)
                .append("typeName", typeName)
                .append("ignoreResponseErrors", ignoreResponseErrors)
                .append("discoverNodes", discoverNodes)
                .append("username", username)
                .append("password", password)
                .append("passwordKey", passwordKey)
                .append("dotReplacement", dotReplacement)
                .append("jsonFieldsPattern", jsonFieldsPattern)
                .append("connectionTimeout", connectionTimeout)
                .append("socketTimeout", socketTimeout)
                .append("maxRetryTimeout", maxRetryTimeout)
                .toString();
    }
}
