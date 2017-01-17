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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;

/**
 * <p>
 * Commits documents to Elasticsearch using ES Transport client.
 * </p>  
 * <p>
 * Despite being a subclass
 * of {@link AbstractMappedCommitter}, setting an <code>idTargetField</code>
 * is not supported.
 * </p>  
 * <h3>XML configuration usage:</h3>
 * 
 * <pre>
 *  &lt;committer class="com.norconex.committer.elasticsearch.ElasticsearchCommitter"&gt;
 *      &lt;indexName&gt;(Name of the index to use)&lt;/indexName&gt;
 *      &lt;typeName&gt;(Name of the type to use)&lt;/typeName&gt;
 *      &lt;clusterName&gt;
 *         (Name of the ES cluster to join. Default is "elasticsearch".)
 *      &lt;/clusterName&gt;
 *      &lt;clusterHosts&gt;
 *      	(Comma delimited list of hosts to connect to join the cluster. 
 *      	Default is "localhost".)
 *      &lt;/clusterHosts&gt;
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
 *      &lt;maxRetryWait&gt;(max delay between retries)&lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * @author Pascal Dimassimo
 * @author Pascal Essiembre
 */
public class ElasticsearchCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = LogManager
            .getLogger(ElasticsearchCommitter.class);

    public static final String DEFAULT_ES_CONTENT_FIELD = "content";

    private final IClientFactory clientFactory;
    private Client client;
    private String clusterName;
    private String clusterHosts;
    private String indexName;
    private String typeName;

    /**
     * Constructor.
     */
    public ElasticsearchCommitter() {
        this(null);
    }

    /**
     * Constructor.
     * @param factory Elasticsearch client factory
     */
    public ElasticsearchCommitter(IClientFactory factory) {
        if (factory == null) {
            this.clientFactory = new DefaultClientFactory();
        } else {
            this.clientFactory = factory;
        }
    }

    /**
     * Gets the cluster name.
     * @return the cluster name
     */
    public String getClusterName() {
        return clusterName;
    }
    /**
     * Sets the cluster name.
     * @param clusterName the cluster name
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
	 * @return the clusterHosts
	 */
	public String getClusterHosts() {
		return clusterHosts;
	}

	/**
	 * @param clusterHosts the clusterHosts to set
	 */
	public void setClusterHosts(String clusterHosts) {
		this.clusterHosts = clusterHosts;
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

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {
        if (StringUtils.isBlank(getIndexName())) {
            throw new CommitterException("Index name is undefined.");
        }
        if (StringUtils.isBlank(getTypeName())) {
            throw new CommitterException("Type name is undefined.");
        }
        LOG.info("Sending " + batch.size() + " operations to Elasticsearch.");
        
        if (client == null) {
            client = clientFactory.createClient(this);
        }

        List<IAddOperation> additions = new ArrayList<IAddOperation>();
        List<IDeleteOperation> deletions = new ArrayList<IDeleteOperation>();
        for (ICommitOperation op : batch) {
            if (op instanceof IAddOperation) {
                additions.add((IAddOperation) op); 
            } else if (op instanceof IDeleteOperation) {
                deletions.add((IDeleteOperation) op); 
            } else {
                throw new CommitterException("Unsupported operation:" + op);
            }
        }
        try {
            bulkDeletedDocuments(deletions);
            bulkAddedDocuments(additions);            
            LOG.info("Done sending operations to Elasticsearch.");
        } catch (IOException e) {
            throw new CommitterException(
                    "Cannot index document batch to Elasticsearch.", e);
        }
    }
    
    /**
     * Add all queued documents to add to a {@link BulkRequestBuilder}
     * 
     * @param bulkRequest
     * @throws IOException
     */
    private void bulkAddedDocuments(List<IAddOperation> addOperations)
            throws IOException {
        if (addOperations.isEmpty()) {
            return;
        }
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (IAddOperation op : addOperations) {
            IndexRequestBuilder request = 
                    client.prepareIndex(getIndexName(), getTypeName());
            String id = op.getMetadata().getString(getSourceReferenceField());
            if (StringUtils.isBlank(id)) {
                id = op.getReference();
            }
            request.setId(id);
            request.setSource(buildSourceContent(op.getMetadata()));
            bulkRequest.add(request);
        }
        sendBulkToES(bulkRequest);
    }
    private XContentBuilder buildSourceContent(Properties fields)
            throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        for (String key : fields.keySet()) {
            // Remove id from source unless specified to keep it
            if (!isKeepSourceReferenceField()
                    && key.equals(getSourceReferenceField())) {
                continue;
            }
            
            // Elasticsearch 2: you can no longer create fields with dots in 
            // the name (https://www.elastic.co/guide/en/elasticsearch/reference/
            // current/breaking_20_mapping_changes.html
            // #_field_names_may_not_contain_dots). 
            // So they are replaced by '_'.
            String replaced = key.replaceAll("\\.", "_");
            builder.field(replaced, fields.getStrings(key));
        }
        return builder.endObject();
    }
    

    private void bulkDeletedDocuments(List<IDeleteOperation> deleteOperations) {
        if (deleteOperations.isEmpty()) {
            return;
        }
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (IDeleteOperation op : deleteOperations) {
            DeleteRequestBuilder request = client.prepareDelete(
                    indexName, typeName, op.getReference());
            bulkRequest.add(request);
        }
        sendBulkToES(bulkRequest);
    }


    /**
     * Send {@link BulkRequestBuilder} to ES
     * 
     * @param bulkRequest
     */
    private void sendBulkToES(BulkRequestBuilder bulkRequest) {
        ListenableActionFuture<BulkResponse> execution = bulkRequest.execute();
        
        BulkResponse bulkResponse = execution.actionGet();
        if (bulkResponse.hasFailures()) {
            throw new CommitterException(
                    "Cannot index document batch to Elasticsearch: "
                            + bulkResponse.buildFailureMessage());
        }
    }


    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {

        	if (StringUtils.isNotBlank(clusterName)) {
        	    writer.writeStartElement("clusterName");
    	        writer.writeCharacters(clusterName);
    	        writer.writeEndElement();
        	}
        
        if (StringUtils.isNotBlank(clusterHosts)) {
            writer.writeStartElement("clusterHosts");
            writer.writeCharacters(clusterHosts);
            writer.writeEndElement();
        }
        
        if (StringUtils.isNotBlank(indexName)) {
	        writer.writeStartElement("indexName");
	        writer.writeCharacters(indexName);
	        writer.writeEndElement();
        }

        if (StringUtils.isNotBlank(typeName)) {
        	    writer.writeStartElement("typeName");
	        writer.writeCharacters(typeName);
	        writer.writeEndElement();
        }
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        if (StringUtils.isNotBlank(xml.getString("targetReferenceField"))) {
            throw new UnsupportedOperationException(
                    "targetReferenceField is not supported by "
                  + "ElasticsearchCommitter");
        }
        String targetContentField = xml.getString("targetContentField");
        if (StringUtils.isNotBlank(targetContentField)) {
            setTargetContentField(targetContentField);
        } else {
            setTargetContentField(DEFAULT_ES_CONTENT_FIELD);
        }
        setClusterName(xml.getString("clusterName", null));
        setClusterHosts(xml.getString("clusterHosts", null));
        setIndexName(xml.getString("indexName", null));
        setTypeName(xml.getString("typeName", null));
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().appendSuper(super.hashCode())
                .append(clusterName).append(indexName)
                .append(typeName).toHashCode();
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
        return new EqualsBuilder().appendSuper(super.equals(obj))
                .append(clusterName, other.clusterName)
                .append(indexName, other.indexName)
                .append(typeName, other.typeName).isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).appendSuper(super.toString())
                .append("clientFactory", clientFactory)
                .append("client", client).append("clusterName", clusterName)
                .append("indexName", indexName).append("typeName", typeName)
                .toString();
    }
}
