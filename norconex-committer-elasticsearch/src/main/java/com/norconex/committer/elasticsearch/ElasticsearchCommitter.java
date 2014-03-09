/* Copyright 2013 Norconex Inc.
 * 
 * This file is part of Norconex ElasticSearch Committer.
 * 
 * Norconex ElasticSearch Committer is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License as 
 * published by the Free Software Foundation, either version 3 of the License, 
 * or (at your option) any later version.
 * 
 * Norconex ElasticSearch Committer is distributed in the hope that it will be 
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Norconex ElasticSearch Committer. 
 * If not, see <http://www.gnu.org/licenses/>.
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.norconex.committer.AbstractMappedCommitter;
import com.norconex.committer.CommitterException;
import com.norconex.committer.IAddOperation;
import com.norconex.committer.ICommitOperation;
import com.norconex.committer.IDeleteOperation;
import com.norconex.commons.lang.map.Properties;

/**
 * Commits documents to Elasticsearch.  Despite being a subclass
 * of {@link AbstractMappedCommitter}, setting an <code>idTargetField</code>
 * is not supported.
 * <p>
 * XML configuration usage:
 * </p>
 * 
 * <pre>
 *  &lt;committer class="com.norconex.committer.elasticsearch.ElasticsearchCommitter"&gt;
 *      &lt;indexName&gt;(Name of the index to use)&lt;/indexName&gt;
 *      &lt;typeName&gt;(Name of the type to use)&lt;/typeName&gt;
 *      &lt;clusterName&gt;
 *         (Name of the ES cluster. Use if you have multiple clusters.)
 *      &lt;/clusterName&gt;
 *      &lt;idSourceField keep="[false|true]"&gt;
 *         (Name of source field that will be mapped to the ES "id" field
 *         or whatever "idTargetField" specified.
 *         Default is the document reference metadata field: 
 *         "document.reference".  Once re-mapped, the metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/idSourceField&gt;
 *      &lt;contentSourceField keep="[false|true]&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/contentSourceField&gt;
 *      &lt;contentTargetField&gt;
 *         (ES target field name for a document content/body.
 *          Default is: content)
 *      &lt;/contentTargetField&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of docs to send ES at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay between retries)&lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * 
 * @author Pascal Dimassimo
 * @author Pascal Essiembre
 */
public class ElasticsearchCommitter extends AbstractMappedCommitter {

    private static final long serialVersionUID = 7000534391754478817L;

    private static final Logger LOG = LogManager
            .getLogger(ElasticsearchCommitter.class);

    public static final String DEFAULT_ES_CONTENT_FIELD = "content";

    private final IClientFactory clientFactory;
    private Client client;
    private String clusterName;
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
     * @param factory Elasticsearch client factorys
     */
    public ElasticsearchCommitter(IClientFactory factory) {
        if (factory == null) {
            this.clientFactory = new DefaultClientFactory();
        } else {
            this.clientFactory = factory;
        }
        setContentTargetField(DEFAULT_ES_CONTENT_FIELD);
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
            request.setId(op.getMetadata().getString(getIdSourceField()));
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
            if (!isKeepIdSourceField() && key.equals(getIdSourceField())) {
                continue;
            }
            List<String> values = fields.getStrings(key);
            for (String value : values) {
                builder.field(key, value);
            }
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
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            throw new CommitterException(
                    "Cannot index document batch to Elasticsearch: "
                            + bulkResponse.buildFailureMessage());
        }
    }


    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {

        writer.writeStartElement("clusterName");
        writer.writeCharacters(clusterName);
        writer.writeEndElement();

        writer.writeStartElement("indexName");
        writer.writeCharacters(indexName);
        writer.writeEndElement();

        writer.writeStartElement("typeName");
        writer.writeCharacters(typeName);
        writer.writeEndElement();
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        if (StringUtils.isNotBlank(xml.getString("idTargetField"))) {
            throw new UnsupportedOperationException(
                    "idTargetField is not supported by ElasticsearchCommitter");
        }
        setClusterName(xml.getString("clusterName", null));
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
        return String.format("ElasticsearchCommitter "
                + "[esBatchSize=%s, docsToAdd=%s, docsToRemove=%s, "
                + "clientFactory=%s, client=%s, clusterName=%s, "
                + "indexName=%s, typeName=%s, bulkRequest=%s, "
                + "BaseCommitter=%s]",
                clientFactory, client, clusterName, indexName, typeName,
                super.toString());
    }


}
