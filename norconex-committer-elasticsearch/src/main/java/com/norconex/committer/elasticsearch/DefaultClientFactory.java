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

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * 
 * Implementation that creates a client that does not hold any data.
 * 
 * @see <a href="http://www.elasticsearch.org/guide/reference/java-api/client/">
 * http://www.elasticsearch.org/guide/reference/java-api/client/</a>
 * 
 * @author Pascal Dimassimo
 * 
 */
public class DefaultClientFactory implements IClientFactory {

    @Override
    public Client createClient(ElasticsearchCommitter committer) {
        NodeBuilder builder;
        if (StringUtils.isNotBlank(committer.getClusterName())) {
            builder = nodeBuilder().clusterName(committer.getClusterName());
        } else {
            builder = nodeBuilder();
        }
        Node node = builder.client(true).node();
        return node.client();
    }
}
