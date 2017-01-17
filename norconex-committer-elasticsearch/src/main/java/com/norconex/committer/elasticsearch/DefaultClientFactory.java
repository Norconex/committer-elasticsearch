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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * 
 * Implementation that creates a client that does not hold any data.
 * 
 * @see <a href="http://www.elasticsearch.org/guide/reference/java-api/client/">
 * http://www.elasticsearch.org/guide/reference/java-api/client/</a>
 * 
 * @author Pascal Dimassimo
 * @author Pascal Essiembre
 */
public class DefaultClientFactory implements IClientFactory {

    private static final Logger LOG = LogManager
            .getLogger(DefaultClientFactory.class);

    @Override
    public Client createClient(ElasticsearchCommitter committer) {
        Builder builder = Settings.builder();
        if (StringUtils.isNotBlank(committer.getClusterName())) {
            builder.put("cluster.name", committer.getClusterName());
        }
        TransportClient client = new PreBuiltTransportClient(builder.build());
        
        String clusterHosts = committer.getClusterHosts();
        if (StringUtils.isBlank(committer.getClusterHosts())) {
            clusterHosts = "localhost";
        }
        String[] hosts = clusterHosts.split(",");
        for (String host : hosts) {
            try {
                client.addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName(host), 9300));
            } catch (UnknownHostException e) {
                LOG.error("Can not add host " + host, e);
            }
        }
        return client;
    }
}
