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

	private static final Logger LOG = LogManager
            .getLogger(DefaultClientFactory.class);
	
	@Override
    public Client createClient(ElasticsearchCommitter committer) {
        
    	if (committer.isUseNodeClient()) {
    		return buildNodeClient(committer);
    	} else {
    		return buildTransportClient(committer);
    	}
    }

	private Client buildNodeClient(ElasticsearchCommitter committer) {
		Builder settingsBuilder = Settings.settingsBuilder()
				.put("path.home", "./es")
				.put("http.enabled", false);
		
		if (StringUtils.isNotBlank(committer.getBindIp())) {
			settingsBuilder.put("network.host", 
					committer.getBindIp());
		}
		
		if (StringUtils.isNotBlank(committer.getClusterHosts())) {
			settingsBuilder.put("discovery.zen.ping.unicast.hosts", 
					committer.getClusterHosts());
		}
		
		Settings settings = settingsBuilder.build();
		
		NodeBuilder builder = nodeBuilder();
		if (StringUtils.isNotBlank(committer.getClusterName())) {
		    builder.clusterName(committer.getClusterName());
		}
		Node node = builder.client(true).settings(settings).node();
		return node.client();
	}

	private Client buildTransportClient(ElasticsearchCommitter committer) {
		TransportClient client;
		if (StringUtils.isNotBlank(committer.getClusterName())) {
			Settings settings = Settings.settingsBuilder()
			        .put("cluster.name", committer.getClusterName()).build();
			client = TransportClient.builder().settings(settings).build();
		} else {
			client = TransportClient.builder().build();
		}
		
		if (StringUtils.isNotBlank(committer.getClusterHosts())) {
			
			String[] hosts = committer.getClusterHosts().split(",");
			for (String host : hosts) {
				try {
					client.addTransportAddress(
							new InetSocketTransportAddress(
									InetAddress.getByName(host), 9300));
				} catch (UnknownHostException e) {
					LOG.error("Can not add host " + host, e);
				}
			}
		}
		
		return client;
	}
}
