/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Token;

/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the RF nodes that lie right next to each other
 * on the ring.
 */
public class SimpleStrategy extends AbstractReplicationStrategy
{
    public SimpleStrategy(String table, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        super(table, tokenMetadata, snitch, configOptions);
    }

    public List<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata)
    {
        int replicas = getReplicationFactor();
        ArrayList<Token> tokens = metadata.sortedTokens();
        List<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);

        if (tokens.isEmpty())
            return endpoints;

        // Add the token at the index by default
        Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token, false);
        while (endpoints.size() < replicas && iter.hasNext())
        {
            endpoints.add(metadata.getEndpoint(iter.next()));
        }
        return endpoints;
    }

    public int getReplicationFactor()
    {
        return Integer.parseInt(this.configOptions.get("replication_factor"));
    }

    public void validateOptions() throws ConfigurationException
    {
        if (configOptions == null || configOptions.get("replication_factor") == null)
        {
            throw new ConfigurationException("SimpleStrategy requires a replication_factor strategy option.");
        }
        validateReplicationFactor(configOptions.get("replication_factor"));
    }
}
