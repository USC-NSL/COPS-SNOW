/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql;

import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;

public abstract class AbstractModification
{
    public static final ConsistencyLevel defaultConsistency = ConsistencyLevel.ONE;

    protected final String keyspace;
    protected final String columnFamily;
    protected final ConsistencyLevel cLevel;
    protected final Long timestamp;
    protected final int timeToLive;
    protected final String keyName;

    public AbstractModification(String keyspace, String columnFamily, String keyAlias, Attributes attrs)
    {
        this(keyspace, columnFamily, keyAlias, attrs.getConsistencyLevel(), attrs.getTimestamp(), attrs.getTimeToLive());
    }

    public AbstractModification(String keyspace, String columnFamily, String keyAlias, ConsistencyLevel cLevel, Long timestamp, int timeToLive)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        this.timestamp = timestamp;
        this.timeToLive = timeToLive;
        this.keyName = keyAlias.toUpperCase();
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return (cLevel != null) ? cLevel : defaultConsistency;
    }

    /**
     * True if an explicit consistency level was parsed from the statement.
     *
     * @return true if a consistency was parsed, false otherwise.
     */
    public boolean isSetConsistencyLevel()
    {
        return cLevel != null;
    }

    public long getTimestamp(ClientState clientState)
    {
        return timestamp == null ? clientState.getTimestamp() : timestamp;
    }

    public boolean isSetTimestamp()
    {
        return timestamp != null;
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param keyspace The working keyspace
     * @param clientState current client status
     *
     * @return list of the mutations
     *
     * @throws InvalidRequestException on the wrong request
     */
    public abstract List<IMutation> prepareRowMutations(String keyspace, ClientState clientState, List<String> variables)
            throws org.apache.cassandra.thrift.InvalidRequestException;

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param keyspace The working keyspace
     * @param clientState current client status
     * @param timestamp global timestamp to use for all mutations
     *
     * @return list of the mutations
     *
     * @throws InvalidRequestException on the wrong request
     */
    public abstract List<IMutation> prepareRowMutations(String keyspace, ClientState clientState, Long timestamp, List<String> variables)
            throws org.apache.cassandra.thrift.InvalidRequestException;
}
