package org.apache.cassandra.cache;
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

import org.apache.cassandra.db.ColumnFamily;

import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.googlecode.concurrentlinkedhashmap.Weighers;

import org.github.jamm.MemoryMeter;

public class ConcurrentLinkedHashCacheProvider implements IRowCacheProvider
{
    public ICache<RowCacheKey, ColumnFamily> create(int capacity, boolean useMemoryWeigher)
    {
        return ConcurrentLinkedHashCache.create(capacity, useMemoryWeigher
                                                            ? createMemoryWeigher()
                                                            : Weighers.<ColumnFamily>singleton());
    }

    private static Weigher<ColumnFamily> createMemoryWeigher()
    {
        return new Weigher<ColumnFamily>()
        {
            final MemoryMeter meter = new MemoryMeter();

            @Override
            public int weightOf(ColumnFamily value)
            {
                return (int) Math.min(meter.measure(value), Integer.MAX_VALUE);
            }
        };
    }
}
