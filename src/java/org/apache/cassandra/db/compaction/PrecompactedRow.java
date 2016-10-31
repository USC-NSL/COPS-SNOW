package org.apache.cassandra.db.compaction;
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


import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.HeapAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PrecompactedRow merges its rows in its constructor in memory.
 */
public class PrecompactedRow extends AbstractCompactedRow
{
    private static Logger logger = LoggerFactory.getLogger(PrecompactedRow.class);

    private final ColumnFamily compactedCf;

    /** it is caller's responsibility to call removeDeleted + removeOldShards from the cf before calling this constructor */
    public PrecompactedRow(DecoratedKey<?> key, ColumnFamily cf)
    {
        super(key);
        compactedCf = cf;
    }

    public static ColumnFamily removeDeletedAndOldShards(DecoratedKey<?> key, CompactionController controller, ColumnFamily cf)
    {
        assert key != null;
        assert controller != null;
        assert cf != null;

        // avoid calling shouldPurge unless we actually need to: it can be very expensive if LCS
        // gets behind and has hundreds of overlapping L0 sstables.  Essentially, this method is an
        // ugly refactor of removeDeletedAndOldShards(controller.shouldPurge(key), controller, cf),
        // taking this into account.
        Boolean shouldPurge = null;

        if (cf.hasExpiredTombstones(controller.gcBefore))
            shouldPurge = controller.shouldPurge(key);
        // We should only gc tombstone if shouldPurge == true. But otherwise,
        // it is still ok to collect column that shadowed by their (deleted)
        // container, which removeDeleted(cf, Integer.MAX_VALUE) will do
        ColumnFamily compacted = ColumnFamilyStore.removeDeleted(cf, shouldPurge != null && shouldPurge ? controller.gcBefore : Integer.MIN_VALUE);

        if (compacted != null && compacted.metadata().getDefaultValidator().isCommutative())
        {
            if (shouldPurge == null)
                shouldPurge = controller.shouldPurge(key);
            if (shouldPurge)
                CounterColumn.mergeAndRemoveOldShards(key, compacted, controller.gcBefore, controller.mergeShardBefore);
        }

        return compacted;
    }

    public static ColumnFamily removeDeletedAndOldShards(DecoratedKey key, boolean shouldPurge, CompactionController controller, ColumnFamily cf)
    {
        // See comment in preceding method
        ColumnFamily compacted = ColumnFamilyStore.removeDeleted(cf, shouldPurge ? controller.gcBefore : Integer.MIN_VALUE);
        if (shouldPurge && compacted != null && compacted.metadata().getDefaultValidator().isCommutative())
            CounterColumn.mergeAndRemoveOldShards(key, compacted, controller.gcBefore, controller.mergeShardBefore);
        return compacted;
    }

    public PrecompactedRow(CompactionController controller, List<SSTableIdentityIterator> rows)
    {
        this(rows.get(0).getKey(),
             removeDeletedAndOldShards(rows.get(0).getKey(), controller, merge(rows)));
    }

    private static ColumnFamily merge(List<SSTableIdentityIterator> rows)
    {
        assert !rows.isEmpty();
        ColumnFamily cf = null;
        for (SSTableIdentityIterator row : rows)
        {
            ColumnFamily thisCF;
            try
            {
                thisCF = row.getColumnFamilyWithColumns();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }

            if (cf == null)
            {
                cf = thisCF;
            }
            else
            {
                // addAll is ok even if cf is an ArrayBackedSortedColumns
                cf.addAll(thisCF, HeapAllocator.instance);
            }
        }
        return cf;
    }

    @Override
    public long write(DataOutput out) throws IOException
    {
        assert compactedCf != null;
        DataOutputBuffer buffer = new DataOutputBuffer();
        DataOutputBuffer headerBuffer = new DataOutputBuffer();
        ColumnIndexer.serialize(compactedCf, headerBuffer);
        ColumnFamily.serializer().serializeForSSTable(compactedCf, buffer);
        int dataSize = headerBuffer.getLength() + buffer.getLength();
        out.writeLong(dataSize);
        out.write(headerBuffer.getData(), 0, headerBuffer.getLength());
        out.write(buffer.getData(), 0, buffer.getLength());
        return dataSize;
    }

    @Override
    public void update(MessageDigest digest)
    {
        assert compactedCf != null;
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            ColumnFamily.serializer().serializeCFInfo(compactedCf, buffer);
            buffer.writeInt(compactedCf.getColumnCount());
            digest.update(buffer.getData(), 0, buffer.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        compactedCf.updateDigest(digest);
    }

    @Override
    public boolean isEmpty()
    {
        return compactedCf == null;
    }

    @Override
    public int columnCount()
    {
        return compactedCf == null ? 0 : compactedCf.getColumnCount();
    }

    @Override
    public long maxTimestamp()
    {
        return compactedCf.maxTimestamp();
    }

    /**
     * @return the full column family represented by this compacted row.
     *
     * We do not provide this method for other AbstractCompactedRow, because this fits the whole row into
     * memory and don't make sense for those other implementations.
     */
    public ColumnFamily getFullColumnFamily()  throws IOException
    {
        return compactedCf;
    }
}
