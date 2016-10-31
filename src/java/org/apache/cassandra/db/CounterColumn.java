/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.IWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.*;
import org.apache.log4j.Logger;

/**
 * A column that represents a partitioned counter.
 */
public class CounterColumn extends Column
{
    private static final Logger logger = Logger.getLogger(CounterColumn.class);

    protected static final CounterContext contextManager = CounterContext.instance();

    private final long timestampOfLastDelete;

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp,
            Long lastAccessTime, Long previousVersionLastAccessTime, Long earliestValidTime, Long latestValidTime, NavigableSet<IColumn> previousVersions)
    {
        super(name, value, timestamp, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, null);
        this.timestampOfLastDelete = Long.MIN_VALUE;
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp, long timestampOfLastDelete,
            Long lastAccessTime, Long previousVersionLastAccessTime, Long earliestValidTime, Long latestValidTime, NavigableSet<IColumn> previousVersions)
    {
        super(name, value, timestamp, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions, null);

        this.timestampOfLastDelete = timestampOfLastDelete;
    }

    //WL TODO: Too many constructors, make sense of this

    public CounterColumn(ByteBuffer name, long value, long timestamp, long timestampOfLastDelete)
    {
        this(name, contextManager.create(value, timestamp, ByteBufferUtil.EMPTY_BYTE_BUFFER, HeapAllocator.instance), timestamp, timestampOfLastDelete,  null, null, LamportClock.getVersion(), null, null);
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp, long timestampOfLastDelete)
    {
        this(name, value, timestamp, timestampOfLastDelete,  null, null, LamportClock.getVersion(), null, null);
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, Long.MIN_VALUE, null, null, LamportClock.getVersion(), null, null);
    }

    public CounterColumn(ByteBuffer name, long value, long timestamp)
    {
        this(name, contextManager.create(value, timestamp, ByteBufferUtil.EMPTY_BYTE_BUFFER, HeapAllocator.instance), timestamp, Long.MIN_VALUE,  null, null, LamportClock.getVersion(), null, null);
    }

    public static CounterColumn create(ByteBuffer name, ByteBuffer value, long timestamp, long timestampOfLastDelete, IColumnSerializer.Flag flag,
            Long lastAccessTime, Long previousVersionLastAccessTime, Long earliestValidTime, Long latestValidTime, NavigableSet<IColumn> previousVersions)
    {
        // #elt being negative means we have to clean delta
        short count = value.getShort(value.position());
        if (flag == IColumnSerializer.Flag.FROM_REMOTE || (flag == IColumnSerializer.Flag.LOCAL && count < 0))
            value = CounterContext.instance().clearAllDelta(value);
        return new CounterColumn(name, value, timestamp, timestampOfLastDelete, lastAccessTime, previousVersionLastAccessTime, earliestValidTime, latestValidTime, previousVersions);
    }

    public long timestampOfLastDelete()
    {
        return timestampOfLastDelete;
    }

    public long total()
    {
        return contextManager.total(value);
    }

    @Override
    public int size()
    {
        /*
         * A counter column adds to a Column :
         *  + 8 bytes for timestampOfLastDelete
         */
        return super.size() + DBConstants.tsSize;
    }

    @Override
    public IColumn diff(IColumn column)
    {
        assert column instanceof CounterColumn || column instanceof DeletedColumn: "Wrong class type.";

        if (column instanceof DeletedColumn) {
            return super.diff(column);
        }

        if (timestamp() < column.timestamp())
            return column;
        if (timestampOfLastDelete() < ((CounterColumn)column).timestampOfLastDelete())
            return column;
        ContextRelationship rel = contextManager.diff(column.value(), value());
        if (ContextRelationship.GREATER_THAN == rel || ContextRelationship.DISJOINT == rel)
            return column;
        return null;
    }

    /*
     * We have to special case digest creation for counter column because
     * we don't want to include the information about which shard of the
     * context is a delta or not, since this information differs from node to
     * node.
     */
    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.duplicate());
        // We don't take the deltas into account in a digest
        contextManager.updateDigest(digest, value);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp);
            buffer.writeByte(serializationFlags());
            buffer.writeLong(timestampOfLastDelete);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public IColumn reconcile(IColumn column, Allocator allocator)
    {
        assert (column instanceof CounterColumn) || (column instanceof DeletedColumn) : "Wrong class type.";

        if (column.isMarkedForDelete()) // live + tombstone: track last tombstone
        {
            if (timestamp() < column.timestamp()) // live < tombstone => this was deleted
            {
                return ((Column) column).updatePreviousVersion(this);
            }
            // live last delete >= tombstone => this is newer than delete and we already know of this delete or a newer one
            if (timestampOfLastDelete() >= column.timestamp())
            {
                return this.updatePreviousVersion((Column) column);
            }
            // live last delete < tombstone => this is newer than delete, but we didn't know about it, so update timestampOfLastDelete
            long updatedTimestampOfLastDelete = column.timestamp();
            return new CounterColumn(name(), value(), timestamp(), updatedTimestampOfLastDelete, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions);
        }
        // live < live last delete => there was a delete in between this and the add that created column, so the value should not include this
        if (timestamp() < ((CounterColumn)column).timestampOfLastDelete())
            return ((Column) column).updatePreviousVersion(this);
        // live last delete > live => there was a delete in between column and the add that created this, so the value should not include column
        if (timestampOfLastDelete() > column.timestamp())
            return this.updatePreviousVersion((Column) column);

        // live + live: merge clocks; update value
        // By convention the currently visible value is column, so we'll merge in the results, and then updatePrevious with that
        CounterColumn mergedCounterColumn = new CounterColumn(
            name(),
            contextManager.merge(value(), column.value(), allocator),
            Math.max(timestamp(), column.timestamp()),
            Math.max(timestampOfLastDelete(), ((CounterColumn)column).timestampOfLastDelete()),
            null, null, LamportClock.getVersion(), null, null);
        mergedCounterColumn.updatePreviousVersion((Column) column);
        return mergedCounterColumn;
    }

    @Override
    public boolean equals(Object o)
    {
        // super.equals() returns false if o is not a CounterColumn
        return super.equals(o) && timestampOfLastDelete == ((CounterColumn)o).timestampOfLastDelete;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (int)(timestampOfLastDelete ^ (timestampOfLastDelete >>> 32));
        return result;
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs)
    {
        return new CounterColumn(cfs.internOrCopy(name, HeapAllocator.instance), ByteBufferUtil.clone(value), timestamp, timestampOfLastDelete, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions);
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new CounterColumn(cfs.internOrCopy(name, allocator), allocator.clone(value), timestamp, timestampOfLastDelete, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions);
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(contextManager.toString(value));
        sb.append("=");
        sb.append(total());
        sb.append("@");
        sb.append(timestamp());
        sb.append("!");
        sb.append(timestampOfLastDelete);
        return sb.toString();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(ByteBufferUtil.bytesToHex(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(contextManager.toString(value));
        sb.append("=");
        sb.append(total());
        sb.append("@");
        sb.append(timestamp());
        sb.append("!");
        if (timestampOfLastDelete == Long.MIN_VALUE) {
            sb.append("--");
        } else {
            sb.append(timestampOfLastDelete);
        }
        return sb.toString();
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_MASK;
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        // We cannot use the value validator as for other columns as the CounterColumnType validate a long,
        // which is not the internal representation of counters
        contextManager.validateContext(value());
    }

    /**
     * Check if a given nodeId is found in this CounterColumn context.
     */
    public boolean hasNodeId(NodeId id)
    {
        return contextManager.hasNodeId(value(), id);
    }

    private CounterColumn computeOldShardMerger(int mergeBefore)
    {
        ByteBuffer bb = contextManager.computeOldShardMerger(value(), NodeId.getOldLocalNodeIds(), mergeBefore);
        if (bb == null)
            return null;
        else
            return new CounterColumn(name(), bb, timestamp(), timestampOfLastDelete, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions);
    }

    private CounterColumn removeOldShards(int gcBefore)
    {
        ByteBuffer bb = contextManager.removeOldShards(value(), gcBefore);
        if (bb == value())
            return this;
        else
        {
            return new CounterColumn(name(), bb, timestamp(), timestampOfLastDelete, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions);
        }
    }

    public static void mergeAndRemoveOldShards(DecoratedKey key, ColumnFamily cf, int gcBefore, int mergeBefore)
    {
        mergeAndRemoveOldShards(key, cf, gcBefore, mergeBefore, true);
    }

    /**
     * There is two phase to the removal of old shards.
     * First phase: we merge the old shard value to the current shard and
     * 'nulify' the old one. We then send the counter context with the old
     * shard nulified to all other replica.
     * Second phase: once an old shard has been nulified for longer than
     * gc_grace (to be sure all other replica had been aware of the merge), we
     * simply remove that old shard from the context (it's value is 0).
     * This method does both phases.
     * (Note that the sendToOtherReplica flag is here only to facilitate
     * testing. It should be true in real code so use the method above
     * preferably)
     */
    public static void mergeAndRemoveOldShards(DecoratedKey key, ColumnFamily cf, int gcBefore, int mergeBefore, boolean sendToOtherReplica)
    {
        ColumnFamily remoteMerger = null;
        if (!cf.isSuper())
        {
            for (IColumn c : cf)
            {
                if (!(c instanceof CounterColumn))
                    continue;
                CounterColumn cc = (CounterColumn) c;
                CounterColumn shardMerger = cc.computeOldShardMerger(mergeBefore);
                CounterColumn merged = cc;
                if (shardMerger != null)
                {
                    merged = (CounterColumn) cc.reconcile(shardMerger);
                    if (remoteMerger == null)
                        remoteMerger = cf.cloneMeShallow();
                    remoteMerger.addColumn(merged);
                }
                CounterColumn cleaned = merged.removeOldShards(gcBefore);
                if (cleaned != cc)
                {
                    cf.replace(cc, cleaned);
                }
            }
        }
        else
        {
            for (IColumn col : cf)
            {
                SuperColumn c = (SuperColumn)col;
                for (IColumn subColumn : c.getSubColumns())
                {
                    if (!(subColumn instanceof CounterColumn))
                        continue;
                    CounterColumn cc = (CounterColumn) subColumn;
                    CounterColumn shardMerger = cc.computeOldShardMerger(mergeBefore);
                    CounterColumn merged = cc;
                    if (shardMerger != null)
                    {
                        merged = (CounterColumn) cc.reconcile(shardMerger);
                        if (remoteMerger == null)
                            remoteMerger = cf.cloneMeShallow();
                        remoteMerger.addColumn(c.name(), merged);
                    }
                    CounterColumn cleaned = merged.removeOldShards(gcBefore);
                    if (cleaned != subColumn)
                        c.replace(subColumn, cleaned);
                }
            }
        }

        if (remoteMerger != null && sendToOtherReplica)
        {
            try
            {
                sendToOtherReplica(key, remoteMerger);
            }
            catch (Exception e)
            {
                logger.error("Error while sending shard merger mutation to remote endpoints", e);
            }
        }
    }

    public IColumn markDeltaToBeCleared()
    {
        return new CounterColumn(name, contextManager.markDeltaToBeCleared(value), timestamp, timestampOfLastDelete, lastAccessTime, lastAccessTimeOfAPreviousVersion, earliestValidTime, latestValidTime, previousVersions);
    }

    private static void sendToOtherReplica(DecoratedKey key, ColumnFamily cf) throws UnavailableException, TimeoutException, IOException
    {
        RowMutation rm = new RowMutation(cf.metadata().ksName, key.key);
        rm.add(cf);

        final InetAddress local = FBUtilities.getBroadcastAddress();
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(local);

        StorageProxy.performWrite(rm, ConsistencyLevel.ANY, localDataCenter, new StorageProxy.WritePerformer()
        {
            @Override
            public void apply(IMutation mutation, Collection<InetAddress> targets, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException, TimeoutException
            {
                // We should only send to the remote replica, not the local one
                targets.remove(local);
                // Fake local response to be a good lad but we won't wait on the responseHandler
                responseHandler.response(null);
                StorageProxy.sendToHintedEndpoints((RowMutation) mutation, targets, responseHandler, localDataCenter, consistency_level);
            }
        });

        // we don't wait for answers
    }
}
