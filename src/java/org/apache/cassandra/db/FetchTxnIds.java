package org.apache.cassandra.db;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.xerial.snappy.Snappy;

import java.nio.ByteBuffer;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by haonan on 15-7-8.
 */
public class FetchTxnIds implements MessageProducer {
    private static FetchTxnIdsSerializer serializer_ = new FetchTxnIdsSerializer();

    public static FetchTxnIdsSerializer serializer()
    {
        return serializer_;
    }

    private final Set<ByteBuffer> inquiryKeys = new HashSet<ByteBuffer>();
    private final InetAddress inquiringNode;

    public FetchTxnIds(Set<ByteBuffer> inquiryKeys)
    {
        this.inquiryKeys.addAll(inquiryKeys);
        this.inquiringNode = DatabaseDescriptor.getListenAddress();
    }

    public FetchTxnIds(Set<ByteBuffer> inquiryKeys, InetAddress inquiringNode)
    {
        this.inquiryKeys.addAll(inquiryKeys);
        this.inquiringNode = inquiringNode;
    }

    public Set<ByteBuffer> getInquiryKeys()
    {
        return inquiryKeys;
    }

    public InetAddress getInquiringNode()
    {
        return inquiringNode;
    }

    public static FetchTxnIds fromBytes(byte[] raw, int version) throws IOException
    {
        return serializer_.deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }

    public static class FetchTxnIdsSerializer implements IVersionedSerializer<FetchTxnIds>
    {
        @Override
        public void serialize(FetchTxnIds fetchId, DataOutput dos, int version) throws IOException
        {
            int size = fetchId.getInquiryKeys().size();
            dos.writeInt(size);
            for (ByteBuffer key : fetchId.getInquiryKeys()) {
                ByteBufferUtil.writeWithShortLength(key, dos);
            }
            dos.writeInt(fetchId.getInquiringNode().getAddress().length);
            dos.write(fetchId.getInquiringNode().getAddress());
        }

        @Override
        public FetchTxnIds deserialize(DataInput dis, int version) throws IOException
        {
            int size = dis.readInt();
            Set<ByteBuffer> inquiryKeys = new HashSet<ByteBuffer>();
            for (int i = 0; i < size; ++i) {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
                inquiryKeys.add(key);
            }
            int addrSize = dis.readInt();
            byte[] rawAddr = new byte[addrSize];
            dis.readFully(rawAddr);
            InetAddress addr = InetAddress.getByAddress(rawAddr);
            return new FetchTxnIds(inquiryKeys, addr);
        }

        @Override
        public long serializedSize(FetchTxnIds fetchId, int version)
        {
            int numKeys = fetchId.getInquiryKeys().size();
            long size = 0L;
            size = DBConstants.shortSize * numKeys;
            size += DBConstants.intSize;	// for size (num of keys)
            size += DBConstants.intSize;
            size += fetchId.getInquiringNode().getAddress().length;
            return size;
        }
    }

    @Override
    public Message getMessage(Integer version) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer_.serialize(this, dob, version);

        byte[] msg = Snappy.compress(dob.getData());
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.FETCH_TXNIDS, Arrays.copyOf(msg, msg.length), version);
    }
}
