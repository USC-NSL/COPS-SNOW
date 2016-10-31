package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class DependencyCheck implements MessageProducer
{
    private static DependencyCheckSerializer serializer_ = new DependencyCheckSerializer();

    public static DependencyCheckSerializer serializer()
    {
        return serializer_;
    }

    private final ArrayList<Dependency> dependencies = new ArrayList<Dependency>();
    private final InetAddress inquiringNode;

    public DependencyCheck(ArrayList<Dependency> dependencies)
    {
        this.dependencies.addAll(dependencies);
        this.inquiringNode = DatabaseDescriptor.getListenAddress();
    }

    public DependencyCheck(ArrayList<Dependency> dependencies, InetAddress inquiringNode)
    {
        this.dependencies.addAll(dependencies);
        this.inquiringNode = inquiringNode;
    }

    public ArrayList<Dependency> getDependencies()
    {
        return dependencies;
    }

    public InetAddress getInquiringNode()
    {
        return inquiringNode;
    }

    public static DependencyCheck fromBytes(byte[] raw, int version) throws IOException
    {
        return serializer_.deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }

    public static class DependencyCheckSerializer implements IVersionedSerializer<DependencyCheck>
    {
        @Override
        public void serialize(DependencyCheck depCheck, DataOutput dos, int version) throws IOException
        {
            int size = depCheck.getDependencies().size();
            dos.writeInt(size);
            for (Dependency dep : depCheck.getDependencies()) {
                Dependency.serializer().serialize(dep, dos);
            }
            dos.writeInt(depCheck.getInquiringNode().getAddress().length);
            dos.write(depCheck.getInquiringNode().getAddress());
        }

        @Override
        public DependencyCheck deserialize(DataInput dis, int version) throws IOException
        {
            int size = dis.readInt();
            ArrayList<Dependency> dependencies = new ArrayList<Dependency>();
            for (int i = 0; i < size; ++i) {
                Dependency dependency = Dependency.serializer().deserialize(dis);
                dependencies.add(dependency);
            }
            int addrSize = dis.readInt();
            byte[] rawAddr = new byte[addrSize];
            dis.readFully(rawAddr);
            InetAddress addr = InetAddress.getByAddress(rawAddr);
            return new DependencyCheck(dependencies, addr);
        }

        @Override
        public long serializedSize(DependencyCheck depCheck, int version)
        {
            int numDep = depCheck.getDependencies().size();
            long size = 0L;
            for (int i = 0; i < numDep; ++i) {
                size += Dependency.serializer().serializedSize(depCheck.getDependencies().get(i));
            }
            size += DBConstants.intSize;	// for size (num of deps)
            size += DBConstants.intSize;
            size += depCheck.getInquiringNode().getAddress().length;
            return size;
        }
    }

    @Override
    public Message getMessage(Integer version) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer_.serialize(this, dob, version);

        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.DEPENDENCY_CHECK, Arrays.copyOf(dob.getData(), dob.getLength()), version);
    }
}
