package org.apache.cassandra.stress.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.Random;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ColumnOrSuperColumnHelper;
import org.apache.cassandra.utils.FBUtilities;

import java.io.PrintStream;

public class DynamicWorkload extends Operation
{
    PrintStream output = System.out;
    private int pre_gen_size;   // for pre-generate 100 key sets

    private static Vector< List<ByteBuffer> > r_key_sets, w_key_sets;

    private static List<ByteBuffer> values;

    public DynamicWorkload(Session session, int index, Vector< List<ByteBuffer> > read_key_sets, Vector< List<ByteBuffer> > write_key_sets, int key_sets_size)
    {
        super(session, index);
        r_key_sets = read_key_sets;
        w_key_sets = write_key_sets;
        pre_gen_size = key_sets_size;
    }

    @Override
    public void run(Cassandra.Client client) throws IOException
    {
        throw new RuntimeException("Dynamic Workload must be run with COPS client library");
    }

    @Override
    public void run(ClientLibrary clientLibrary) throws IOException
    {
        //do all random tosses here
        double opTypeToss = Stress.randomizer.nextDouble();
        if (opTypeToss <= session.getWrite_fraction()) {
            double transactionToss = Stress.randomizer.nextDouble();
            boolean transaction = (transactionToss <= session.getWrite_transaction_fraction());
            write(clientLibrary, session.getColumns_per_key_write(), session.getKeys_per_write(), transaction);
        } else {
            read(clientLibrary, session.getColumns_per_key_read(), session.getKeys_per_read());
        }
    }
    //This is a copy of MultiGetter.run with columnsPerKey and keysPerRead being used instead of the session parameters
    public void read(ClientLibrary clientLibrary, int columnsPerKey, int keysPerRead) throws IOException
    {
        //TODO: Perhaps we should randomize which columns we grab?
        SlicePredicate nColumnsPredicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                      false, columnsPerKey));


        int offset = index * session.getKeysPerThread();
        Map<ByteBuffer,List<ColumnOrSuperColumn>> results;

        int columnCount = 0;
        long bytesCount = 0;

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            List<ByteBuffer> keys = generateKeys(offset, offset + keysPerRead, true);

            for (int j = 0; j < session.getSuperColumns(); j++)
            {
                ColumnParent parent = new ColumnParent("Super1").setSuper_column(ByteBufferUtil.bytes("S" + j));

                long startNano = System.nanoTime();

                boolean success = false;
                String exceptionMessage = null;

                for (int t = 0; t < session.getRetryTimes(); t++)
                {
                    if (success)
                        break;

                    try
                    {
			columnCount = 0;
                        results = clientLibrary.transactional_multiget_slice(keys, parent, nColumnsPredicate);

			//TODO Count subcolumns returned by super columns and use that to check for success
                        success = (results.size() > 0);
                        if (!success)
                            exceptionMessage = "Wrong number of columns: " + results.size() + " instead of " + columnsPerKey*keysPerRead;

                        for (List<ColumnOrSuperColumn> result : results.values()) {
                            columnCount += result.size();
                            for (ColumnOrSuperColumn cosc : result) {
                                bytesCount += ColumnOrSuperColumnHelper.findLength(cosc);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        exceptionMessage = getExceptionMessage(e);
                    }
                }

                if (!success)
                {
                    error(String.format("Operation [%d] retried %d times - error on calling multiget_slice for keys %s %s%n",
                                        index,
                                        session.getRetryTimes(),
                                        keys,
                                        (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
                }

                session.operations.getAndIncrement();
                session.keys.getAndAdd(keys.size());
                session.columnCount.getAndAdd(columnCount);
                session.bytes.getAndAdd(bytesCount);
                long latencyNano = System.nanoTime() - startNano;
                session.latency.getAndAdd(latencyNano/1000000);
                session.latencies.add(latencyNano/1000);
                //RO6
                session.readlatency.getAndAdd(latencyNano/1000000);
                session.readlatencies.add(latencyNano/1000);

                offset += keysPerRead;
            }
        }
        else
        {
            ColumnParent parent = new ColumnParent("Standard1");

            List<ByteBuffer> keys = generateKeys(offset, offset + keysPerRead, true);

            long startNano = System.nanoTime();

            boolean success = false;
            String exceptionMessage = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
		    columnCount = 0;
                    results = clientLibrary.transactional_multiget_slice(keys, parent, nColumnsPredicate);

                    for (List<ColumnOrSuperColumn> result : results.values()) {
                        columnCount += result.size();
                        for (ColumnOrSuperColumn cosc : result) {
                            bytesCount += ColumnOrSuperColumnHelper.findLength(cosc);
                        }
                    }

                    success = (columnCount == columnsPerKey*keysPerRead);
		    if (!success) {
			exceptionMessage = "Wrong number of columns: " + columnCount + " instead of " + columnsPerKey*keysPerRead + ": ";
			for (Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : results.entrySet()) {
			    String key = ByteBufferUtil.string(entry.getKey());
			    int colCount = entry.getValue().size();
			    exceptionMessage += colCount + " for " + key + " ";
			}
		    }
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                    success = false;
                }
            }

            if (!success)
            {
                List<String> raw_keys = new ArrayList<String>();
                for (ByteBuffer key : keys) {
                    raw_keys.add(ByteBufferUtil.string(key));
                }
                error(String.format("Operation [%d] retried %d times - error on calling multiget_slice for keys %s %s%n",
                                    index,
                                    session.getRetryTimes(),
                                    raw_keys,
                                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            session.operations.getAndIncrement();
            session.keys.getAndAdd(keys.size());
            session.columnCount.getAndAdd(columnCount);
            session.bytes.getAndAdd(bytesCount);
            long latencyNano = System.nanoTime() - startNano;
            session.latency.getAndAdd(latencyNano/1000000);
            session.latencies.add(latencyNano/1000);

            //RO6
            session.readlatency.getAndAdd(latencyNano/1000000);
            session.readlatencies.add(latencyNano/1000);

            offset += keysPerRead;
        }
    }

    private List<ByteBuffer> generateKeys(int start, int limit, boolean forReads) throws IOException
    {
        int numKeys = limit - start;

        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();

        // fetch a pre-generated key_set for zipfian case
        if (session.useZipfianGenerator()) {
            // get a random number between 0 -- key_set_size -1
            Random rand = new Random();
            int randomNum = rand.nextInt(pre_gen_size);
            keys = forReads ? r_key_sets.get(randomNum) : w_key_sets.get(randomNum);
            return keys;
        }

        for (int i = 0; i < numKeys*10 && keys.size() < numKeys; i++)
        {
            // We don't want to repeat keys within a mutate or a slice
            // TODO make more efficient
            ByteBuffer newKey;
            newKey = ByteBuffer.wrap(generateKey());
            if (!keys.contains(newKey)) {
                keys.add(newKey);
            }
        }

        if (keys.size() != numKeys) {
            error("Could not generate enough unique keys, " + keys.size() + " instead of " + numKeys);
        }

        return keys;
    }



    public void write(ClientLibrary clientLibrary, int columnsPerKey, int keysPerWrite, boolean transaction) throws IOException
    {
        if (values == null)
            values = generateValues();

        List<Column> columns = new ArrayList<Column>();
        List<SuperColumn> superColumns = new ArrayList<SuperColumn>();

        for (int i = 0; i < columnsPerKey; i++)
        {
            columns.add(new Column(columnName(i, session.timeUUIDComparator))
                                .setValue(values.get(i % values.size()))
                                .setTimestamp(FBUtilities.timestampMicros()));
        }

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            // supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                superColumns.add(new SuperColumn(ByteBufferUtil.bytes(superColumnName), columns));
            }
        }

        Map<ByteBuffer, Map<String, List<Mutation>>> records = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        int offset = index * session.getKeysPerThread();
        List<ByteBuffer> keys = generateKeys(offset, offset + keysPerWrite, false);
        for (ByteBuffer key : keys)
        {
            records.put(key, session.getColumnFamilyType() == ColumnFamilyType.Super
                             ? getSuperColumnsMutationMap(superColumns)
                             : getColumnsMutationMap(columns));
        }

        long startNano = System.nanoTime();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                if (transaction) {
                    clientLibrary.transactional_batch_mutate(records);
                } else {
                    clientLibrary.batch_mutate(records);
                }
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error inserting keys %s %s%n",
                                index,
                                session.getRetryTimes(),
                                keys,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndAdd(keysPerWrite);
        session.columnCount.getAndAdd(keysPerWrite*session.getColumns_per_key_write());
        session.bytes.getAndAdd(keysPerWrite*session.getColumns_per_key_write()*session.getColumnSize());
        long latencyNano = System.nanoTime() - startNano;
        session.latency.getAndAdd(latencyNano/1000000);
        session.latencies.add(latencyNano/1000);

        //RO6
        session.writelatency.getAndAdd(latencyNano/1000000);
        session.writelatencies.add(latencyNano/1000);
    }

    private Map<String, List<Mutation>> getSuperColumnsMutationMap(List<SuperColumn> superColumns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (SuperColumn s : superColumns)
        {
            ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn().setSuper_column(s);
            mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
        }

        mutationMap.put("Super1", mutations);

        return mutationMap;
    }

    private Map<String, List<Mutation>> getColumnsMutationMap(List<Column> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }

        mutationMap.put("Standard1", mutations);

        return mutationMap;
    }
}
