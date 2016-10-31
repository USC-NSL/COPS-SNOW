package org.apache.cassandra.thrift;

import org.apache.cassandra.db.Dependency;
import org.apache.cassandra.db.FetchIdCompletion;
import org.apache.cassandra.db.FetchTxnIds;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.service.*;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by yangyang333 on 16-4-24.
 */
public class BatchWrites {
    private int BATCH_INTERVAL_IN_MS = 100;
    private static final int MAX_BATCH_WRITE_QUEUE_SIZE = 1000;
    private static Logger logger = LoggerFactory.getLogger(BatchWrites.class);
    private static BlockingQueue<Pair<Pair<RowMutation, IWriteResponseHandler>, Long>> batchedWritesQueue = new LinkedBlockingDeque<Pair<Pair<RowMutation, IWriteResponseHandler>, Long>>(MAX_BATCH_WRITE_QUEUE_SIZE);
    public static boolean enqueue (Pair<Pair<RowMutation, IWriteResponseHandler>, Long> mutation) {
        try {
            batchedWritesQueue.put(mutation);
        } catch (InterruptedException e) {
            // re-check loop condition after interrupt
            return false;
        }
        return true;
    }
    public BatchWrites() {
        Runnable doBatchedWrites = new Runnable()
        {
            public void run()
            {
                logger.info("current time: " + System.currentTimeMillis() + "; queue size: " + batchedWritesQueue.size());
                // check if no writes have been batched, simply return
                if (batchedWritesQueue.isEmpty()) {
                    return;
                }
                // group all batched writes so far together
                long chosenTime_min = Integer.MAX_VALUE;
                List<Pair<RowMutation, IWriteResponseHandler>> batchedMutations = new LinkedList<Pair<RowMutation, IWriteResponseHandler>>();
                int elementCount = batchedWritesQueue.size();   // probably we dont need this...
                while (!batchedWritesQueue.isEmpty() && elementCount != 0) {
                    try {
                        Pair<Pair<RowMutation, IWriteResponseHandler>, Long> mutations_handler = batchedWritesQueue.take();
                        Pair<RowMutation, IWriteResponseHandler> mutations = mutations_handler.left;
                        batchedMutations.add(mutations);
                        if (mutations_handler.right < chosenTime_min) {
                            chosenTime_min = mutations_handler.right;
                        }
                        --elementCount;
                    } catch (InterruptedException e) {
                        // re-check loop condition after interrupt
                        continue;
                    }
                }
                // do dep check for all batched mutations together
                fetchTxnIds(batchedMutations, chosenTime_min);
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(doBatchedWrites, BATCH_INTERVAL_IN_MS, BATCH_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
    }

    public void fetchTxnIds(List<Pair<RowMutation, IWriteResponseHandler>> rowMutations, long chosenTime) {
        HashSet<ByteBuffer> keySet = new HashSet<ByteBuffer>();
        ConcurrentHashMap<InetAddress, Set<ByteBuffer>> GroupedKeys = new ConcurrentHashMap<InetAddress, Set<ByteBuffer>>();
        Map<ByteBuffer, HashSet<ByteBuffer>> mutationMap = new HashMap<ByteBuffer, HashSet<ByteBuffer>>();
        for (Pair<RowMutation, IWriteResponseHandler> mutation_pair : rowMutations) {
            RowMutation mutation = mutation_pair.left;
            if (mutation.getDependencies().size() > 0) {
                for (Dependency dep : mutation.getDependencies()) {
                    keySet.add(dep.getLocatorKey());
                    List<InetAddress> localEndpoints = StorageService.instance.getLocalLiveNaturalEndpoints(mutation.getTable(), dep.getLocatorKey());
                    assert localEndpoints.size() == 1 : "Assumed for now";
                    InetAddress localEndpoint = localEndpoints.get(0);
                    if (GroupedKeys.get(localEndpoint) != null) {
                        GroupedKeys.get(localEndpoint).add(dep.getLocatorKey());
                    } else {
                        Set<ByteBuffer> keyList = new HashSet<ByteBuffer>();
                        keyList.add(dep.getLocatorKey());
                        GroupedKeys.put(localEndpoint, keyList);
                    }
                }
                mutationMap.put(mutation.key(), keySet);
                keySet.clear();
            }
        }
        // now groupedKeys has all keys of an entire batchmutate grouped into each endpoint
        // send out fetchtxnid message
        assert GroupedKeys.size() > 0 : "No dependencies; this condition should be filtered out earlier in StorageProxy";
        FetchTxnIdsCallBack fetchIdCallback = new FetchTxnIdsCallBack(new FetchIdCompletion(rowMutations), mutationMap, chosenTime, GroupedKeys.size());
        for (InetAddress ep : GroupedKeys.keySet()) {
            MessagingService.instance().sendRR(new FetchTxnIds(GroupedKeys.get(ep)), ep, fetchIdCallback);
        }
        GroupedKeys.clear();
        GroupedKeys = null;
    }
}
