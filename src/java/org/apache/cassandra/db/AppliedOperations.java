package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.ReadTransactionIdTracker;    //HL: for txnId check during dep_check
import org.apache.cassandra.db.DependencyCheck;
import java.io.IOException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.db.DBConstants;
import org.xerial.snappy.Snappy;

public class AppliedOperations
{
    private static Logger logger = LoggerFactory.getLogger(AppliedOperations.class);

    enum OpStatus {
        PENDING,
        APPLIED
    }

    /**
     * Pending Operations are partitioned by node (via it's ShortNodeId that is embedded in timestamps).
     * We know that ops arrive from each node in order here, but can then be processed in a different order
     * because we're multithreaded.  So we can only clean up applied Ops that are older than the oldest of
     * the per-thread newest ops.
     */

    static Map<Short, SortedMap<Long, OpStatus>> shortNodeIdToPendingOps = new HashMap<Short, SortedMap<Long, OpStatus>>();
    static Map<String, Long> threadIdToNewestOp = new HashMap<String, Long>();

    private static class DepCheckReplyInfo
    {
        private final Message message;
        private final String id;
        private final ByteBuffer locatorKey;    //HL: keep locator key

        public DepCheckReplyInfo(Message message, String id, ByteBuffer locatorKey)
        {
            this.message = message;
            this.id = id;
            this.locatorKey = locatorKey;
        }

        public Message getMessage()
        {
            return message;
        }

        public String getId()
        {
            return id;
        }

        public ByteBuffer getLocatorKey() { return locatorKey; }
    }
    static Map<Long, Queue<DepCheckReplyInfo>> blockedDepChecks = new HashMap<Long, Queue<DepCheckReplyInfo>>();

    private static void updateNewestOp(String threadName, Long opTimestamp)
    {
        if (!threadName.contains("MutationStage") && !threadName.contains("RequestResponseStage")) {
            //We ignore updates that aren't mutations, they only show up at system start up and we don't care about them
            logger.debug("Ignoring updateNewestOp for: " + threadName);
            return;
        }
        Long previousOpTimestamp = threadIdToNewestOp.get(threadName);
        if (previousOpTimestamp == null) {
	        //Replicated operations get written too, we'll have more writing threads if we have local writers and replicated writers
            //WL TODO: keep the size of threadIdToNewestOp bounded
            //assert threadIdToNewestOp.size() <= 2*DatabaseDescriptor.getConcurrentWriters() : "Set of writing threads should be static (" + threadIdToNewestOp.size() + ", " + DatabaseDescriptor.getConcurrentWriters() + "), adding " + threadName + " to " + threadIdToNewestOp;
            threadIdToNewestOp.put(threadName, opTimestamp);
        } else {
            if (opTimestamp > previousOpTimestamp) {
                threadIdToNewestOp.put(threadName, opTimestamp);
            }
        }
    }

    private static Long oldestNewestOp()
    {
        Long oldest = Long.MAX_VALUE;
        for (Long opTimestamp : threadIdToNewestOp.values()) {
            oldest = Math.min(oldest, opTimestamp);
        }
        return oldest;
    }

    //WL TODO reduce the granularity of synchronization here

    public static synchronized void addPendingOp(ByteBuffer locatorKey, long timestamp)
    {
        if (VersionUtil.extractDatacenter(timestamp) == ShortNodeId.getLocalDC()) {
            if (logger.isDebugEnabled())
                logger.debug("Local DC origin, so not adding pending op: {}", new Dependency(locatorKey, timestamp));
            return;
        }

        if (logger.isDebugEnabled())
            logger.debug("Add pending op: {}", new Dependency(locatorKey, timestamp));

        updateNewestOp(Thread.currentThread().getName(), timestamp);

        SortedMap<Long, OpStatus> pendingOps = shortNodeIdToPendingOps.get(VersionUtil.extractShortNodeId(timestamp));
        if (pendingOps == null) {
            pendingOps = new TreeMap<Long, OpStatus>();
            shortNodeIdToPendingOps.put(VersionUtil.extractShortNodeId(timestamp), pendingOps);
        }

        pendingOps.put(timestamp, OpStatus.PENDING);
    }

    public static synchronized void addAppliedOp(ByteBuffer locatorKey, long timestamp)
    {
        logger.debug("addAppliedOp called: {}", new Dependency(locatorKey, timestamp));

        if (VersionUtil.extractDatacenter(timestamp) == ShortNodeId.getLocalDC()) {
            return;
        }

        logger.debug("Add applied op: {}", new Dependency(locatorKey, timestamp));

        updateNewestOp(Thread.currentThread().getName(), timestamp);

        SortedMap<Long, OpStatus> pendingOps = shortNodeIdToPendingOps.get(VersionUtil.extractShortNodeId(timestamp));
        //Note: We can get applied ops that weren't pending because they don't have deps
        if (pendingOps == null) {
            pendingOps = new TreeMap<Long, OpStatus>();
            shortNodeIdToPendingOps.put(VersionUtil.extractShortNodeId(timestamp), pendingOps);
        }

        pendingOps.put(timestamp, OpStatus.APPLIED);

        //remove all but one of the prefix of pendingOps that have been applied and are older than the oldest entry in threadIdToNewestOp
        if (pendingOps.size() > 1) {
            Iterator<Entry<Long, OpStatus>> firstNeededIt = pendingOps.entrySet().iterator();
            Entry<Long, OpStatus> firstNeededOp = firstNeededIt.next();
            Long lastAppliedKey;
            if (firstNeededOp.getValue() == OpStatus.PENDING) {
                lastAppliedKey = null;
            } else {
                lastAppliedKey = firstNeededOp.getKey();
            }
            Long oldestNewestOp = oldestNewestOp();
            while (firstNeededIt.hasNext()) {
                firstNeededOp = firstNeededIt.next();
                if (firstNeededOp.getValue() == OpStatus.PENDING) {
                    break;
                } else if (firstNeededOp.getKey() > oldestNewestOp) {
                    break;
                } else {
                    lastAppliedKey = firstNeededOp.getKey();
                }
            }

            if (lastAppliedKey != null) {
                for (Iterator<Entry<Long, OpStatus>> opIt = pendingOps.entrySet().iterator(); opIt.hasNext(); ) {
                    Entry<Long, OpStatus> op = opIt.next();
                    if (opIt.hasNext() && op.getKey() != lastAppliedKey) {
                        opIt.remove();
                    } else {
                        break;
                    }
                }
            }
        }

        //respond to any blocked dep checks on this op
        Queue<DepCheckReplyInfo> blockedQueue = blockedDepChecks.get(timestamp);
        if (blockedQueue != null) {
            for (DepCheckReplyInfo dcri : blockedQueue) {
                try {
                    sendDepCheckReply(dcri.getMessage(), dcri.getId(), dcri.getLocatorKey());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static synchronized void checkDependency(DependencyCheck depCheck, Message depCheckMessage, String id)
    {
        // now depcheck has a list of dependencies to check, we iterate through them
        for (Dependency dep : depCheck.getDependencies()) {
            // Don't check dependencies for values written in this DC, we know they've been applied
            if (VersionUtil.extractDatacenter(dep.getTimestamp()) == ShortNodeId.getLocalDC()) {
                continue;
            }
            ByteBuffer locatorKey = dep.getLocatorKey();
            SortedMap<Long, OpStatus> pendingOps = shortNodeIdToPendingOps.get(VersionUtil.extractShortNodeId(dep.getTimestamp()));
            if (pendingOps == null || pendingOps.size() == 0) {
                //no pendingOps => nothing's been received from that node yet
                blockDepCheck(dep.getTimestamp(), depCheckMessage, id, locatorKey);
            }
            else if (pendingOps.get(pendingOps.firstKey()) == OpStatus.PENDING) {
                //first op pending => nothing's been applied yet
                blockDepCheck(dep.getTimestamp(), depCheckMessage, id, locatorKey);
            }
            else if (dep.getTimestamp() <= pendingOps.firstKey()) {
                //firstKey and everything older than it have been applied => respond immediately
                try {
                    sendDepCheckReply(depCheckMessage, id, locatorKey);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            else if (dep.getTimestamp() > pendingOps.lastKey()) {
                //lastKey is the mostly recently received op from this node => hasn't been applied yet
                blockDepCheck(dep.getTimestamp(), depCheckMessage, id, locatorKey);
            }
            else {
                OpStatus opStatus = pendingOps.get(dep.getTimestamp());
                if (opStatus == null || opStatus == OpStatus.PENDING) {
                    blockDepCheck(dep.getTimestamp(), depCheckMessage, id, locatorKey);
                } else {
                    try {
                        sendDepCheckReply(depCheckMessage, id, locatorKey);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static void blockDepCheck(long timestamp, Message depCheckMessage, String id, ByteBuffer locatorKey)
    {
        logger.debug("Block dependency check. (dcm.lt={})", depCheckMessage.getLamportTimestamp());

        //TODO add timeouts to cause an error if they are never satisfied

        Queue<DepCheckReplyInfo> blockedQueue = blockedDepChecks.get(timestamp);
        if (blockedQueue == null) {
            blockedQueue = new LinkedList<DepCheckReplyInfo>();
            blockedDepChecks.put(timestamp, blockedQueue);
        }
        blockedQueue.add(new DepCheckReplyInfo(depCheckMessage, id, locatorKey));
    }

    //HL: check and pass along read-only transaction ids when sending dep_check_response back
    private static void sendDepCheckReply(Message depCheckMessage, String id, ByteBuffer locatorKey) throws IOException
    {
        logger.debug("Send dependency check reply. (dcm.lt={})", depCheckMessage.getLamportTimestamp());

        //HL: now before sending back dep_check response, we find all the txnIds associated with this
        //locator key, and put this list of ids into message body to be sent back
        ArrayList<Long> txnIdList = new ArrayList<Long>();
        txnIdList.addAll(ReadTransactionIdTracker.getReadTxnIds(locatorKey));
        //convert txnIds to byte array and pass it into dep_check reply message
        int size = DBConstants.intSize + txnIdList.size() * DBConstants.longSize;
        DataOutputBuffer buffer = new DataOutputBuffer(size);
        buffer.writeInt(txnIdList.size());
        for (Long txnId : txnIdList) {
            buffer.writeLong(txnId);
        }
        /* Comment out Eiger's code
        byte[] empty = new byte[0];
        Message reply = depCheckMessage.getReply(FBUtilities.getBroadcastAddress(), empty, depCheckMessage.getVersion());
        MessagingService.instance().sendReply(reply, id, depCheckMessage.getFrom());
        */
        Message reply = depCheckMessage.getReply(FBUtilities.getBroadcastAddress(), buffer.getData(), depCheckMessage.getVersion());
        MessagingService.instance().sendReply(reply, id, depCheckMessage.getFrom());
    }

    private static void sendTxnIdsBack(Message fetchIdMessage, String id, Set<ByteBuffer> keyList) throws IOException
    {
        logger.debug("Send dependency check reply. (dcm.lt={})", fetchIdMessage.getLamportTimestamp());

        //HL: now before sending back fetchId response, we find all the txnIds associated with this
        //locator key, and put this list of ids into message body to be sent back
        ArrayList<Long> subTxnIdList = new ArrayList<Long>();
        Map<ByteBuffer, ArrayList<Long>> totalIdList = new HashMap<ByteBuffer, ArrayList<Long>>();
        int numKeys = keyList.size();
        int numIds = 0;
        for (ByteBuffer key : keyList) {
            subTxnIdList = ReadTransactionIdTracker.getReadTxnIds(key);
            numIds += subTxnIdList.size();
            totalIdList.put(key, subTxnIdList);
        }
        int size = (numKeys + 1) * DBConstants.intSize + DBConstants.shortSize * numKeys + numIds * DBConstants.longSize;
        //convert txnIds to byte array and pass it into dep_check reply message
        DataOutputBuffer buffer = new DataOutputBuffer(size);
        buffer.writeInt(numKeys);
        for (Map.Entry<ByteBuffer, ArrayList<Long>> keyids : totalIdList.entrySet()) {
            buffer.writeInt(keyids.getValue().size());
            ByteBufferUtil.writeWithShortLength(keyids.getKey(), buffer);
            for (Long eachid : keyids.getValue()) {
                buffer.writeLong(eachid);
            }
        }
        byte[] compressed = Snappy.compress(buffer.getData());
        Message reply = fetchIdMessage.getReply(FBUtilities.getBroadcastAddress(), compressed, fetchIdMessage.getVersion());
        MessagingService.instance().sendReply(reply, id, fetchIdMessage.getFrom());

        totalIdList.clear();
        totalIdList = null;
        subTxnIdList.clear();
        subTxnIdList = null;
    }

    // Use this instead of fakedepcheckreply
    public static void sendFetchTxnIdsReply(FetchTxnIds fetchId, Message fetchIdMessage, String id) {
        Set<ByteBuffer> keyList = new HashSet<ByteBuffer>();
        keyList = fetchId.getInquiryKeys();
        try {
            sendTxnIdsBack(fetchIdMessage, id, keyList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
