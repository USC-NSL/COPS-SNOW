package org.apache.cassandra.service;

import org.apache.cassandra.db.Dependency;
import org.apache.cassandra.db.ReadTransactionIdTracker;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.xerial.snappy.Snappy;

/**
 * Created by yangyang333 on 15-7-9.
 */
public class FetchTxnIdsCallBack implements IAsyncCallback {
    private static Logger logger_ = LoggerFactory.getLogger(FetchTxnIdsCallBack.class);

    //private final long startTime;
    private int responses = 0;
    private Map<ByteBuffer, HashSet<ByteBuffer>> mutationMap;
    private final ICompletable completable;
    //update txnid list
    private final long chosenTime;          //HL: used to query read by time
    private final int numEP;		    //HL: number of endpoints

    public FetchTxnIdsCallBack(ICompletable completable, Map<ByteBuffer, HashSet<ByteBuffer>> mutationMap, long chosenTime, int numEP)
    {
        this.mutationMap = mutationMap;
        this.completable = completable;
        this.chosenTime = chosenTime;
        this.numEP = numEP;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        //not on the read path
        return false;
    }

    @Override
    synchronized public void response(Message msg)
    {
        responses++;

        assert responses > 0 &&  responses <= numEP : responses + "?" + numEP;

        //extract txnId List
        try {
            updateTxnIdList(msg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (responses == numEP) {
            completable.complete();
        }
    }

    public void updateTxnIdList(Message msg) throws IOException{
        byte[] uncompressed = Snappy.uncompress(msg.getMessageBody());
        ByteArrayInputStream inputByteStream = new ByteArrayInputStream(uncompressed);
        DataInputStream inputStream = new DataInputStream(inputByteStream);
        int numKeys = inputStream.readInt();
        // if a write keeps a version of 0, we modify it to 1 to be differentiated from reads, not sure necessary or
        // not, just for safety.
        long useChosenTime = chosenTime;
        if (chosenTime == 0) {
            useChosenTime = 1L;
        }
        Map<ByteBuffer, ArrayList<Long>> returnedIdsMap = new HashMap<ByteBuffer, ArrayList<Long>>();
        for (int i = 0; i < numKeys; ++i) {
            int blockSize = inputStream.readInt();
            ByteBuffer depKey = ByteBufferUtil.readWithShortLength(inputStream);
            ArrayList<Long> idList = new ArrayList<Long>();
            for (int x = 0; x < blockSize; ++x) {
                long txnId = inputStream.readLong();
                idList.add(txnId);
            }
            for (Map.Entry<ByteBuffer, HashSet<ByteBuffer>> entry : mutationMap.entrySet()) {
                ByteBuffer locatorKey = entry.getKey();
                if (entry.getValue().contains(depKey)) {
                    if (returnedIdsMap.get(locatorKey) == null) {
                        returnedIdsMap.put(locatorKey, idList);
                    } else {
                        returnedIdsMap.get(locatorKey).addAll(idList);
                    }
                }
            }
            idList.clear();
        }
        for (Map.Entry<ByteBuffer, ArrayList<Long>> entry : returnedIdsMap.entrySet()) {
            ReadTransactionIdTracker.checkIfTxnIdBeenRecorded(entry.getKey(), entry.getValue(), useChosenTime);
        }
        returnedIdsMap.clear();
        returnedIdsMap = null;
    }
}
