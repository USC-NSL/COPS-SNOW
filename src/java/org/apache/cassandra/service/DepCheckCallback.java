package org.apache.cassandra.service;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Set;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.db.Dependency;
import org.apache.cassandra.db.ReadTransactionIdTracker;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.apache.hadoop.io.DataInputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.io.util.*;

public class DepCheckCallback implements IAsyncCallback
{
    private static Logger logger_ = LoggerFactory.getLogger(DepCheckCallback.class);

    private final long startTime;
    private int responses = 0;
    //private final Set<Dependency> deps;
    private final ICompletable completable;
    private final ByteBuffer locatorKey;    //HL: keep locator key in call back so we can use later for
    //update txnid list
    private final long chosenTime;          //HL: used to query read by time
    private final int numEP;		    //HL: number of endpoints
    //HL: now this callback object also takes locatorKey
    public DepCheckCallback(ICompletable completable, ByteBuffer locatorKey, long chosenTime, int numEP)
    {
        //this.deps = deps;
        this.startTime = System.currentTimeMillis();
        this.completable = completable;
        this.locatorKey = locatorKey;
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
    /*
     * HL: Process dep_check reply messages. For each reply, we extract txnId list from
     * message body, and add it to this server's txnIdList, fill in corresponding txn's
     * effective time with this write's commit time.
     */
    synchronized public void response(Message msg)
    {
        responses++;

        assert responses > 0 && responses <= numEP : responses + "?" + numEP;

        //extract txnId List
        try {
            updateTxnIdList(msg, locatorKey, chosenTime);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (responses == numEP) {
            completable.complete();
        }
    }

    //HL: for each dep_check reply, we update txnId List.
    //locatorKey is the key associated with the write (all deps for)
    public void updateTxnIdList(Message msg, ByteBuffer locatorKey, long chosenTime) throws IOException{
        ByteArrayInputStream inputByteStream = new ByteArrayInputStream(msg.getMessageBody());
        DataInputStream inputStream = new DataInputStream(inputByteStream);
        int numIds = inputStream.readInt();
        // if a write keeps a version of 0, we modify it to 1 to be differentiated from reads, not sure necessary or
        // not, just for safety.
        if (chosenTime == 0) {
            chosenTime = 1L;
        }
        ArrayList<Long> txnIdList = new ArrayList<Long>();
        while (inputStream.available() > 0) {
            txnIdList.add(inputStream.readLong());
        }
        ReadTransactionIdTracker.checkIfTxnIdBeenRecorded(locatorKey, txnIdList, chosenTime);
        txnIdList.clear();;
        txnIdList = null;
    }

    //WL TODO: Add a timeout that fires and complains if this hangs
}