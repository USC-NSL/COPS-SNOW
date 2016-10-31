package org.apache.cassandra.db;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.xerial.snappy.Snappy;

/**
 * Created by yangyang333 on 15-7-8.
 */
public class FetchTxnIdsVerbHandler implements IVerbHandler {
    private static Logger logger_ = LoggerFactory.getLogger(FetchTxnIdsVerbHandler.class);

    @Override
    public void doVerb(Message message, String id)
    {
        FetchTxnIds fetchId = null;
        try {
            byte[] raw_msg = Snappy.uncompress(message.getMessageBody());
            fetchId = FetchTxnIds.fromBytes(raw_msg, message.getVersion());
        } catch (IOException e) {
            logger_.error("Error in decoding fetchTxnIds");
        }
        AppliedOperations.sendFetchTxnIdsReply(fetchId, message, id);
    }
}
