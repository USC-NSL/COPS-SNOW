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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.LamportClock;
import org.apache.cassandra.utils.ShortNodeId;
import org.apache.cassandra.utils.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowMutationVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(RowMutationVerbHandler.class);

    @Override
    public void doVerb(Message message, String id)
    {
        //HL: When we receive a replicated mutation, record current version on this server used for
        //read by time later if needed
        long chosenTime = LamportClock.currentVersion();
        try
        {
            RowMutation rm = RowMutation.fromBytes(message.getMessageBody(), message.getVersion());
            if (logger_.isDebugEnabled())
              logger_.debug("Deserialized " + rm);

            // Check if there were any forwarding headers in this message
            byte[] forwardBytes = message.getHeader(RowMutation.FORWARD_HEADER);
            if (forwardBytes != null && message.getVersion() >= MessagingService.VERSION_11)
                forwardToLocalNodes(message, forwardBytes);

            //Check Dependencies
            assert VersionUtil.extractDatacenter(rm.extractTimestamp()) != ShortNodeId.getLocalDC() : "Do not expect replication mutations from the localDC (yet)";
            if (rm.getDependencies().size() > 0) {
                // If we check dependencies, the final response will call applyAndRespond
                StorageProxy.checkDependencies(rm.getTable(), rm.key(), rm.extractTimestamp(), rm.getDependencies(), new RowMutationCompletion(message, id, rm), chosenTime);
            } else {
                // No deps to check, applyAndRespond immediately
                applyAndRespond(message, id, rm);
            }
        }
        catch (IOException e)
        {
            logger_.error("Error in row mutation", e);
        }
    }

    protected void applyAndRespond(Message message, String id, RowMutation rm)
    {
        try
        {
            assert message != null && id != null && rm != null : message + ", " + id + ", " + rm;

            if (logger_.isDebugEnabled())
                logger_.debug("Applying " + rm);

            rm.apply();

            WriteResponse response = new WriteResponse(rm.getTable(), rm.key(), true);
            Message responseMessage = WriteResponse.makeWriteResponseMessage(message, response);
            if (logger_.isDebugEnabled())
                logger_.debug(rm + " applied.  Sending response to " + id + "@" + message.getFrom());
            MessagingService.instance().sendReply(responseMessage, id, message.getFrom());
        }
        catch (IOException e)
        {
            logger_.error("Error in row mutation", e);
        }
    }

    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private void forwardToLocalNodes(Message message, byte[] forwardBytes) throws IOException
    {
        DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(forwardBytes));
        int size = dis.readInt();

        // remove fwds from message to avoid infinite loop
        Message messageCopy = message.withHeaderRemoved(RowMutation.FORWARD_HEADER);
        for (int i = 0; i < size; i++)
        {
            // Send a message to each of the addresses on our Forward List
            InetAddress address = CompactEndpointSerializationHelper.deserialize(dis);
            String id = dis.readUTF();
            if (logger_.isDebugEnabled())
                logger_.debug("Forwarding message to " + address + " with= ID: " + id);
            // Let the response go back to the coordinator
            MessagingService.instance().sendOneWay(messageCopy, id, address);
        }
    }

    private static class RMVHHandle
    {
        public static final RowMutationVerbHandler instance = new RowMutationVerbHandler();
    }

    public static RowMutationVerbHandler instance()
    {
        return RMVHHandle.instance;
    }
}