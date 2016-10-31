package org.apache.cassandra.db;

import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.service.IWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.Pair;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Created by yangyang333 on 15-7-9.
 */
public class FetchIdCompletion implements ICompletable {
    private final List<Pair<RowMutation, IWriteResponseHandler>> rowMutations;

    public FetchIdCompletion(List<Pair<RowMutation, IWriteResponseHandler>> rms) {
        this.rowMutations = rms;
    }
    @Override
    public void complete() {
        for (Pair<RowMutation, IWriteResponseHandler> rm : rowMutations) {
            StorageProxy.insertLocal(rm.left, rm.right);
        }
    }
}
