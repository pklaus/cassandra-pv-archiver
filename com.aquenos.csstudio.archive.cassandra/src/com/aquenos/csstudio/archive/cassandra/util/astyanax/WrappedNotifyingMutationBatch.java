/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.util.astyanax;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.WriteAheadLog;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;

/**
 * Simple implementation of a {@link NotifyingMutationBatch} that simply wraps a
 * normal {@link MutationBatch} and generates events by overriding the
 * {@link #execute()} and {@link #discardMutations()} methods.
 * 
 * @author Sebastian Marsching
 */
public class WrappedNotifyingMutationBatch implements NotifyingMutationBatch {

    private final static Object FLAG_OBJECT = new Object();

    private MutationBatch wrappedMutationBatch;
    private HashSet<MutationBatchListener> temporaryListeners = new HashSet<NotifyingMutationBatch.MutationBatchListener>();
    private HashSet<MutationBatchListener> permanentListeners = new HashSet<NotifyingMutationBatch.MutationBatchListener>();

    ThreadLocal<Object> executeFlag = new ThreadLocal<Object>();

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Creates a {@link NotifyingMutationBatch} that wraps the provided
     * {@link MutationBatch}.
     * 
     * @param wrappedMutationBatch
     *            mutation batch to be wrapped.
     */
    public WrappedNotifyingMutationBatch(MutationBatch wrappedMutationBatch) {
        this.wrappedMutationBatch = wrappedMutationBatch;
    }

    @Override
    public <K, C> ColumnListMutation<C> withRow(
            ColumnFamily<K, C> columnFamily, K rowKey) {
        return wrappedMutationBatch.withRow(columnFamily, rowKey);
    }

    @Override
    public <K> void deleteRow(Collection<ColumnFamily<K, ?>> columnFamilies,
            K rowKey) {
        wrappedMutationBatch.deleteRow(columnFamilies, rowKey);
    }

    @Override
    public void discardMutations() {
        try {
            wrappedMutationBatch.discardMutations();
        } finally {
            if (executeFlag.get() == null) {
                // The execute flag is not set, so this has not been called from
                // #execute().
                for (MutationBatchListener listener : permanentListeners) {
                    try {
                        listener.afterDiscard(this);
                    } catch (Exception e) {
                        logger.error(
                                "Error while executing mutation batch listener: "
                                        + e.getMessage(), e);
                    }
                }
                for (MutationBatchListener listener : temporaryListeners) {
                    try {
                        listener.afterDiscard(this);
                    } catch (Exception e) {
                        logger.error(
                                "Error while executing mutation batch listener: "
                                        + e.getMessage(), e);
                    }
                }
                temporaryListeners.clear();
            }
        }
    }

    @Override
    public void mergeShallow(MutationBatch other) {
        wrappedMutationBatch.mergeShallow(other);
    }

    @Override
    public boolean isEmpty() {
        return wrappedMutationBatch.isEmpty();
    }

    @Override
    public int getRowCount() {
        return wrappedMutationBatch.getRowCount();
    }

    @Override
    public Map<ByteBuffer, Set<String>> getRowKeys() {
        return wrappedMutationBatch.getRowKeys();
    }

    @Override
    public NotifyingMutationBatch pinToHost(Host host) {
        wrappedMutationBatch.pinToHost(host);
        return this;
    }

    @Override
    public NotifyingMutationBatch setConsistencyLevel(
            ConsistencyLevel consistencyLevel) {
        wrappedMutationBatch.setConsistencyLevel(consistencyLevel);
        return this;
    }

    @Override
    public NotifyingMutationBatch withConsistencyLevel(
            ConsistencyLevel consistencyLevel) {
        wrappedMutationBatch.withConsistencyLevel(consistencyLevel);
        return this;
    }

    @Override
    public NotifyingMutationBatch withRetryPolicy(RetryPolicy retry) {
        wrappedMutationBatch.withRetryPolicy(retry);
        return this;
    }

    @Override
    public NotifyingMutationBatch usingWriteAheadLog(WriteAheadLog manager) {
        wrappedMutationBatch.usingWriteAheadLog(manager);
        return this;
    }

    @Override
    public NotifyingMutationBatch lockCurrentTimestamp() {
        wrappedMutationBatch.lockCurrentTimestamp();
        return this;
    }

    @SuppressWarnings("deprecation")
    @Override
    public NotifyingMutationBatch setTimeout(long timeout) {
        wrappedMutationBatch.setTimeout(timeout);
        return this;
    }

    @Override
    public NotifyingMutationBatch setTimestamp(long timestamp) {
        wrappedMutationBatch.setTimestamp(timestamp);
        return this;
    }

    @Override
    public NotifyingMutationBatch withTimestamp(long timestamp) {
        wrappedMutationBatch.withTimestamp(timestamp);
        return this;
    }

    @Override
    public ByteBuffer serialize() throws Exception {
        return wrappedMutationBatch.serialize();
    }

    @Override
    public void deserialize(ByteBuffer data) throws Exception {
        wrappedMutationBatch.deserialize(data);
    }

    @Override
    public OperationResult<Void> execute() throws ConnectionException {
        boolean success = false;
        try {
            executeFlag.set(FLAG_OBJECT);
            OperationResult<Void> result = wrappedMutationBatch.execute();
            success = true;
            return result;
        } finally {
            executeFlag.remove();
            for (MutationBatchListener listener : permanentListeners) {
                try {
                    listener.afterExecute(this, success);
                } catch (Exception e) {
                    logger.error(
                            "Error while executing mutation batch listener: "
                                    + e.getMessage(), e);
                }
            }
            for (MutationBatchListener listener : temporaryListeners) {
                try {
                    listener.afterExecute(this, success);
                } catch (Exception e) {
                    logger.error(
                            "Error while executing mutation batch listener: "
                                    + e.getMessage(), e);
                }
            }
            temporaryListeners.clear();
        }
    }

    @Override
    public Future<OperationResult<Void>> executeAsync()
            throws ConnectionException {
        return wrappedMutationBatch.executeAsync();
    }

    @Override
    public void addListener(MutationBatchListener listener, boolean permanent) {
        if (permanent) {
            temporaryListeners.remove(listener);
            permanentListeners.add(listener);
        } else {
            permanentListeners.remove(listener);
            temporaryListeners.add(listener);
        }
    }

    @Override
    public void removeListener(MutationBatchListener listener) {
        permanentListeners.remove(listener);
        temporaryListeners.remove(listener);
    }

}
