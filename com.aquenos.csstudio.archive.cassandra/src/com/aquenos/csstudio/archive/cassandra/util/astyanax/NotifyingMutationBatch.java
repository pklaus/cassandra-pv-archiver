/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.util.astyanax;

import com.netflix.astyanax.MutationBatch;

/**
 * Extended mutation batch version that allows to register listeners that are
 * notified when the mutation batch is executed or discarded.
 * 
 * @author Sebastian Marsching
 */
public interface NotifyingMutationBatch extends MutationBatch {

    /**
     * Event listener for a mutation batch. The listener is notified when
     * certain events happen on the mutation batch it is registered with.
     */
    public static interface MutationBatchListener {
        /**
         * Event handler called after the mutation batch has been executed.
         * 
         * @param source
         *            mutation batch that triggered this event.
         * @param success
         *            <code>true</code> if and only if the execution was
         *            successful (did not throw an exception).
         */
        void afterExecute(NotifyingMutationBatch source, boolean success);

        /**
         * Event handler called after the mutations in the mutation batch have
         * been discarded. This event handler is not called when the mutations
         * are discarded after being executed as part of the regular execution
         * process.
         * 
         * @param source
         *            mutation batch that triggered this event.
         */
        void afterDiscard(NotifyingMutationBatch source);
    }

    /**
     * Adds a listener to this mutation batch. If this listener has already been
     * added, it will not be added a second time. However, if the
     * <code>permanent</code> flag is different than the last time, the type of
     * the registration will be changed.
     * 
     * @param listener
     *            mutation batch listener to register with this mutation batch.
     * @param permanent
     *            if <code>true</code> the listener will stay registered after
     *            the mutation batch has been executed or discarded. If
     *            <code>false</code> the listener is automatically unregistered
     *            when the mutation batch is executed or discarded.
     */
    void addListener(MutationBatchListener listener, boolean permanent);

    /**
     * Removes a listener that has been registered previously. If the listener
     * is not registered, nothing will happen.
     * 
     * @param listener
     *            mutation batch listener to unregister from this mutation
     *            batch.
     */
    void removeListener(MutationBatchListener listener);

}
