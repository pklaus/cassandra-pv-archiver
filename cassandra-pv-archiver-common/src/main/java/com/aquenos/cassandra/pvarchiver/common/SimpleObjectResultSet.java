/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.common;

import java.util.Iterator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Simple {@link ObjectResultSet} implementation. This implementation simply
 * returns the elements from a single iterator (or iterable).
 * </p>
 * 
 * <p>
 * Like indicated for the {@link ObjectResultSet} interface, this class is
 * <em>not</em> thread safe.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 * @param <V>
 *            type of the objects contained in the result set.
 */
public class SimpleObjectResultSet<V> extends AbstractObjectResultSet<V> {

    /**
     * Creates a result set that contains the objects from the specified
     * iterable. This does the same as calling
     * <code>fromIterator(iterable.iterator())</code>.
     * 
     * @param <V>
     *            type of the objects contained in the result set.
     * @param iterable
     *            iterable with the objects that will be returned by this result
     *            set.
     * @return newly created result set.
     * @throws NullPointerException
     *             if <code>iterable</code> is <code>null</code>.
     */
    public static <V> SimpleObjectResultSet<V> fromIterable(Iterable<V> iterable) {
        return new SimpleObjectResultSet<V>(iterable.iterator());
    }

    /**
     * Creates a result set that contains the objects returned by the specified
     * iterator. If the iterator is <code>null</code>, the result set is empty.
     * This is a convenience method calling the Simple
     * 
     * @param <V>
     *            type of the objects contained in the result set.
     * @param iterator
     *            iterator returning the objects that will be returned by this
     *            result set.
     * @return newly created result set.
     */
    public static <V> SimpleObjectResultSet<V> fromIterator(Iterator<V> iterator) {
        return new SimpleObjectResultSet<V>(iterator);
    }

    private Iterator<V> iterator;

    private SimpleObjectResultSet(Iterator<V> iterator) {
        this.iterator = iterator;
    }

    @Override
    protected ListenableFuture<Iterator<V>> fetchNextPage() {
        Iterator<V> returnValue = this.iterator;
        this.iterator = null;
        return Futures.immediateFuture(returnValue);
    }

}
