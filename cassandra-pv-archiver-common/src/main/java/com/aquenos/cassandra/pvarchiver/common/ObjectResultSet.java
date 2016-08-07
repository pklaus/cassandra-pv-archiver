/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 * 
 * This interface is based on the concepts from the "ResultSet" interface
 * Copyright (C) 2012-2015 DataStax Inc.
 */

package com.aquenos.cassandra.pvarchiver.common;

import java.util.Iterator;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Set of objects that represent the result of a query. This class closely
 * resembles Cassandra's <code>ResultSet</code>. Basically, it provides the same
 * methods but deals with typed objects instead of CQL rows.
 * </p>
 * 
 * <p>
 * This design is intended to make it very easy to wrap a <code>ResultSet</code>
 * in a <code>ObjectResultSet</code> while retaining the possibility to
 * implement a <code>ObjectResultSet</code> that is not based on a
 * <code>ResultSet</code>.
 * </p>
 *
 * <p>
 * Implementations of this interface have to be expected to be <em>not</em>
 * thread safe.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 * @param <V>
 *            type of the object contained in the result set.
 */
public interface ObjectResultSet<V> extends Iterable<V> {

    /**
     * <p>
     * Simple implementation of an iterator that consumes a object result-set.
     * This class can be used for an iterator returned by the
     * {@link ObjectResultSet#iterator()} method.
     * </p>
     * 
     * <p>
     * The {@link #hasNext()} method returns
     * <code>!objectResultSet.isExhausted()</code>. The {@link #next()} method
     * delegates to and returns the value of <code>objectResultSet.one()</code>.
     * The {@link #remove()} throws an {@link UnsupportedOperationException}.
     * </p>
     * 
     * <p>
     * As the {@link ObjectResultSet#isExhausted()} and
     * {@link ObjectResultSet#one()} methods may block, the iterator may block.
     * </p>
     * 
     * @author Sebastian Marsching
     *
     * @param <V>
     *            type of the objects returns by the iterator.
     */
    public static class ObjectResultSetIterator<V> implements Iterator<V> {

        private ObjectResultSet<? extends V> objectResultSet;

        /**
         * Creates an iterator that iterates over the specified object
         * result-set, consuming the objects in the set.
         * 
         * @param objectResultSet
         *            object result-set over which the iterator iterates. The
         *            iterator only uses the set's <code>isExhausted()</code>
         *            and <code>one()</code> methods.
         */
        public ObjectResultSetIterator(
                ObjectResultSet<? extends V> objectResultSet) {
            this.objectResultSet = objectResultSet;
        }

        @Override
        public boolean hasNext() {
            return !objectResultSet.isExhausted();
        }

        @Override
        public V next() {
            return objectResultSet.one();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(
                    "This iterator does not support removing elements.");
        }

    }

    /**
     * <p>
     * Returns all remaining objects in this object result-set as a list.
     * </p>
     * 
     * <p>
     * Calls to this method will block until all objects have been retrieved
     * from the database.
     * </p>
     * 
     * <p>
     * Note that, contrary to {@code iterator()} or successive calls to
     * {@code one()}, this method forces fetching the full content of the object
     * result-set at once, holding it all in memory in particular. It is thus
     * recommended to prefer iterations through {@code iterator()} when
     * possible, especially if the result set can be big.
     * </p>
     * 
     * @return list containing the remaining objects of this object result set.
     *         The returned list is empty if and only this result set is
     *         exhausted. This result set will be exhausted after a call to this
     *         method.
     */
    List<V> all();

    /**
     * <p>
     * Forces fetching the next page of object for this object result-set.
     * </p>
     * 
     * <p>
     * The use of this method is entirely optional. It will be called
     * automatically while the result set is consumed (through {@link #one()},
     * {@link #all()} or iteration) when needed (i.e. when
     * <code>getAvailableWithoutFetching() == 0</code> and
     * <code>isFullyFetched() == false</code>).
     * </p>
     * 
     * <p>
     * You can however call this method manually to force the fetching of the
     * next page of objects. This can allow to prefetch objects before they are
     * strictly needed.
     * </p>
     * 
     * <p>
     * This method is not blocking. However, <code>hasNext()</code> on the
     * iterator returned by {@link #iterator()} or calling {@link #one()} will
     * block until the fetch query returns if all objects that have been fetched
     * previously have already been returned.
     * </p>
     * 
     * <p>
     * Only one page of objects (for a given result set) can be fetched at any
     * given time. If this method is called twice and the query triggered by the
     * first call has not returned yet when the second one is performed, then
     * the second call will simply return a future on the currently in progress
     * query.
     * </p>
     *
     * @return future on the completion of fetching the next page of objects. If
     *         the result set is already fully retrieved (
     *         <code>isFullyFetched() == true</code>), then the returned future
     *         will return immediately (you should thus call
     *         {@link #isFullyFetched()} to know if calling this method can be
     *         of any use}).
     */
    ListenableFuture<Void> fetchMoreResults();

    /**
     * Returns the number of objects that can be retrieved from this object
     * result-set without blocking to fetch.
     *
     * @return number of objects readily available in this object result-set. If
     *         {@link #isFullyFetched()}, this is the total number of objects
     *         remaining in this result set (after which the result set will be
     *         exhausted).
     */
    int getAvailableWithoutFetching();

    /**
     * <p>
     * Tells whether this object result-set has more objects.
     * </p>
     * 
     * <p>
     * Calls to this method may block if
     * <code>getAvailableWithoutFetching() == 0</code> and
     * <code>isFullyFetched() == false</code>.
     * </p>
     * 
     * @return <code>true</code> if this object result-set is exhausted (
     *         <code>one() == null</code>), <code>false</code> if there are
     *         objects left in this result set.
     */
    boolean isExhausted();

    /**
     * <p>
     * Tells whether all objects from this object result set have been fetched
     * from the database.
     * </p>
     * 
     * <p>
     * Note that if this method returns <code>true</code>, then
     * {@link #getAvailableWithoutFetching} will return how many objects remain
     * in the result set before exhaustion. However, if this method returns
     * <code>false</code>, this does not guarantee that the result set is not
     * exhausted (you should call {@link #isExhausted()} to verify it).
     *
     * @return <code>true</code> if all objects have been fetched,
     *         <code>false</code> if there might be more objects that have not
     *         been loaded from the database yet.
     */
    boolean isFullyFetched();

    /**
     * <p>
     * Returns an iterator over the objects contained in this object result-set.
     * </p>
     *
     * <p>
     * Calls to the {@link Iterator#hasNext()} method may block if
     * <code>getAvailableWithoutFetching() == 0</code> and
     * <code>isFullyFetched() == false</code>.
     * </p>
     *
     * <p>
     * The {@link Iterator#next()} method is equivalent to calling
     * {@link #one()}. So this iterator will consume objects from this result
     * set and after a full iteration, the result set will be empty.
     * </p>
     *
     * <p>
     * The returned iterator does not support the {@link Iterator#remove()}
     * method.
     * </p>
     *
     * @return iterator that will consume and return the remaining objects of
     *         this object result-set.
     */
    @Override
    Iterator<V> iterator();

    /**
     * <p>
     * Returns the next object from this object result-set.
     * </p>
     * 
     * <p>
     * Calls to this method may block if
     * <code>getAvailableWithoutFetching() == 0</code> and
     * <code>isFullyFetched() == false</code>.
     * </p>
     *
     * @return next object in this object result-set or <code>null</code> if
     *         this result set is exhausted.
     */
    V one();

}
