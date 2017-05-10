/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.common;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
     * Sized iterator that iterates over a collection.
     * 
     * @author Sebastian Marsching
     *
     * @param <E>
     *            the type of elements returned by this iterator.
     */
    private static class SizedCollectionIterator<E>
            implements SizedIterator<E> {

        private final Iterator<E> iterator;
        private int remaining;
        private Throwable savedThrowable;

        public SizedCollectionIterator(Collection<E> collection) {
            this.iterator = collection.iterator();
            this.remaining = collection.size();
        }

        @Override
        public boolean hasNext() {
            return remaining != 0;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (savedThrowable != null) {
                throw new RuntimeException(
                        "A previous call to next() failed, cannot continue.",
                        savedThrowable);
            }
            // If the collection's iterator's next() method throws an exception,
            // we cannot know for sure whether the element has been consumed.
            // This means that we do not know whether we have to decrement the
            // number of remaining elements. The only safe way to go forward is
            // failing all future calls to next(). This is somewhat consistent
            // to what we do when we create the result set with an iterator,
            // because if that iterator throws an exception, the result set will
            // not contain any elements and its methods related to fetching
            // samples will throw an exception.
            E element;
            try {
                element = iterator.next();
            } catch (Error | RuntimeException e) {
                savedThrowable = e;
                throw e;
            }
            --remaining;
            return element;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int remainingCount() {
            return remaining;
        }

    }

    /**
     * Creates a result set that contains the objects from the specified
     * iterable. This is similar to calling
     * <code>fromIterator(iterable.iterator())</code>, but more efficient
     * because the {@link #fetchNextPage()} method will provide a
     * {@link SizedIterator}.
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
    public static <V> SimpleObjectResultSet<V> fromIterable(
            Iterable<V> iterable) {
        if (iterable instanceof Collection<?>) {
            Collection<V> collection = (Collection<V>) iterable;
            if (collection.size() == Integer.MAX_VALUE) {
                // If the collection contains Integer.MAX_VALUE (or more)
                // elements, we cannot know the number of contained elements for
                // sure. For this reason, we have to fall back to using a simple
                // iterator. This scenario is extremely unlikely, but it still
                // needs to be handled correctly.
                return new SimpleObjectResultSet<>(iterable.iterator());
            } else {
                return new SimpleObjectResultSet<>(
                        new SizedCollectionIterator<>(collection));
            }
        } else {
            return new SimpleObjectResultSet<>(iterable.iterator());
        }
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
    public static <V> SimpleObjectResultSet<V> fromIterator(
            Iterator<V> iterator) {
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
