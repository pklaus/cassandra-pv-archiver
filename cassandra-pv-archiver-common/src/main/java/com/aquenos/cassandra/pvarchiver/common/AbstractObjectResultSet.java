/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.common;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Abstract base class for classes that implement the {@link ObjectResultSet}
 * interface. This base class handles most of the complexity involved in writing
 * an implementation of {@link ObjectResultSet}. Child classes only have to
 * implement the {@link #fetchNextPage()} method for a complete implementation
 * of the {@link ObjectResultSet} interface.
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
public abstract class AbstractObjectResultSet<V> implements ObjectResultSet<V> {

    /**
     * Sized iterator that iterates over a collection and removes its elements.
     * This iterator only works if the iterator provided by the collection
     * implements the <code>remove()</code> method.
     * 
     * @author Sebastian Marsching
     *
     * @param <E>
     *            the type of elements returned by this iterator.
     */
    private static class ConsumingSizedCollectionIterator<E>
            implements SizedIterator<E> {

        private final Collection<E> collection;
        private final Iterator<E> iterator;

        public ConsumingSizedCollectionIterator(Collection<E> collection) {
            this.collection = collection;
            this.iterator = this.collection.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public E next() {
            E element = iterator.next();
            iterator.remove();
            return element;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int remainingCount() {
            return collection.size();
        }

    }

    private static final Function<Object, Void> ANY_TO_VOID = new Function<Object, Void>() {
        @Override
        public Void apply(Object input) {
            return null;
        }
    };

    private ListenableFuture<Iterator<V>> activeFetchFuture;
    private ListenableFuture<Void> activeFetchFutureAsVoid;
    private ArrayDeque<SizedIterator<V>> currentPages = new ArrayDeque<>();
    private Iterator<V> iterator;
    private boolean noMorePages;
    private Throwable savedThrowable;

    @Override
    public List<V> all() {
        return ImmutableList.copyOf(iterator());
    }

    @Override
    public ListenableFuture<Void> fetchMoreResults() {
        processActiveFetchFutureIfDone();
        if (activeFetchFuture == null) {
            activeFetchFuture = fetchNextPage();
        }
        if (activeFetchFutureAsVoid == null) {
            activeFetchFutureAsVoid = Futures.transform(activeFetchFuture,
                    ANY_TO_VOID);
        }
        return activeFetchFutureAsVoid;
    }

    @Override
    public int getAvailableWithoutFetching() {
        // If a fetch operation is in progress, we first want to incorporate the
        // objects returned by this operation if it is done.
        processActiveFetchFutureIfDone();
        // We remove all empty pages from the beginning of the list. This means
        // that all methods relying on this method can be sure that the first
        // page contains an element if this method returns a non-zero value.
        // This also means that we will not keep a reference to an iterator
        // longer than necessary, which can be useful if the iterator refers to
        // a non-empty collection (which might occupy a significant amount of
        // memory).
        SizedIterator<V> firstPage = currentPages.peek();
        while (firstPage != null && !firstPage.hasNext()) {
            currentPages.poll();
            firstPage = currentPages.peek();
        }
        int availableWithoutFetching = 0;
        for (SizedIterator<V> page : currentPages) {
            int pageRemainingCount = page.remainingCount();
            availableWithoutFetching += pageRemainingCount;
            // If the sum of all available elements is greater than
            // Integer.MAX_VALUE, the counter might overflow.
            if (availableWithoutFetching < 0) {
                if (pageRemainingCount < 0) {
                    throw new RuntimeException(
                            "remainingCount() returned a negative number. This is a violation of the contract defined by the SizedIterator interface.");
                } else {
                    return Integer.MAX_VALUE;
                }
            }
        }
        return availableWithoutFetching;
    }

    @Override
    public boolean isExhausted() {
        while (getAvailableWithoutFetching() == 0 && !noMorePages) {
            // If there is no fetch operation in progress, we start a new one.
            // Otherwise, we first check whether the operation in progress has
            // finished because we would like to use the objects returned by
            // that operation before starting a new one.
            if (activeFetchFuture == null) {
                fetchMoreResults();
            } else {
                processActiveFetchFutureIfDone();
            }
            // If we have an operation in progress, we wait for it to finish. If
            // we do not have one, we go to the next iteration of the loop
            // because it might be that the list of objects has been filled by
            // processActiveFetchFutureIfDone().
            if (activeFetchFuture != null) {
                try {
                    activeFetchFuture.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(
                            "The thread was interrupted while waiting for a fetch operation to finish.",
                            e);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause == null) {
                        throw new RuntimeException(
                                "Caught an ExecutionException without a cause: "
                                        + e.getMessage(),
                                e);
                    } else if (cause instanceof Error) {
                        throw (Error) cause;
                    } else if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    } else {
                        throw new RuntimeException(
                                "Retrieving the next page of objects failed: "
                                        + cause.getMessage(),
                                cause);
                    }
                }
            }
        }
        // If we got here, we either have objects or there are no pages left.
        return getAvailableWithoutFetching() == 0;
    }

    @Override
    public boolean isFullyFetched() {
        // If a fetch operation is in progress, we first want to incorporate the
        // result of this operation if it is done.
        processActiveFetchFutureIfDone();
        return noMorePages;
    }

    @Override
    public Iterator<V> iterator() {
        if (iterator == null) {
            iterator = new ObjectResultSetIterator<V>(this);
        }
        return iterator;
    }

    @Override
    public V one() {
        if (isExhausted()) {
            return null;
        }
        // If isExhausted() returns false, the first page in the list must be
        // non-empty (isExhausted calls getAvailableWithoutFetching() which
        // removes empty iterators from the start of the list).
        SizedIterator<V> currentPage = currentPages.peek();
        return currentPage.next();
    }

    /**
     * <p>
     * Fetches the next page of objects. This method must be implemented by
     * child classes and its implementation must not block.
     * </p>
     * 
     * <p>
     * This method start the fetching of the next page of objects and returns a
     * future that provides access to the objects once they have been fetched.
     * If there are no more objects, the future returned by this method must
     * return <code>null</code>.
     * </p>
     * 
     * <p>
     * The abstract base class guarantees that this method is only called when
     * the future returned by an earlier call is done and has yielded a non-null
     * result.
     * </p>
     * 
     * <p>
     * While implementations of this method may return any iterator, they should
     * return a {@link SizedIterator} whenever possible. Returning a
     * <code>SizedIterator</code> will result in a better performance because
     * unnecessary copy operations can be avoided.
     * </p>
     * 
     * @return future that completes when fetching the next page of objects has
     *         finished and returns an iterator over these objects or
     *         <code>null</code> if there are no more objects in the result set.
     */
    protected abstract ListenableFuture<Iterator<V>> fetchNextPage();

    private void processActiveFetchFutureIfDone() {
        if (noMorePages) {
            return;
        }
        if (activeFetchFuture != null && activeFetchFuture.isDone()) {
            if (savedThrowable != null) {
                throw new RuntimeException(
                        "A fetch operation failed. Cannot continue.",
                        savedThrowable);
            }
            Iterator<V> iterator;
            try {
                iterator = activeFetchFuture.get();
            } catch (InterruptedException e) {
                // This should never happen because the future is already done.
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        "Unexpected InterruptedException while trying to get result from done future.",
                        e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause == null) {
                    throw new RuntimeException(
                            "Caught an ExecutionException without a cause: "
                                    + e.getMessage(),
                            e);
                } else if (cause instanceof Error) {
                    throw (Error) cause;
                } else if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(
                            "Retrieving the next page of samples failed: "
                                    + cause.getMessage(),
                            cause);
                }
            }
            if (iterator == null) {
                noMorePages = true;
            } else {
                if (!iterator.hasNext()) {
                    // If the iterator is empty, there is no sense in adding it
                    // to the list because it would not be used anyway.
                } else if (iterator instanceof SizedIterator<?>) {
                    currentPages.add((SizedIterator<V>) iterator);
                } else {
                    LinkedList<V> iteratorElements = new LinkedList<>();
                    while (iterator.hasNext()) {
                        // If the iterator's next() method throws an exception,
                        // we have to save this exception. If we did not and the
                        // next() method actually consumed the offending
                        // element, a future call to this method might succeed.
                        // This could lead to a situation in which calling one
                        // of the methods that trigger this method would fail,
                        // but a subsequent call would succeed. This is a
                        // problem because this would not only skip the
                        // offending element, but all elements preceding that
                        // element. Simply failing completely after a fetch
                        // operation failed makes much more sense.
                        V nextElement;
                        try {
                            nextElement = iterator.next();
                        } catch (Error | RuntimeException e) {
                            savedThrowable = e;
                            throw e;
                        }
                        iteratorElements.add(nextElement);
                    }
                    currentPages.add(new ConsumingSizedCollectionIterator<>(
                            iteratorElements));
                }
            }
        }
        activeFetchFuture = null;
        activeFetchFutureAsVoid = null;
    }

}
