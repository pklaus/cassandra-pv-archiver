/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * Queue that limits the number of elements inside the queue based on how old
 * they are. When adding a new element with the {@link #add(Object)} method, all
 * elements at the head of the queue that are older than the specified time
 * limit are removed. The number of elements that have been removed can be
 * retrieved with the {@link #getOverflowCount()} method.
 * </p>
 * 
 * <p>
 * This queue implementation is not thread-safe and is specifically designed to
 * be used by the {@link ArchivingService}. While it implements the complete
 * {@link Queue} interface, the {@link #contains(Object)},
 * {@link #containsAll(Collection)}, {@link #remove(Object)},
 * {@link #removeAll(Collection)}, and {@link #retainAll(Collection)} methods
 * are not particularly efficient.
 * </p>
 * 
 * <p>
 * Internally, this qeueue is backed by a {@link LinkedList} that stores each
 * element together with the time-stamp when it has been added.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 * @param <E>
 *            type of the elements stored in this queue.
 */
public class TimeBoundedQueue<E> implements BoundedQueue<E> {

    private int overflowCount;
    private LinkedList<Pair<Long, E>> queue = new LinkedList<Pair<Long, E>>();
    private List<E> queueSimpleView = Lists.transform(queue,
            new Function<Pair<Long, E>, E>() {
                @Override
                public E apply(Pair<Long, E> input) {
                    return input == null ? null : input.getRight();
                }
            });
    private long timeLimitMilliseconds;

    /**
     * Creates a new queue that uses the specified time limit. When adding
     * elements using the {@link #add(Object)} method, elements at the head of
     * the queue that are older than the specified time-limit are removed.
     * 
     * @param timeLimitMilliseconds
     *            max. time that elements are stored inside this queue
     *            (specified in milliseconds). Older elements are only removed
     *            when a new element is added.
     * @throws IllegalArgumentException
     *             if <code>timeLimitMilliseconds</code> is less than or equal
     *             to zero.
     */
    public TimeBoundedQueue(long timeLimitMilliseconds) {
        Preconditions.checkArgument(timeLimitMilliseconds > 0L,
                "The time limit must be strictly positive.");
        this.timeLimitMilliseconds = timeLimitMilliseconds;
    }

    @Override
    public boolean add(E element) {
        long currentTime = System.currentTimeMillis();
        Pair<Long, E> head = queue.peek();
        while (head != null
                && head.getLeft() < currentTime - timeLimitMilliseconds) {
            queue.poll();
            ++overflowCount;
            head = queue.getFirst();
        }
        return queue.add(Pair.of(currentTime, element));
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        for (E element : c) {
            add(element);
        }
        return true;
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public boolean contains(Object o) {
        return queueSimpleView.contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return queueSimpleView.containsAll(c);
    }

    @Override
    public E element() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return peek();
    }

    @Override
    public int getAndResetOverflowCount() {
        int overflowCount = this.overflowCount;
        this.overflowCount = 0;
        return overflowCount;
    }

    @Override
    public int getOverflowCount() {
        return overflowCount;
    }

    /**
     * Tells whether this queue is empty.
     * 
     * @return <code>true</code> if this queue is empty, <code>false</code> if
     *         there is at least one element stored in this queue.
     */
    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return queueSimpleView.iterator();
    }

    @Override
    public boolean offer(E e) {
        // From the description of this method, is is a bit unclear whether we
        // should remove old elements to make space. However, it seems logical
        // that this method does the same as add except throwing an exception
        // when the add operation fails (which it never does in our
        // implementation).
        return add(e);
    }

    /**
     * Returns the first element in this queue without removing it. The first
     * element is the element that has been stored in this queue for the longest
     * time.
     * 
     * @return the element at the head of this queue or <code>null</code> if
     *         this queue is empty.
     */
    @Override
    public E peek() {
        Pair<Long, E> firstPair = queue.peek();
        return firstPair == null ? null : firstPair.getRight();
    }

    /**
     * Removes the first element in this queue and returns it. The first element
     * is the element that has been stored in this queue for the longest time.
     * 
     * @return the element that was at the head of this queue before removing it
     *         or <code>null</code> if this queue is empty.
     */
    @Override
    public E poll() {
        Pair<Long, E> firstPair = queue.poll();
        return firstPair == null ? null : firstPair.getRight();
    }

    @Override
    public E remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return poll();
    }

    @Override
    public boolean remove(Object o) {
        return queueSimpleView.remove(o);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return queueSimpleView.removeAll(c);
    }

    @Override
    public void resetOverflowCount() {
        overflowCount = 0;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return queueSimpleView.retainAll(c);
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public Object[] toArray() {
        return queueSimpleView.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return queueSimpleView.toArray(a);
    }

}
