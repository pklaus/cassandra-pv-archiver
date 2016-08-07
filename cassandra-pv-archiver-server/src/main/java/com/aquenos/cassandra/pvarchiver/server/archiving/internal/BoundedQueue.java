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
import java.util.Queue;

/**
 * Queue that limits the number of elements stored inside it.
 * 
 * @author Sebastian Marsching
 *
 * @param <E>
 *            type of the elements stored in this queue.
 */
public interface BoundedQueue<E> extends Queue<E> {

    /**
     * Adds the specified element to the end of this queue. If this queue
     * contains elements that are older than the time limit specified when
     * creating this queue, these elements are silently removed and the overflow
     * counter is updated accordingly.
     * 
     * @param e
     *            element that shall be added to this queue.
     * @return always <code>true</code>.
     * @see #getOverflowCount()
     */
    @Override
    boolean add(E e);

    /**
     * Adds the all elements from the specified collection to the end of this
     * queue. If this queue contains elements that are older than the time limit
     * specified when creating this queue, these elements are silently removed
     * and the overflow counter is updated accordingly.
     * 
     * @param c
     *            collection with all the elements that shall be added to this
     *            queue.
     * @return always <code>true</code>.
     * @see #getOverflowCount()
     */
    @Override
    boolean addAll(Collection<? extends E> c);

    /**
     * Returns the current overflow count and resets it. This has exactly the
     * same effect as calling {@link #getOverflowCount()} followed by
     * {@link #resetOverflowCount()}.
     * 
     * @return the value of the {@linkplain #getOverflowCount() overflow count}
     *         before resetting it.
     */
    int getAndResetOverflowCount();

    /**
     * Returns the current value of the overflow count. Each time the
     * {@link #add(Object)} method removes an element because it is too old, the
     * overflow count is incremented by one. The overflow count can be reset
     * using the {@link #resetOverflowCount()} or
     * {@link #getAndResetOverflowCount()} methods. If the overflow count
     * exceeds {@link Integer#MAX_VALUE} because it is not reset, it wraps and
     * becomes negative.
     * 
     * @return number of elements that have been discarded because they were too
     *         old.
     */
    int getOverflowCount();

    /**
     * Adds the specified element to the end of this queue. This method is
     * identical to {@link #add(Object)}.
     * 
     * @return always <code>true</code>.
     */
    @Override
    boolean offer(E e);

    /**
     * Resets the overflow count to zero. For getting the current overflow count
     * and resetting it at the same time, the
     * {@link #getAndResetOverflowCount()} method can be used.
     */
    void resetOverflowCount();

}
