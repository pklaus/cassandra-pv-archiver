/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;

/**
 * <p>
 * Blocking queue that does not allow duplicate elements. This queue
 * implementation ensures that each element is only queued once. An attempt to
 * queue the same element again is not successful until the element has been
 * removed from the queue.
 * </p>
 * 
 * <p>
 * This class is intended for use by the {@link ArchivingService} and only
 * implements the methods that are necessary for this purpose. Internally, it is
 * backed by a {@link BlockingQueue} for queuing the elements and a
 * {@link ConcurrentHashMap} for ensuring that each element is only queued once.
 * Consequently, this class is safe for concurrent use by multiple threads.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 * @param <E>
 *            type of the elements that are stored in this queue.
 */
public class LinkedBlockingUniqueQueue<E> {

    private LinkedBlockingQueue<E> queue = new LinkedBlockingQueue<E>();
    private Set<E> queuedElements = Collections
            .newSetFromMap(new ConcurrentHashMap<E, Boolean>(64, 0.75f, 64));

    /**
     * Adds the specified element to the end of the queue. The element is only
     * added if it is not already in the queue.
     * 
     * @param element
     *            element that shall be added to this queue.
     * @return <code>true</code> if the specified element has been added to the
     *         queue, <code>false</code> if the specified element has already
     *         been queued before and not been removed since.
     * @throws NullPointerException
     *             if the specified element is <code>null</code>.
     */
    public boolean add(E element) {
        if (queuedElements.add(element)) {
            queue.add(element);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Removes and returns an element from the head of the queue. If the queue
     * is empty, this method blocks until an element becomes available. The
     * element at the head of the queue is the element that has been queued
     * earlier than all the other elements in the queue (the oldest element).
     * 
     * @return element from the head of the queue.
     * @throws InterruptedException
     *             if the calling thread is interrupted while it is waiting for
     *             an element to become available.
     */
    public E take() throws InterruptedException {
        E element = queue.take();
        queuedElements.remove(element);
        return element;
    }

}
