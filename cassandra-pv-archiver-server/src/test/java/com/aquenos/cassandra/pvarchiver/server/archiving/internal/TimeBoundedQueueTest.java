/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Tests for the {@link TimeBoundedQueue}.
 * 
 * @author Sebastian Marsching
 */
public class TimeBoundedQueueTest {

    /**
     * Tests that the time bounded queue actually limits the number of elements.
     * This test has been specifically designed to detect a bug in which the
     * <code>add</code> method would unexpectedly throw a
     * <code>NoSuchElementException</code>.
     */
    @Test
    public void testLimit() {
        TimeBoundedQueue<Object> queue = new TimeBoundedQueue<>(1000L);
        queue.add(new Object());
        assertEquals(1, queue.size());
        assertEquals(0, queue.getOverflowCount());
        queue.add(new Object());
        // There is a very small chance that this test will fail spuriously
        // when the thread is interrupted for more than a second. However, this
        // is so unlikely that we can risk it.
        assertEquals(2, queue.size());
        assertEquals(0, queue.getOverflowCount());
        try {
            Thread.sleep(1200L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        queue.add(new Object());
        // The two old elements should have been removed from the queue.
        assertEquals(1, queue.size());
        assertEquals(2, queue.getOverflowCount());
    }

}
