/*
 * Copyright 2017 aquenos GmbH.
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

/**
 * <p>
 * Iterator that provides the number of remaining elements. This interface can
 * be used when the number of elements returned in an iteration has to be known
 * in advance, but the {@link Collection} interface cannot be used (e.g. because
 * elements are consumed by the iterator).
 * </p>
 * 
 * <p>
 * Like regular iterators, sized iterators have to be expected to not be
 * thread-safe unless explicitly stated differently in the documentation of the
 * implementation.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 * @param <E>
 *            the type of elements returned by this iterator.
 * @since 3.2
 */
public interface SizedIterator<E> extends Iterator<E> {

    /**
     * <p>
     * Returns the number of elements that will be provided by this iterator. If
     * <code>remainingCount()</code> returns zero, {@link #hasNext()} returns
     * <code>false</code>. If <code>hasNext()</code> returns <code>true</code>,
     * <code>remainingCount()</code> returns a number greater than zero. Each
     * time {@link #next()} is called, this number is decremented by one.
     * </p>
     * 
     * <p>
     * If the number of remaining elements is greater than
     * {@link Integer#MAX_VALUE}, {@link Integer#MAX_VALUE} is returned instead
     * of the actual number.
     * </p>
     * 
     * @return number of remaining elements in this iterator. This is the number
     *         of times that <code>next</code> can be called without throwing a
     *         {@link NoSuchElementException}.
     */
    int remainingCount();

}
