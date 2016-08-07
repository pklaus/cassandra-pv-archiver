/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.util;

import java.util.Iterator;
import java.util.List;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Object result-set implementation that is backed by a Cassandra
 * <code>ResultSet</code>. This implementation delegates most of the methods to
 * the backing <code>ResultSet</code>. However, it uses a user-supplied function
 * for converting the <code>Row</code>s in the result set to the appropriate
 * object type.
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
public class ResultSetBasedObjectResultSet<V> implements ObjectResultSet<V> {

    private Function<Object, Void> RETURN_VOID_FUNCTION = Functions
            .constant(null);

    private Function<? super Row, ? extends V> converterFunction;
    private Iterator<V> iterator;
    private ResultSet resultSet;

    /**
     * <p>
     * Creates a object result-set that is backed by the specified Cassandra
     * result-set. Each time an object is requested, this class fetches a row
     * from the backing result set and calls the specified converter function to
     * convert the row to an object.
     * </p>
     * 
     * <p>
     * The converter function will never be called with a <code>null</code>
     * argument.
     * </p>
     * 
     * @param resultSet
     *            result set that provides the rows that represent the objects
     *            in the object result-set.
     * @param converterFunction
     *            function that converts rows to objects. The function is called
     *            exactly once for every row that is fetched from the backing
     *            result set.
     */
    public ResultSetBasedObjectResultSet(ResultSet resultSet,
            Function<? super Row, ? extends V> converterFunction) {
        this.resultSet = resultSet;
        this.converterFunction = converterFunction;
    }

    @Override
    public List<V> all() {
        return ImmutableList.copyOf(iterator());
    }

    @Override
    public ListenableFuture<Void> fetchMoreResults() {
        return Futures.transform(resultSet.fetchMoreResults(),
                RETURN_VOID_FUNCTION);
    }

    @Override
    public int getAvailableWithoutFetching() {
        return resultSet.getAvailableWithoutFetching();
    }

    @Override
    public boolean isExhausted() {
        return resultSet.isExhausted();
    }

    @Override
    public boolean isFullyFetched() {
        return resultSet.isFullyFetched();
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
        Row row = resultSet.one();
        if (row == null) {
            return null;
        } else {
            return converterFunction.apply(row);
        }
    }

}
