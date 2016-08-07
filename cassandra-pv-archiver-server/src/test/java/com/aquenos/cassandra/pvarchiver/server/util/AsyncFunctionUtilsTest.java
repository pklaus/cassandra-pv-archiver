/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.util;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Tests for the {@link AsyncFunctionUtils}.
 * 
 * @author Sebastian Marsching
 */
public class AsyncFunctionUtilsTest {

    private final AsyncFunction<Integer, Integer> multiplyByFive = new AsyncFunction<Integer, Integer>() {
        @Override
        public ListenableFuture<Integer> apply(Integer input) throws Exception {
            return Futures.immediateFuture(input * 5);
        }
    };

    private final AsyncFunction<Integer, Integer> addThree = new AsyncFunction<Integer, Integer>() {
        @Override
        public ListenableFuture<Integer> apply(Integer input) throws Exception {
            return Futures.immediateFuture(input + 3);
        }
    };

    private final AsyncFunction<Integer, Integer> addSeven = new AsyncFunction<Integer, Integer>() {
        @Override
        public ListenableFuture<Integer> apply(Integer input) throws Exception {
            return Futures.immediateFuture(input + 7);
        }
    };

    /**
     * Tests the {@link AsyncFunctionUtils#aggregate(AsyncFunction...)} and
     * {@link AsyncFunctionUtils#aggregate(Iterable)} methods.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testAggregate() throws Exception {
        AsyncFunction<Integer, List<Integer>> parallelFunction = AsyncFunctionUtils
                .aggregate(multiplyByFive, addThree, addSeven);
        List<Integer> resultList = parallelFunction.apply(2).get();
        assertEquals(10, (int) resultList.get(0));
        assertEquals(5, (int) resultList.get(1));
        assertEquals(9, (int) resultList.get(2));
        @SuppressWarnings("unchecked")
        List<AsyncFunction<Integer, Integer>> functionList = Lists
                .newArrayList(multiplyByFive, addThree, addSeven);
        AsyncFunction<Integer, List<Integer>> parallelFunction2 = AsyncFunctionUtils
                .aggregate(functionList);
        resultList = parallelFunction2.apply(5).get();
        assertEquals(25, (int) resultList.get(0));
        assertEquals(8, (int) resultList.get(1));
        assertEquals(12, (int) resultList.get(2));
    }

    /**
     * Tests the {@link AsyncFunctionUtils#aggregate(AsyncFunction...)} method
     * passing two functions that have different return types.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testAggregateMixedTypes() throws Exception {
        AsyncFunction<Integer, String> toString = new AsyncFunction<Integer, String>() {
            @Override
            public ListenableFuture<String> apply(Integer input)
                    throws Exception {
                return Futures.immediateFuture(input.toString());
            }
        };
        AsyncFunction<Integer, Long> toLong = new AsyncFunction<Integer, Long>() {
            @Override
            public ListenableFuture<Long> apply(Integer input) throws Exception {
                return Futures.immediateFuture((long) input);
            }
        };
        AsyncFunction<Integer, List<Object>> parallelFunction = AsyncFunctionUtils
                .<Integer, Object> aggregate(toString, toLong);
        List<Object> resultList = parallelFunction.apply(2).get();
        assertEquals("2", resultList.get(0));
        assertEquals(Long.valueOf(2L), resultList.get(1));
    }

    /**
     * Tests the {@link AsyncFunctionUtils#asAsyncFunction(Function)} method.
     * 
     * @throws Exception
     */
    @Test
    public void testAsAsyncFunction() throws Exception {
        Function<Object, String> syncFunction1 = Functions.toStringFunction();
        Function<Integer, Integer> syncFunction2 = new Function<Integer, Integer>() {
            @Override
            public Integer apply(Integer input) {
                if (input == null) {
                    return null;
                } else {
                    return input + 5;
                }
            }
        };
        AsyncFunction<Object, String> asyncFunction1 = AsyncFunctionUtils
                .asAsyncFunction(syncFunction1);
        AsyncFunction<Integer, Integer> asyncFunction2 = AsyncFunctionUtils
                .asAsyncFunction(syncFunction2);
        assertEquals(syncFunction1.apply(123L), asyncFunction1.apply(123L)
                .get());
        assertEquals(syncFunction1.apply(456L), asyncFunction1.apply(456L)
                .get());
        assertEquals(syncFunction2.apply(3), asyncFunction2.apply(3).get());
        assertEquals(syncFunction2.apply(19), asyncFunction2.apply(19).get());
    }

    /**
     * Tests the
     * {@link AsyncFunctionUtils#compose(AsyncFunction, AsyncFunction)},
     * {@link AsyncFunctionUtils#compose(AsyncFunction, Function)}, and
     * {@link AsyncFunctionUtils#compose(Function, AsyncFunction)} methods.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testCompose() throws Exception {
        final Function<Integer, Long> intToLongSync = new Function<Integer, Long>() {
            @Override
            public Long apply(Integer input) {
                return (long) input;
            }
        };
        final Function<Long, String> longToStringSync = new Function<Long, String>() {
            @Override
            public String apply(Long input) {
                return input.toString();
            }
        };
        AsyncFunction<Integer, Long> intToLongAsync = new AsyncFunction<Integer, Long>() {
            @Override
            public ListenableFuture<Long> apply(Integer input) throws Exception {
                return Futures.immediateFuture((long) input);
            }
        };
        AsyncFunction<Long, String> longToStringAsync = new AsyncFunction<Long, String>() {
            @Override
            public ListenableFuture<String> apply(Long input) throws Exception {
                return Futures.immediateFuture(input.toString());
            }
        };
        AsyncFunction<Integer, String> serialFunction;
        serialFunction = AsyncFunctionUtils.compose(intToLongAsync,
                longToStringAsync);
        assertEquals("5", serialFunction.apply(5).get());
        serialFunction = AsyncFunctionUtils.compose(intToLongAsync,
                longToStringSync);
        assertEquals("19", serialFunction.apply(19).get());
        serialFunction = AsyncFunctionUtils.compose(intToLongSync,
                longToStringAsync);
        assertEquals("42", serialFunction.apply(42).get());
    }

    /**
     * Tests the {@link AsyncFunctionUtils#compose(AsyncFunction...)} and
     * {@link AsyncFunctionUtils#compose(Iterable)} methods.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testComposeVarargs() throws Exception {
        AsyncFunction<Integer, Integer> serialFunction = AsyncFunctionUtils
                .compose(multiplyByFive, addThree, addSeven);
        assertEquals(Integer.valueOf(20), serialFunction.apply(2).get());
        @SuppressWarnings("unchecked")
        List<AsyncFunction<Integer, Integer>> functionList = Lists
                .newArrayList(multiplyByFive, addThree, addSeven);
        AsyncFunction<Integer, Integer> serialFunction2 = AsyncFunctionUtils
                .compose(functionList);
        assertEquals(Integer.valueOf(35), serialFunction2.apply(5).get());
    }

    /**
     * Tests the {@link AsyncFunctionUtils#compose(AsyncFunction...)} method
     * using a series of functions that have different input and output types.
     * As the output type of each function matches the input type of the next
     * function, this should work. However, raw types and unchecked conversions
     * have to be used to the combined function because Java's type system does
     * not allow us to specify such a condition through generics.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testComposeVarargsMixedTypes() throws Exception {
        AsyncFunction<Integer, Long> intToLong = new AsyncFunction<Integer, Long>() {
            @Override
            public ListenableFuture<Long> apply(Integer input) throws Exception {
                return Futures.immediateFuture((long) input);
            }
        };
        AsyncFunction<Long, Long> multiplyByTwo = new AsyncFunction<Long, Long>() {
            @Override
            public ListenableFuture<Long> apply(Long input) throws Exception {
                return Futures.immediateFuture(input * 2L);
            }
        };
        AsyncFunction<Long, String> longToString = new AsyncFunction<Long, String>() {
            @Override
            public ListenableFuture<String> apply(Long input) throws Exception {
                return Futures.immediateFuture(input.toString());
            }
        };
        @SuppressWarnings({ "rawtypes", "unchecked" })
        AsyncFunction<Integer, String> serialFunction = AsyncFunctionUtils
                .compose((AsyncFunction) intToLong,
                        (AsyncFunction) multiplyByTwo,
                        (AsyncFunction) longToString);
        assertEquals("10", serialFunction.apply(5).get());
    }

    /**
     * Tests the {@link AsyncFunctionUtils#identity()} method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testIdentity() throws Exception {
        AsyncFunction<String, String> stringIdentityFunction = AsyncFunctionUtils
                .identity();
        AsyncFunction<Integer, Integer> integerIdentityFunction = AsyncFunctionUtils
                .identity();
        assertEquals("foo", stringIdentityFunction.apply("foo").get());
        assertEquals("bar", stringIdentityFunction.apply("bar").get());
        assertEquals(Integer.valueOf(123),
                integerIdentityFunction.apply(Integer.valueOf(123)).get());
        assertEquals(Integer.valueOf(456),
                integerIdentityFunction.apply(Integer.valueOf(456)).get());
    }

    /**
     * Tests the {@link AsyncFunctionUtils#ifThen(Predicate, AsyncFunction)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testIfThen() throws Exception {
        Predicate<Integer> predicate = new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
                return input < 4;
            }
        };
        AsyncFunction<Integer, Integer> combinedFunction = AsyncFunctionUtils
                .ifThen(predicate, multiplyByFive);
        assertEquals(Integer.valueOf(15), combinedFunction.apply(3).get());
        assertEquals(Integer.valueOf(9), combinedFunction.apply(9).get());
    }

    /**
     * Tests the
     * {@link AsyncFunctionUtils#ifThenElse(Predicate, AsyncFunction, AsyncFunction)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testIfThenElse() throws Exception {
        Predicate<Integer> predicate = new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
                return input < 4;
            }
        };
        AsyncFunction<Integer, String> functionIfTrue = new AsyncFunction<Integer, String>() {
            @Override
            public ListenableFuture<String> apply(Integer input)
                    throws Exception {
                return Futures.immediateFuture("small number: " + input);
            }
        };
        AsyncFunction<Integer, String> functionIfFalse = new AsyncFunction<Integer, String>() {
            @Override
            public ListenableFuture<String> apply(Integer input)
                    throws Exception {
                return Futures.immediateFuture("big number: " + input);
            }
        };
        AsyncFunction<Integer, String> combinedFunction = AsyncFunctionUtils
                .ifThenElse(predicate, functionIfTrue, functionIfFalse);
        assertEquals("small number: 3", combinedFunction.apply(3).get());
        assertEquals("big number: 9", combinedFunction.apply(9).get());
    }

}
