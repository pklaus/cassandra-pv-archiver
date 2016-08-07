/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.util;

import java.util.List;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Helper methods for dealing with Guava asynchronous function objects. In
 * particular, this class provides methods for combining several asynchronous
 * functions into a single asynchronous function.
 * 
 * @author Sebastian Marsching
 */
public abstract class AsyncFunctionUtils {

    // We use the same identity function regardless of the type parameters. This
    // way, we can avoid allocating many objects for the same implementation.
    private static final AsyncFunction<Object, Object> IDENTITY_FUNCTION = asAsyncFunction(Functions
            .identity());

    /**
     * <p>
     * Creates an asynchronous function that calls the specified asynchronous
     * functions and produces a listenable future that is the aggregation of the
     * futures returned by the individual functions.
     * </p>
     * 
     * <p>
     * The asynchronous function returned will call the passed functions'
     * {@link AsyncFunction#apply(Object) apply(...)} methods in series (in the
     * thread calling the returned function's <code>apply(...)</code> method),
     * but the future returned by the <code>apply(...)</code> of the returned
     * function will represent a future that completes when all of the futures
     * returned by the individual functions have completed. If all those futures
     * completed successfully, the future provides a list with the values of the
     * individual futures. If any of the futures returned by the individual
     * functions fails, the future returned by the returned function also fails.
     * </p>
     * 
     * <p>
     * This method is very similar to {@link Futures#allAsList(Iterable)}. The
     * main difference is, that it works on {@link AsyncFunction}s instead of
     * {@link ListenableFuture}s and thus is suitable for the combination of
     * functions before they are actually supposed to be called.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            type of the elements of the resulting function's output list.
     *            This must be compatible with the output types of the specified
     *            <code>functions</code>.
     * @param functions
     *            functions shall be called when the function returned by this
     *            method is called. Each of these functions gets the input
     *            provided to the function returned by this method passed when
     *            being called.
     * @return asynchronous function that represents a combination of the
     *         specified functions all run on the same input and that returns a
     *         future that completes when the futures returned by the individual
     *         functions have completed.
     * @throws NullPointerException
     *             if any of the passed functions are <code>null</code>.
     */
    @SafeVarargs
    public static <I, O> AsyncFunction<I, List<O>> aggregate(
            AsyncFunction<? super I, ? extends O>... functions) {
        // The functions array is evaluated later when the returned function is
        // called. Therefore, we create a copy so that modifications of the
        // source do not have undesired effects later. As a side effect, this
        // will also throw a NullPointerException if one of the elements is
        // null, which is better than throwing the exception later when the
        // returned function is called.
        return aggregateInternal(ImmutableList.copyOf(functions));
    }

    /**
     * <p>
     * Creates an asynchronous function that calls the specified asynchronous
     * functions and produces a listenable future that is the aggregation of the
     * futures returned by the individual functions.
     * </p>
     * 
     * <p>
     * The asynchronous function returned will call the passed functions'
     * {@link AsyncFunction#apply(Object) apply(...)} methods in series (in the
     * thread calling the returned function's <code>apply(...)</code> method),
     * but the future returned by the <code>apply(...)</code> of the returned
     * function will represent a future that completes when all of the futures
     * returned by the individual functions have completed. If all those futures
     * completed successfully, the future provides a list with the values of the
     * individual futures. If any of the futures returned by the individual
     * functions fails, the future returned by the returned function also fails.
     * </p>
     * 
     * <p>
     * This method is very similar to {@link Futures#allAsList(Iterable)}. The
     * main difference is, that it works on {@link AsyncFunction}s instead of
     * {@link ListenableFuture}s and thus is suitable for the combination of
     * functions before they are actually supposed to be called.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            type of the elements of the resulting function's output list.
     *            This must be compatible with the output types of the specified
     *            <code>functions</code>.
     * @param functions
     *            functions shall be called when the function returned by this
     *            method is called. Each of these functions gets the input
     *            provided to the function returned by this method passed when
     *            being called.
     * @return asynchronous function that represents a combination of the
     *         specified functions all run on the same input and that returns a
     *         future that completes when the futures returned by the individual
     *         functions have completed.
     * @throws NullPointerException
     *             if any of the elements returned by the passed iterable are
     *             <code>null</code>.
     */
    public static <I, O> AsyncFunction<I, List<O>> aggregate(
            Iterable<? extends AsyncFunction<? super I, ? extends O>> functions) {
        // The passed functions are evaluated later when the returned function
        // is called. Therefore, we create a copy so that modifications of the
        // source do not have undesired effects later. As a side effect, this
        // will also throw a NullPointerException if one of the elements is
        // null, which is better than throwing the exception later when the
        // returned function is called.
        return aggregateInternal(ImmutableList.copyOf(functions));
    }

    private static <I, O> AsyncFunction<I, List<O>> aggregateInternal(
            final List<AsyncFunction<? super I, ? extends O>> functions) {
        return new AsyncFunction<I, List<O>>() {
            @Override
            public ListenableFuture<List<O>> apply(final I input)
                    throws Exception {
                List<ListenableFuture<? extends O>> futures;
                futures = Lists
                        .transform(
                                functions,
                                new Function<AsyncFunction<? super I, ? extends O>, ListenableFuture<? extends O>>() {
                                    @Override
                                    public ListenableFuture<? extends O> apply(
                                            AsyncFunction<? super I, ? extends O> function) {
                                        try {
                                            return function.apply(input);
                                        } catch (Throwable t) {
                                            return Futures
                                                    .immediateFailedFuture(t);
                                        }
                                    }
                                });
                return Futures.allAsList(futures);
            }
        };
    }

    /**
     * Converts a synchronous function to an asynchronous function. The returned
     * asynchronous function simply wraps the specified synchronous function.
     * When the asynchronous function is called, it calls the synchronous
     * function and wraps the result in an immediate future and returns it. If
     * the synchronous function throws an exception, the exception is wrapped in
     * an immediate failed future and that failed future is returned.
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            output type of the resulting function.
     * @param function
     *            synchronous function that shall be wrapped in an asynchronous
     *            function.
     * @return asynchronous function wrapping the specified synchronous
     *         function.
     * @throws NullPointerException
     *             if the specified <code>function</code> is null.
     */
    public static <I, O> AsyncFunction<I, O> asAsyncFunction(
            final Function<? super I, ? extends O> function) {
        Preconditions.checkNotNull(function);
        return new AsyncFunction<I, O>() {
            @Override
            public ListenableFuture<O> apply(I input) throws Exception {
                O output;
                try {
                    output = function.apply(input);
                } catch (Throwable t) {
                    return Futures.immediateFailedFuture(t);
                }
                return Futures.immediateFuture(output);
            }
        };
    }

    /**
     * <p>
     * Creates an asynchronous function that is the composition of the two
     * specified asynchronous functions called in series.
     * </p>
     * 
     * <p>
     * The returned function will first call the first of the specified
     * functions. When the future returned by that function completes, it will
     * call the second of the specified functions, passing it the value of the
     * future. The returned function returns the future returned by the second
     * of the specified functions.
     * </p>
     * 
     * <p>
     * The first function is called in the thread that called the returned
     * function. The second function is called in the thread that completes the
     * future returned by the first function. If the future returned by the
     * first function completes before the first function returns, the second
     * function is called in the thread that calls the returned function. For
     * these reasons, the second asynchronous function should not block or
     * perform a considerable amount of work. Instead, it should quickly return
     * a future and perform all actual work in a separate thread that completes
     * the future when the work has finished.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction)}, but it works
     * on asynchronous functions instead of futures. Therefore, it is suitable
     * for building an asynchronous function that represents the concatenation
     * of other asynchronous functions before any of those functions are
     * actually supposed to be called.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            output type of the resulting function.
     * @param <T>
     *            output type of <code>function1</code>. This type must be
     *            compatible with the input type of <code>function2</code>.
     * @param function1
     *            first function to be called. This function is executed in the
     *            thread that calls the returned function.
     * @param function2
     *            second function to be called. This function is called with the
     *            return value of the first function when the future returned by
     *            the first function completes. The function is called in the
     *            thread that completes the future (or the thread that called
     *            the returned function if the future has already been completed
     *            when the first function returns).
     * @return asynchronous function that represents a combination of the
     *         specified functions. The second function is called with the
     *         result of the first one after the future returned by the first
     *         one has completed.
     * @throws NullPointerException
     *             if any of the specified arguments is <code>null</code>.
     */
    public static <I, O, T> AsyncFunction<I, O> compose(
            AsyncFunction<? super I, T> function1,
            AsyncFunction<? super T, ? extends O> function2) {
        return compose(MoreExecutors.sameThreadExecutor(), function1, function2);
    }

    /**
     * <p>
     * Creates an asynchronous function that is the composition of the two
     * specified asynchronous functions called in series.
     * </p>
     * 
     * <p>
     * The returned function will first call the first of the specified
     * functions. When the future returned by that function completes, it will
     * call the second of the specified functions, passing it the value of the
     * future. The returned function returns the future returned by the second
     * of the specified functions.
     * </p>
     * 
     * <p>
     * The first function is called in the thread that called the returned
     * function. The second function is called through the means of the
     * specified executor.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction, Executor)}, but
     * it works on asynchronous functions instead of futures. Therefore, it is
     * suitable for building an asynchronous function that represents the
     * concatenation of other asynchronous functions before any of those
     * functions are actually supposed to be called.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            output type of the resulting function.
     * @param <T>
     *            output type of <code>function1</code>. This type must be
     *            compatible with the input type of <code>function2</code>.
     * @param executor
     *            executor used for executing the second function.
     * @param function1
     *            first function to be called. This function is executed in the
     *            thread that calls the returned function.
     * @param function2
     *            second function to be called. This function is called with the
     *            return value of the first function when the future returned by
     *            the first function completes. The function is called through
     *            means of the specified <code>executor</code>.
     * @return asynchronous function that represents a combination of the
     *         specified functions. The second function is called with the
     *         result of the first one after the future returned by the first
     *         one has completed.
     * @throws NullPointerException
     *             if any of the specified arguments is <code>null</code>.
     */
    public static <I, O, T> AsyncFunction<I, O> compose(
            final Executor executor,
            final AsyncFunction<? super I, T> function1,
            final AsyncFunction<? super T, ? extends O> function2) {
        Preconditions.checkNotNull(executor);
        Preconditions.checkNotNull(function1);
        Preconditions.checkNotNull(function2);
        return new AsyncFunction<I, O>() {
            @Override
            public ListenableFuture<O> apply(I input) throws Exception {
                return Futures.transform(function1.apply(input), function2,
                        executor);
            }
        };
    }

    /**
     * <p>
     * Creates an asynchronous function that is the composition of an
     * asynchronous function and a synchronous function called in series.
     * </p>
     * 
     * <p>
     * The returned function will first call the specified asynchronous
     * function. When the future returned by that function completes, it will
     * call the specified synchronous function, passing it the value of the
     * future. The returned function returns a future that provides the value
     * returned by the specified synchronous function.
     * </p>
     * 
     * <p>
     * The asynchronous function is called in the thread that calls the returned
     * function. The synchronous function is called in the thread that completes
     * the future returned by the asynchronous function. If the future returned
     * by the asynchronous function completes before the asynchronous function
     * returns, the synchronous function is called in the thread that calls the
     * returned function. For these reasons, the supplied synchronous function
     * should not block or perform a considerable amount of work.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, Function)}, but it works on an
     * asynchronous function instead of a future. Therefore, it is suitable for
     * building an asynchronous function that represents the concatenation of
     * other functions before any of those functions are actually supposed to be
     * called.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            output type of the resulting function.
     * @param <T>
     *            output type of <code>function1</code>. This type must be
     *            compatible with the input type of <code>function2</code>.
     * @param function1
     *            first function to be called. This function is executed in the
     *            thread that calls the returned function.
     * @param function2
     *            second function to be called. This function is called with the
     *            return value of the first function when the future returned by
     *            the first function completes. The function is called in the
     *            thread that completes the future (or the thread that called
     *            the returned function if the future has already been completed
     *            when the first function returns).
     * @return asynchronous function that represents a combination of the
     *         specified functions. The second function is called with the
     *         result of the first one after the future returned by the first
     *         one has completed.
     * @throws NullPointerException
     *             if any of the specified arguments is <code>null</code>.
     */
    public static <I, O, T> AsyncFunction<I, O> compose(
            final AsyncFunction<? super I, T> function1,
            final Function<? super T, ? extends O> function2) {
        return compose(MoreExecutors.sameThreadExecutor(), function1, function2);
    }

    /**
     * <p>
     * Creates an asynchronous function that is the composition of an
     * asynchronous function and a synchronous function called in series.
     * </p>
     * 
     * <p>
     * The returned function will first call the specified asynchronous
     * function. When the future returned by that function completes, it will
     * call the specified synchronous function, passing it the value of the
     * future. The returned function returns a future that provides the value
     * returned by the specified synchronous function.
     * </p>
     * 
     * <p>
     * The asynchronous function is called in the thread that calls the returned
     * function. The synchronous function is called through the means of the
     * specified executor.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, Function, Executor)}, but it
     * works on an asynchronous function instead of a future. Therefore, it is
     * suitable for building an asynchronous function that represents the
     * concatenation of other functions before any of those functions are
     * actually supposed to be called.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            output type of the resulting function.
     * @param <T>
     *            output type of <code>function1</code>. This type must be
     *            compatible with the input type of <code>function2</code>.
     * @param executor
     *            executor used for executing the synchronous function.
     * @param function1
     *            first function to be called. This function is executed in the
     *            thread that calls the returned function.
     * @param function2
     *            second function to be called. This function is called with the
     *            return value of the first function when the future returned by
     *            the first function completes. The function is called through
     *            means of the specified <code>executor</code>.
     * @return asynchronous function that represents a combination of the
     *         specified functions. The second function is called with the
     *         result of the first one after the future returned by the first
     *         one has completed.
     * @throws NullPointerException
     *             if any of the specified arguments is <code>null</code>.
     */
    public static <I, O, T> AsyncFunction<I, O> compose(
            final Executor executor,
            final AsyncFunction<? super I, T> function1,
            final Function<? super T, ? extends O> function2) {
        Preconditions.checkNotNull(executor);
        Preconditions.checkNotNull(function1);
        Preconditions.checkNotNull(function2);
        return new AsyncFunction<I, O>() {
            @Override
            public ListenableFuture<O> apply(I input) throws Exception {
                return Futures.transform(function1.apply(input), function2,
                        executor);
            }
        };
    }

    /**
     * <p>
     * Creates an asynchronous function that is the composition of a synchronous
     * function and an asynchronous function called in series.
     * </p>
     * 
     * <p>
     * The returned function will first call the specified synchronous function.
     * It will then call the specified asynchronous function, passing it the
     * return value of the synchronous function. The returned function returns
     * the future returned by the specified asynchronous function.
     * </p>
     * 
     * <p>
     * Both functions are called in the thread that calls the returned function.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function.
     * @param <O>
     *            output type of the resulting function.
     * @param <T>
     *            output type of <code>function1</code>. This type must be
     *            compatible with the input type of <code>function2</code>.
     * @param function1
     *            first function to be called. This function is executed in the
     *            thread that calls the returned function.
     * @param function2
     *            second function to be called. This function is called with the
     *            return value of the first function. The function is called in
     *            the thread that calls the returned function.
     * @return asynchronous function that represents a combination of the
     *         specified functions. The second function is called with the
     *         result of the first one.
     * @throws NullPointerException
     *             if any of the specified arguments is <code>null</code>.
     */
    public static <I, O, T> AsyncFunction<I, O> compose(
            final Function<? super I, T> function1,
            final AsyncFunction<? super T, O> function2) {
        Preconditions.checkNotNull(function1);
        Preconditions.checkNotNull(function2);
        return new AsyncFunction<I, O>() {
            @Override
            public ListenableFuture<O> apply(I input) throws Exception {
                return function2.apply(function1.apply(input));
            }
        };
    }

    /**
     * <p>
     * Creates an asynchronous function that calls the specified asynchronous
     * functions in series.
     * </p>
     * 
     * <p>
     * The returned function will call the specified functions in the specified
     * order, passing the result of each function to the following one. The
     * first of the specified functions receives the input passed to the
     * returned function and the returned function returns the output returned
     * by the last of the specified functions.
     * </p>
     * 
     * <p>
     * Each of the functions' {@link AsyncFunction#apply(Object) apply(...)}
     * method is called in the thread that completes the future returned by the
     * previous function. The first function is called in the thread that calls
     * the returned function. For this reason, the supplied asynchronous
     * functions should not block or perform a considerable amount of work.
     * Instead, they should quickly return a future and perform all actual work
     * in a separate thread that completes the future when the work has
     * finished.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction)}, but it works
     * on asynchronous functions instead of futures and can take several
     * functions. Therefore, it is suitable for building an asynchronous
     * function that represents the concatenation of other asynchronous
     * functions before any of those functions are actually supposed to be
     * called.
     * </p>
     * 
     * <p>
     * Due to limitations of Java generics, this method only supports a list of
     * functions where the return type of each function is compatible with the
     * input type of each function. However, this is not a limitation of the
     * implementation. When raw types are used, this method can work with
     * arbitrary asynchronous functions as long as the return type of each
     * function is compatible with the input type of the next function. For
     * concatenating functions with different types in a type-safe way, consider
     * using {@link #compose(AsyncFunction, AsyncFunction)} instead.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <T>
     *            input and output type of the resulting function. The input and
     *            output types of the supplied <code>functions</code> must be
     *            compatible with this type.
     * @param functions
     *            functions to be encapsulated in the returned function. The
     *            functions are executed in series when the returned function is
     *            called.
     * @return asynchronous function that represents a combination of the
     *         specified functions, each one run after the previous one has
     *         completed.
     * @throws NullPointerException
     *             if any of the specified <code>functions</code> are
     *             <code>null</code>.
     */
    @SafeVarargs
    public static <T> AsyncFunction<T, T> compose(
            AsyncFunction<? super T, ? extends T>... functions) {
        return compose(MoreExecutors.sameThreadExecutor(), functions);
    }

    /**
     * <p>
     * Creates an asynchronous function that calls the specified asynchronous
     * functions in series.
     * </p>
     * 
     * <p>
     * The returned function will call the specified functions in the specified
     * order, passing the result of each function to the following one. The
     * first of the specified functions receives the input passed to the
     * returned function and the returned function returns the output returned
     * by the last of the specified functions.
     * </p>
     * 
     * <p>
     * The functions' {@link AsyncFunction#apply(Object) apply(...)} method is
     * called through means of the specified <code>executor</code>, except for
     * the first of the specified functions which is always called in the thread
     * that calls the returned function.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction, Executor)}, but
     * it works on asynchronous functions instead of futures and can take
     * several functions. Therefore, it is suitable for building an asynchronous
     * function that represents the concatenation of other asynchronous
     * functions before any of those functions are actually supposed to be
     * called.
     * </p>
     * 
     * <p>
     * Due to limitations of Java generics, this method only supports a list of
     * functions where the return type of each function is compatible with the
     * input type of each function. However, this is not a limitation of the
     * implementation. When raw types are used, this method can work with
     * arbitrary asynchronous functions as long as the return type of each
     * function is compatible with the input type of the next function. For
     * concatenating functions with different types in a type-safe way, consider
     * using {@link #compose(Executor, AsyncFunction, AsyncFunction)} instead.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <T>
     *            input and output type of the resulting function. The input and
     *            output types of the supplied <code>functions</code> must be
     *            compatible with this type.
     * @param executor
     *            executor used for calling the functions'
     *            <code>apply(...)</code> methods.
     * @param functions
     *            functions to be encapsulated in the returned function. The
     *            functions are executed in series when the returned function is
     *            called.
     * @return asynchronous function that represents a combination of the
     *         specified functions, each one run after the previous one has
     *         completed.
     * @throws NullPointerException
     *             if the specified <code>executor</code> is <code>null</code>
     *             and if any of the specified <code>functions</code> are
     *             <code>null</code>.
     */
    @SafeVarargs
    public static <T> AsyncFunction<T, T> compose(Executor executor,
            AsyncFunction<? super T, ? extends T>... functions) {
        // The functions array is evaluated later when the returned function is
        // called. Therefore, we create a copy so that modifications of the
        // source do not have undesired effects later. As a side effect, this
        // will also throw a NullPointerException if one of the elements is
        // null, which is better than throwing the exception later when the
        // returned function is called.
        return composeInternal(executor, ImmutableList.copyOf(functions));
    }

    /**
     * <p>
     * Creates an asynchronous function that calls the specified asynchronous
     * functions in series.
     * </p>
     * 
     * <p>
     * The returned function will call the specified functions in the specified
     * order, passing the result of each function to the following one. The
     * first of the specified functions receives the input passed to the
     * returned function and the returned function returns the output returned
     * by the last of the specified functions.
     * </p>
     * 
     * <p>
     * Each of the functions' {@link AsyncFunction#apply(Object) apply(...)}
     * method is called in the thread that completes the future returned by the
     * previous function. The first function is called in the thread that calls
     * the returned function. For this reason, the supplied asynchronous
     * functions should not block or perform a considerable amount of work.
     * Instead, they should quickly return a future and perform all actual work
     * in a separate thread that completes the future when the work has
     * finished.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction, Executor)}, but
     * it works on asynchronous functions instead of futures and can take
     * several functions. Therefore, it is suitable for building an asynchronous
     * function that represents the concatenation of other asynchronous
     * functions before any of those functions are actually supposed to be
     * called.
     * </p>
     * 
     * <p>
     * Due to limitations of Java generics, this method only supports a list of
     * functions where the return type of each function is compatible with the
     * input type of each function. However, this is not a limitation of the
     * implementation. When raw types are used, this method can work with
     * arbitrary asynchronous functions as long as the return type of each
     * function is compatible with the input type of the next function. For
     * concatenating functions with different types in a type-safe way, consider
     * using {@link #compose(AsyncFunction, AsyncFunction)} instead.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <T>
     *            input and output type of the resulting function. The input and
     *            output types of the supplied <code>functions</code> must be
     *            compatible with this type.
     * @param functions
     *            functions to be encapsulated in the returned function. The
     *            functions are executed in series when the returned function is
     *            called.
     * @return asynchronous function that represents a combination of the
     *         specified functions, each one run after the previous one has
     *         completed.
     * @throws NullPointerException
     *             if any of the elements returned by the passed iterable are
     *             <code>null</code>.
     */
    public static <T> AsyncFunction<T, T> compose(
            Iterable<? extends AsyncFunction<? super T, ? extends T>> functions) {
        return compose(MoreExecutors.sameThreadExecutor(), functions);
    }

    /**
     * <p>
     * Creates an asynchronous function that calls the specified asynchronous
     * functions in series.
     * </p>
     * 
     * <p>
     * The returned function will call the specified functions in the specified
     * order, passing the result of each function to the following one. The
     * first of the specified functions receives the input passed to the
     * returned function and the returned function returns the output returned
     * by the last of the specified functions.
     * </p>
     * 
     * <p>
     * The functions' {@link AsyncFunction#apply(Object) apply(...)} method is
     * called through means of the specified <code>executor</code>, except for
     * the first of the specified functions which is always called in the thread
     * that calls the returned function.
     * </p>
     * 
     * <p>
     * This method is similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction, Executor)}, but
     * it works on asynchronous functions instead of futures and can take
     * several functions. Therefore, it is suitable for building an asynchronous
     * function that represents the concatenation of other asynchronous
     * functions before any of those functions are actually supposed to be
     * called.
     * </p>
     * 
     * <p>
     * Due to limitations of Java generics, this method only supports a list of
     * functions where the return type of each function is compatible with the
     * input type of each function. However, this is not a limitation of the
     * implementation. When raw types are used, this method can work with
     * arbitrary asynchronous functions as long as the return type of each
     * function is compatible with the input type of the next function. For
     * concatenating functions with different types in a type-safe way, consider
     * using {@link #compose(Executor, AsyncFunction, AsyncFunction)} instead.
     * </p>
     * 
     * <p>
     * The asynchronous function returned by this method works in a completely
     * asynchronous way: When called, the function returns a listenable future
     * that completes when all functions have completed or fails if one of the
     * functions has failed. Each function is called asynchronously once the
     * future returned by the previous function has completed.
     * </p>
     * 
     * @param <T>
     *            input and output type of the resulting function. The input and
     *            output types of the supplied <code>functions</code> must be
     *            compatible with this type.
     * @param executor
     *            executor used for calling the functions'
     *            <code>apply(...)</code> methods.
     * @param functions
     *            functions to be encapsulated in the returned function. The
     *            functions are executed in series when the returned function is
     *            called.
     * @return asynchronous function that represents a combination of the
     *         specified functions, each one run after the previous one has
     *         completed.
     * @throws NullPointerException
     *             if the specified <code>executor</code> is <code>null</code>
     *             and if any of the elements returned by the passed iterable
     *             are <code>null</code>.
     */
    public static <T> AsyncFunction<T, T> compose(Executor executor,
            Iterable<? extends AsyncFunction<? super T, ? extends T>> functions) {
        // The functions array is evaluated later when the returned function is
        // called. Therefore, we create a copy so that modifications of the
        // source do not have undesired effects later. As a side effect, this
        // will also throw a NullPointerException if one of the elements is
        // null, which is better than throwing the exception later when the
        // returned function is called.
        return composeInternal(executor, ImmutableList.copyOf(functions));
    }

    private static <T> AsyncFunction<T, T> composeInternal(
            final Executor executor,
            final List<AsyncFunction<? super T, ? extends T>> functions) {
        Preconditions.checkNotNull(executor);
        return new AsyncFunction<T, T>() {
            @Override
            public ListenableFuture<T> apply(final T input) throws Exception {
                ListenableFuture<T> lastFuture = Futures.immediateFuture(input);
                for (AsyncFunction<? super T, ? extends T> function : functions) {
                    // The first function is always executed in the calling
                    // thread because an immediate future executes its listeners
                    // right when they are added.
                    lastFuture = Futures.transform(lastFuture, function,
                            executor);
                }
                return lastFuture;
            }
        };
    }

    /**
     * Returns the identity function. The identity function simply returns its
     * input (wrapped in an immediate future).
     * 
     * @param <T>
     *            input and output type of the resulting function.
     * @return identity function.
     */
    public static <T> AsyncFunction<T, T> identity() {
        // We can safely do the unchecked cast because the identity function
        // always returns its input and thus it will always work correctly,
        // regardless of the generic types.
        @SuppressWarnings("unchecked")
        AsyncFunction<T, T> function = (AsyncFunction<T, T>) IDENTITY_FUNCTION;
        return function;
    }

    /**
     * <p>
     * Creates an asynchronous function that only executes another asynchronous
     * function if a condition is met.
     * </p>
     * 
     * <p>
     * When called, the returned asynchronous function evaluates the specified
     * predicate. If the predicate returns <code>true</code>, the specified
     * function is called and its result is returned. If it is
     * <code>false</code>, the input passed to the returned function is returned
     * directly (wrapping it in an immediate future).
     * </p>
     * 
     * <p>
     * The input passed to the returned function when it is called is passed to
     * both the predicate and the specified function (if the latter is called).
     * </p>
     * 
     * @param <T>
     *            input and output type of the resulting function. This type
     *            must be compatible with the input and output types of the
     *            specified <code>predicate</code> and <code>function</code>.
     * @param predicate
     *            predicate to be tested when the returned function is called.
     * @param function
     *            function to be called when the returned function is called and
     *            the specified <code>predicate</code> is <code>true</code>.
     * @return asynchronous function that only calls the specified asynchronous
     *         function if the specified predicate is <code>true</code>.
     * @throws NullPointerException
     *             if any of the specified arguments is <code>null</code>.
     */
    public static <T> AsyncFunction<T, T> ifThen(
            final Predicate<? super T> predicate,
            final AsyncFunction<? super T, T> function) {
        return new AsyncFunction<T, T>() {
            @Override
            public ListenableFuture<T> apply(T input) throws Exception {
                if (predicate.apply(input)) {
                    return function.apply(input);
                } else {
                    return Futures.immediateFuture(input);
                }
            }
        };
    }

    /**
     * <p>
     * Creates an asynchronous function that executes either of two asynchronous
     * functions based on a predicate.
     * </p>
     * 
     * <p>
     * When called, the returned asynchronous function evaluates the specified
     * predicate. If the predicate returns <code>true</code>, the first of the
     * specified functions is called and its result is returned. If the
     * predicate is <code>false</code>, the second of the specified functions is
     * called and its result is returned.
     * </p>
     * 
     * <p>
     * The input passed to the returned function when it is called is passed to
     * both the predicate and the function that is selected based on the
     * predicate.
     * </p>
     * 
     * @param <I>
     *            input type of the resulting function. This type must be
     *            compatible with the input types of the specified
     *            <code>predicate</code>, <code>functionIfTrue</code>, and
     *            <code>functionIfFalse</code>.
     * @param <O>
     *            output type of the resulting function. This type must be
     *            compatible with the output types of the specified
     *            <code>functionIfTrue</code> and <code>functionIfFalse</code>.
     * @param predicate
     *            predicate to be tested when the returned function is called.
     * @param functionIfTrue
     *            function to be called when the returned function is called and
     *            the specified <code>predicate</code> is <code>true</code>.
     * @param functionIfFalse
     *            function to be called when the returned function is called and
     *            the specified <code>predicate</code> is <code>false</code>.
     * @return asynchronous function that calls one of two specified
     *         asynchronous functions based on the specified predicate.
     * @throws NullPointerException
     *             if any of the specified arguments is <code>null</code>.
     */
    public static <I, O> AsyncFunction<I, O> ifThenElse(
            final Predicate<? super I> predicate,
            final AsyncFunction<? super I, O> functionIfTrue,
            final AsyncFunction<? super I, O> functionIfFalse) {
        Preconditions.checkNotNull(predicate);
        Preconditions.checkNotNull(functionIfTrue);
        Preconditions.checkNotNull(functionIfFalse);
        return new AsyncFunction<I, O>() {
            @Override
            public ListenableFuture<O> apply(I input) throws Exception {
                if (predicate.apply(input)) {
                    return functionIfTrue.apply(input);
                } else {
                    return functionIfFalse.apply(input);
                }
            }
        };
    }

}
