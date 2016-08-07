/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Helper methods for dealing with futures without having to take care of
 * checked exceptions.
 * 
 * @author Sebastian Marsching
 */
public abstract class FutureUtils {

    /**
     * Implementation of a chaining future that is used by the
     * <code>transform(...)</code> methods. This implementation is inspired by
     * the implementation used in Guava's {@link Futures} utility.
     * 
     * @author Sebastian Marsching
     *
     * @param <I>
     *            type of the input future.
     * @param <O>
     *            type of the output (returned) future.
     */
    private static class ChainingFuture<I, O> extends AbstractFuture<O> {

        private class OutputFutureCallback implements FutureCallback<O> {
            @Override
            public void onSuccess(O result) {
                try {
                    set(result);
                } finally {
                    outputFuture = null;
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    if (t instanceof CancellationException) {
                        cancel(false);
                    } else {
                        setException(t);
                    }
                } finally {
                    outputFuture = null;
                }
            }
        };

        private AsyncFunction<? super Throwable, ? extends O> errorFunction;
        private ListenableFuture<? extends I> inputFuture;
        private ListenableFuture<? extends O> outputFuture;
        private AsyncFunction<? super I, ? extends O> successFunction;

        public ChainingFuture(ListenableFuture<? extends I> inputFuture,
                AsyncFunction<? super I, ? extends O> onSuccess,
                AsyncFunction<? super Throwable, ? extends O> onError,
                Executor executor) {
            this.inputFuture = inputFuture;
            this.errorFunction = onError;
            this.successFunction = onSuccess;
            Futures.addCallback(this.inputFuture, new FutureCallback<I>() {
                @Override
                public void onSuccess(I result) {
                    ChainingFuture.this.inputFuture = null;
                    ListenableFuture<? extends O> outputFuture;
                    try {
                        outputFuture = ChainingFuture.this.successFunction
                                .apply(result);
                    } catch (Throwable t) {
                        outputFuture = Futures.immediateFailedFuture(t);
                    }
                    ChainingFuture.this.outputFuture = outputFuture;
                    Futures.addCallback(outputFuture,
                            new OutputFutureCallback());
                }

                @Override
                public void onFailure(Throwable t) {
                    // If we get an exception because the input future has been
                    // cancelled, we do not want to process this exception.
                    // Instead, we also cancel the this future.
                    if (ChainingFuture.this.inputFuture.isCancelled()) {
                        ChainingFuture.this.inputFuture = null;
                        ChainingFuture.this.cancel(false);
                        return;
                    }
                    ChainingFuture.this.inputFuture = null;
                    ListenableFuture<? extends O> outputFuture;
                    try {
                        outputFuture = ChainingFuture.this.errorFunction
                                .apply(t);
                    } catch (Throwable t2) {
                        outputFuture = Futures.immediateFailedFuture(t2);
                    }
                    ChainingFuture.this.outputFuture = outputFuture;
                    Futures.addCallback(outputFuture,
                            new OutputFutureCallback());
                }
            }, executor);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (super.cancel(mayInterruptIfRunning)) {
                cancel(inputFuture, mayInterruptIfRunning);
                cancel(outputFuture, mayInterruptIfRunning);
                return true;
            } else {
                return false;
            }
        }

        private void cancel(ListenableFuture<?> future,
                boolean mayInterruptIfRunning) {
            if (future != null) {
                future.cancel(mayInterruptIfRunning);
            }
        }

    }

    /**
     * Adapter that presents a <code>ListenableFuture</code> from Spring as a
     * <code>ListenableFuture</code> from Guava. Used by
     * {@link FutureUtils#asListenableFuture(ListenableFuture)}.
     * 
     * @author Sebastian Marsching
     *
     * @param <T>
     *            type of the future.
     */
    private static class GuavaListenableFutureAdapter<T> implements
            ListenableFuture<T> {

        private ExecutionList executionList = new ExecutionList();
        private org.springframework.util.concurrent.ListenableFuture<? extends T> future;

        public GuavaListenableFutureAdapter(
                org.springframework.util.concurrent.ListenableFuture<? extends T> future) {
            this.future = future;
            this.future
                    .addCallback(new org.springframework.util.concurrent.ListenableFutureCallback<T>() {
                        @Override
                        public void onSuccess(T result) {
                            executionList.execute();
                        }

                        @Override
                        public void onFailure(Throwable ex) {
                            executionList.execute();
                        }
                    });
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            executionList.add(listener, executor);
        }

    }

    /**
     * Adapter that presents a <code>ListenableFuture</code> from Guava as a
     * <code>ListenableFuture</code> from Spring. Used by
     * {@link FutureUtils#asSpringListenableFuture(ListenableFuture)}.
     * 
     * @author Sebastian Marsching
     *
     * @param <T>
     *            type of the future.
     */
    private static class SpringListenableFutureAdapter<T> implements
            org.springframework.util.concurrent.ListenableFuture<T> {

        private org.springframework.util.concurrent.ListenableFutureCallbackRegistry<T> callbacks = new org.springframework.util.concurrent.ListenableFutureCallbackRegistry<T>();
        private ListenableFuture<? extends T> future;

        public SpringListenableFutureAdapter(
                ListenableFuture<? extends T> future) {
            this.future = future;
            this.future.addListener(new Runnable() {
                @Override
                public void run() {
                    Throwable t;
                    try {
                        T result = SpringListenableFutureAdapter.this.future
                                .get();
                        callbacks.success(result);
                        return;
                    } catch (ExecutionException e) {
                        t = e.getCause();
                        if (t == null) {
                            t = e;
                        }
                    } catch (InterruptedException e) {
                        // We do not expect an InterruptedException because the
                        // future should have completed when this listener is
                        // called.
                        Thread.currentThread().interrupt();
                        t = new RuntimeException(
                                "Unexpected InterruptedException.", e);
                    } catch (Throwable t1) {
                        t = t1;
                    }
                    callbacks.failure(t);
                }
            }, MoreExecutors.sameThreadExecutor());
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }

        @Override
        public void addCallback(ListenableFutureCallback<? super T> callback) {
            callbacks.addCallback(callback);
        }

        @Override
        public void addCallback(SuccessCallback<? super T> successCallback,
                FailureCallback failureCallback) {
            callbacks.addSuccessCallback(successCallback);
            callbacks.addFailureCallback(failureCallback);
        }

    }

    private static final Function<Object, Void> FUNCTION_RETURN_NULL = Functions
            .constant(null);

    private static <V> FutureCallback<V> forwardingCallback(
            final SettableFuture<? super V> future) {
        // If the source future is cancelled, we cancel the target future.
        // Unfortunately, there is no reasonable way to do it the other way
        // round: We could register a listener with the target future to find
        // out when it has been cancelled, but we cannot know the value of the
        // mayInterruptIfRunning flag and we would need to avoid causing an
        // infinite loop.
        return new FutureCallback<V>() {
            @Override
            public void onSuccess(V result) {
                future.set(result);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof CancellationException) {
                    // For a SettableFuture the mayInterruptIfRunning flag does
                    // not make a difference.
                    future.cancel(false);
                } else {
                    future.setException(t);
                }
            }
        };
    }

    /**
     * Wraps a {@link org.springframework.util.concurrent.ListenableFuture} in a
     * {@link com.google.common.util.concurrent.ListenableFuture}. This way, a
     * listenable future that was created by code using Spring can be used with
     * code that expects a listenable future that is compatible with Guava. All
     * methods from the {@link Future} interface are directly delegated to the
     * original future. The method for adding callbacks registers callbacks
     * internally that are executed when the original future executes its
     * callbacks.
     * 
     * @param <T>
     *            type of the value provided by the returned future.
     * @param future
     *            listenable future from the Spring library.
     * @return listenable future compatible with Guava, that wraps the specified
     *         <code>future</code>.
     * @throws NullPointerException
     *             if <code>future</code> is <code>null</code>.
     */
    public static <T> ListenableFuture<T> asListenableFuture(
            org.springframework.util.concurrent.ListenableFuture<? extends T> future) {
        Preconditions.checkNotNull(future);
        return new GuavaListenableFutureAdapter<T>(future);
    }

    /**
     * Wraps a {@link com.google.common.util.concurrent.ListenableFuture} in a
     * {@link org.springframework.util.concurrent.ListenableFuture}. This way, a
     * listenable future that was created by code using Guava can be used with
     * code that expects a listenable future that is compatible with Spring. All
     * methods from the {@link Future} interface are directly delegated to the
     * original future. The methods for adding callbacks register callbacks
     * internally that are executed when the original future executes its
     * callbacks.
     * 
     * @param <T>
     *            type of the value provided by the returned future.
     * @param future
     *            listenable future from the Guava library.
     * @return listenable future compatible with Spring, that wraps the
     *         specified <code>future</code>.
     * @throws NullPointerException
     *             if <code>future</code> is <code>null</code>.
     */
    public static <T> org.springframework.util.concurrent.ListenableFuture<T> asSpringListenableFuture(
            ListenableFuture<? extends T> future) {
        Preconditions.checkNotNull(future);
        return new SpringListenableFutureAdapter<T>(future);
    }

    /**
     * <p>
     * Forwards the result of a listenable future to another (settable) future.
     * When the source future succeeds, the target future succeeds with the
     * result value of the source future. When the source future fails, the
     * target future fails with the exception of the source future.
     * </p>
     * 
     * <p>
     * When the source future is cancelled successfully (and it throws a
     * {@link CancellationException}), the target future is cancelled as well.
     * If the target future is cancelled, however, this does not have any effect
     * on the source future.
     * </p>
     * 
     * @param <V>
     *            type of the value provided by the futures. The type returned
     *            by the <code>source</code> must be compatible with the type
     *            expected by the <code>target</code>.
     * @param source
     *            listenable future that provides the source (result or error).
     * @param target
     *            settable future that is updated with the result or error of
     *            the source future, once the source future completes.
     */
    public static <V> void forward(ListenableFuture<? extends V> source,
            SettableFuture<? super V> target) {
        Preconditions.checkNotNull(source);
        Preconditions.checkNotNull(target);
        // Setting the result on the target future is a quick operation, so we
        // can run it using the same-thread executor.
        Futures.addCallback(source, forwardingCallback(target));
    }

    /**
     * <p>
     * Returns the result of calling the future's {@link Future#get() get()}
     * method.
     * </p>
     * 
     * <p>
     * If the <code>get()</code> method throws an {@link ExecutionException},
     * the cause attached to this exception is extracted. If the cause is a
     * {@link RuntimeException} or an {@link Error}, the cause is rethrown. If
     * it is a checked exception, it is wrapped in a {@link RuntimeException}
     * and thrown. If the cause is null, the {@link ExecutionException} is
     * wrapped in a {@link RuntimeException} and thrown.
     * </p>
     * 
     * <p>
     * If the <code>get()</code> method throws an {@link InterruptedException},
     * the current thread's interruption state is restored by calling
     * {@link Thread#interrupt()} and the InterruptedException is wrapped in a
     * {@link RuntimeException} and thrown.
     * </p>
     * 
     * @param <T>
     *            type of the value provided by the <code>future</code>.
     * @param future
     *            future from which the result shall be retrieved.
     * @return return value of {@link Future#get()}.
     * @throws NullPointerException
     *             if <code>future</code> is <code>null</code>.
     * @throws RuntimeException
     *             if {@link Future#get()} throws an exception.
     */
    public static <T> T getUnchecked(Future<? extends T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "An asynchronous operation was interrupted before it could complete.",
                    e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else if (cause != null) {
                throw new RuntimeException(
                        "An asynchronous operation failed"
                                + (cause.getMessage() != null ? (": " + cause.getMessage())
                                        : "."), cause);
            } else {
                throw new RuntimeException("An asynchronous operation failed.",
                        e);
            }
        }
    }

    /**
     * <p>
     * Transforms a listenable future using the specified synchronous functions.
     * This is very similar to
     * {@link Futures#transform(ListenableFuture, Function, Executor)}, but in
     * addition to providing a transformation of a result value, exceptions
     * thrown by the original future can be transformed as well.
     * </p>
     * 
     * <p>
     * If the specified future completes successfully, the
     * <code>successFunction</code> is called with the value of the original
     * future and the value returned by that function is provided by the
     * returned future. If the specified future completes with an exception, the
     * <code>errorFunction</code> is called, passing the exception. This
     * function can then convert this exception to a value that is returned by
     * the future returned by this method.
     * </p>
     * 
     * <p>
     * The <code>successFunction</code> and <code>errorFunction</code> are
     * executed in the thread that executes the listeners attached to the
     * original <code>input</code> future. Therefore, the functions should only
     * perform short, non-blocking tasks.
     * </p>
     * 
     * <p>
     * If the <code>successFunction</code> or the <code>errorFunction</code>
     * throw an exception, that exception is thrown by the future returned by
     * this method.
     * </p>
     * 
     * <p>
     * The implementation tries to keep the cancellation state between the
     * original future and the transformed future in sync. It cancels the
     * returned future when it detects that the original future has been
     * cancelled and it tries to cancel the original future when the returned
     * future is cancelled. Cancellation of the original future does not result
     * in the <code>errorFunction</code> being called.
     * </p>
     * 
     * @param <I>
     *            type of the value provided by the <code>input</code> future.
     * @param <O>
     *            type of the value provided by the output (resulting) future.
     *            Typically, this type is inferred from the returned types of
     *            the <code>successFunction</code> and
     *            <code>errorFunction</code>.
     * @param input
     *            future to be transformed.
     * @param successFunction
     *            function called when the <code>input</code> future completes
     *            successfully. The function gets the result of the
     *            <code>input</code> future passed to it.
     * @param errorFunction
     *            function called when the <code>input</code> future completes
     *            with an exception. The functions gets the exception thrown by
     *            the <code>input</code> future passed to it.
     * @return future that completes when the <code>successFunction</code> or
     *         <code>errorFunction</code> completes and providing the value or
     *         exception returned by that function.
     * @throws NullPointerException
     *             if any of the arguments are <code>null</code>.
     */
    public static <I, O> ListenableFuture<O> transform(
            ListenableFuture<I> input,
            Function<? super I, ? extends O> successFunction,
            Function<? super Throwable, ? extends O> errorFunction) {
        return transform(input,
                AsyncFunctionUtils.asAsyncFunction(successFunction),
                AsyncFunctionUtils.asAsyncFunction(errorFunction),
                MoreExecutors.sameThreadExecutor());
    }

    /**
     * <p>
     * Transforms a listenable future using the specified synchronous functions.
     * This is very similar to
     * {@link Futures#transform(ListenableFuture, Function, Executor)}, but in
     * addition to providing a transformation of a result value, exceptions
     * thrown by the original future can be transformed as well.
     * </p>
     * 
     * <p>
     * If the specified future completes successfully, the
     * <code>successFunction</code> is called with the value of the original
     * future and the value returned by that function is provided by the
     * returned future. If the specified future completes with an exception, the
     * <code>errorFunction</code> is called, passing the exception. This
     * function can then convert this exception to a value that is returned by
     * the future returned by this method.
     * </p>
     * 
     * <p>
     * The <code>successFunction</code> and <code>errorFunction</code> are
     * executed through the means of the specified <code>executor</code>.
     * </p>
     * 
     * <p>
     * If the <code>successFunction</code> or the <code>errorFunction</code>
     * throw an exception, that exception is thrown by the future returned by
     * this method.
     * </p>
     * 
     * <p>
     * The implementation tries to keep the cancellation state between the
     * original future and the transformed future in sync. It cancels the
     * returned future when it detects that the original future has been
     * cancelled and it tries to cancel the original future when the returned
     * future is cancelled. Cancellation of the original future does not result
     * in the <code>errorFunction</code> being called.
     * </p>
     * 
     * @param <I>
     *            type of the value provided by the <code>input</code> future.
     * @param <O>
     *            type of the value provided by the output (resulting) future.
     *            Typically, this type is inferred from the returned types of
     *            the <code>successFunction</code> and
     *            <code>errorFunction</code>.
     * @param input
     *            future to be transformed.
     * @param successFunction
     *            function called when the <code>input</code> future completes
     *            successfully. The function gets the result of the
     *            <code>input</code> future passed to it.
     * @param errorFunction
     *            function called when the <code>input</code> future completes
     *            with an exception. The functions gets the exception thrown by
     *            the <code>input</code> future passed to it.
     * @param executor
     *            executor used to run the <code>sucessFunction</code> and
     *            <code>errorFunction</code>.
     * @return future that completes when the <code>successFunction</code> or
     *         <code>errorFunction</code> completes and providing the value or
     *         exception returned by that function.
     * @throws NullPointerException
     *             if any of the arguments are <code>null</code>.
     */
    public static <I, O> ListenableFuture<O> transform(
            ListenableFuture<I> input,
            Function<? super I, ? extends O> successFunction,
            Function<? super Throwable, ? extends O> errorFunction,
            Executor executor) {
        return transform(input,
                AsyncFunctionUtils.asAsyncFunction(successFunction),
                AsyncFunctionUtils.asAsyncFunction(errorFunction), executor);
    }

    /**
     * <p>
     * Transforms a listenable future using the specified asynchronous
     * functions. This is very similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction)}, but in
     * addition to providing a transformation of a result value, exceptions
     * thrown by the original future can be transformed as well.
     * </p>
     * 
     * <p>
     * If the specified future completes successfully, the
     * <code>successFunction</code> is called with the value of the original
     * future and the value returned by that function is provided by the
     * returned future. If the specified future completes with an exception, the
     * <code>errorFunction</code> is called, passing the exception. This
     * function can then convert this exception to a value that is returned by
     * the future returned by this method.
     * </p>
     * 
     * <p>
     * The <code>successFunction</code> and <code>errorFunction</code> are
     * executed in the thread that executes the listeners attached to the
     * original <code>input</code> future. Therefore, the functions should only
     * perform short, non-blocking tasks.
     * </p>
     * 
     * <p>
     * If the <code>successFunction</code> or the <code>errorFunction</code>
     * throw an exception, that exception is thrown by the future returned by
     * this method.
     * </p>
     * 
     * <p>
     * The implementation tries to keep the cancellation state between the
     * original future and the transformed future in sync. It cancels the
     * returned future when it detects that the original future has been
     * cancelled and it tries to cancel the original future when the returned
     * future is cancelled. Cancellation of the original future does not result
     * in the <code>errorFunction</code> being called.
     * </p>
     * 
     * @param <I>
     *            type of the value provided by the <code>input</code> future.
     * @param <O>
     *            type of the value provided by the output (resulting) future.
     *            Typically, this type is inferred from the returned types of
     *            the <code>successFunction</code> and
     *            <code>errorFunction</code>.
     * @param input
     *            future to be transformed.
     * @param successFunction
     *            function called when the <code>input</code> future completes
     *            successfully. The function gets the result of the
     *            <code>input</code> future passed to it.
     * @param errorFunction
     *            function called when the <code>input</code> future completes
     *            with an exception. The functions gets the exception thrown by
     *            the <code>input</code> future passed to it.
     * @return future that completes when the future returned by the
     *         <code>successFunction</code> or <code>errorFunction</code>
     *         completes and providing the value or exception returned by that
     *         future.
     * @throws NullPointerException
     *             if any of the arguments are <code>null</code>.
     */
    public static <I, O> ListenableFuture<O> transform(
            ListenableFuture<I> input,
            AsyncFunction<? super I, ? extends O> successFunction,
            AsyncFunction<? super Throwable, ? extends O> errorFunction) {
        return transform(input, successFunction, errorFunction,
                MoreExecutors.sameThreadExecutor());
    }

    /**
     * <p>
     * Transforms a listenable future using the specified asynchronous
     * functions. This is very similar to
     * {@link Futures#transform(ListenableFuture, AsyncFunction, Executor)}, but
     * in addition to providing a transformation of a result value, exceptions
     * thrown by the original future can be transformed as well.
     * </p>
     * 
     * <p>
     * If the specified future completes successfully, the
     * <code>successFunction</code> is called with the value of the original
     * future and the value returned by that function is provided by the
     * returned future. If the specified future completes with an exception, the
     * <code>errorFunction</code> is called, passing the exception. This
     * function can then convert this exception to a value that is returned by
     * the future returned by this method.
     * </p>
     * 
     * <p>
     * The <code>successFunction</code> and <code>errorFunction</code> are
     * executed through the means of the specified <code>executor</code>.
     * </p>
     * 
     * <p>
     * If the <code>successFunction</code> or the <code>errorFunction</code>
     * throw an exception, that exception is thrown by the future returned by
     * this method.
     * </p>
     * 
     * <p>
     * The implementation tries to keep the cancellation state between the
     * original future and the transformed future in sync. It cancels the
     * returned future when it detects that the original future has been
     * cancelled and it tries to cancel the original future when the returned
     * future is cancelled. Cancellation of the original future does not result
     * in the <code>errorFunction</code> being called.
     * </p>
     * 
     * @param <I>
     *            type of the value provided by the <code>input</code> future.
     * @param <O>
     *            type of the value provided by the output (resulting) future.
     *            Typically, this type is inferred from the returned types of
     *            the <code>successFunction</code> and
     *            <code>errorFunction</code>.
     * @param input
     *            future to be transformed.
     * @param successFunction
     *            function called when the <code>input</code> future completes
     *            successfully. The function gets the result of the
     *            <code>input</code> future passed to it.
     * @param errorFunction
     *            function called when the <code>input</code> future completes
     *            with an exception. The functions gets the exception thrown by
     *            the <code>input</code> future passed to it.
     * @param executor
     *            executor used to run the <code>sucessFunction</code> and
     *            <code>errorFunction</code>.
     * @return future that completes when the future returned by the
     *         <code>successFunction</code> or <code>errorFunction</code>
     *         completes and providing the value or exception returned by that
     *         future.
     * @throws NullPointerException
     *             if any of the arguments are <code>null</code>.
     */
    public static <I, O> ListenableFuture<O> transform(
            ListenableFuture<I> input,
            AsyncFunction<? super I, ? extends O> successFunction,
            AsyncFunction<? super Throwable, ? extends O> errorFunction,
            Executor executor) {
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(successFunction);
        Preconditions.checkNotNull(errorFunction);
        Preconditions.checkNotNull(executor);
        return new ChainingFuture<I, O>(input, successFunction, errorFunction,
                executor);
    }

    /**
     * <p>
     * Transforms a listenable future that returns any type to a listenable
     * future that returns {@link Void}. The future returned will always return
     * a value of <code>null</code>. Such a future is typically desirable, if
     * one has a future that returns an unknown type (or access to the actual
     * result is not supposed to be exposed in the API) and one only wants to
     * use the future to check when the operation has finished and to get any
     * error state associated with the operation.
     * </p>
     * 
     * <p>
     * The implementation tries to keep the cancellation state between the
     * original future and the transformed future in sync. It cancels the
     * returned future when it detects that the original future has been
     * cancelled and it tries to cancel the original future when the returned
     * future is cancelled.
     * </p>
     * 
     * @param future
     *            listenable future of any type.
     * @return listenable future returning <code>null</code> that finishes when
     *         the passed future finishes. If the passed future throws an
     *         exception, this future will throw the same exception.
     */
    public static ListenableFuture<Void> transformAnyToVoid(
            ListenableFuture<?> future) {
        return Futures.transform(future, FUNCTION_RETURN_NULL);
    }

}
