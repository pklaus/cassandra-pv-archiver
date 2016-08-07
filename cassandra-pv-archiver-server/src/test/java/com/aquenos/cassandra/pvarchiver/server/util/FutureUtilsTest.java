/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Tests for the {@link FutureUtils}.
 * 
 * @author Sebastian Marsching
 */
public class FutureUtilsTest {

    /**
     * Simple implementation of a failure callback for Spring's
     * <code>ListenableFuture</code>. Used by
     * {@link FutureUtilsTest#testAsSpringListenableFuture()}
     * 
     * @author Sebastian Marsching
     */
    private static class StoringFailureCallback implements
            org.springframework.util.concurrent.FailureCallback {

        public boolean done;
        public Throwable exception;

        @Override
        public void onFailure(Throwable ex) {
            // The callback should only be triggered once.
            assertFalse(done);
            this.exception = ex;
            this.done = true;
        }

    }

    /**
     * Simple implementation of a callback for Spring's
     * <code>ListenableFuture</code>. Used by
     * {@link FutureUtilsTest#testAsSpringListenableFuture()}
     * 
     * @author Sebastian Marsching
     */
    private static class StoringListenableFutureCallback<T> implements
            org.springframework.util.concurrent.ListenableFutureCallback<T> {

        public boolean done;
        public T result;
        public boolean success;
        public Throwable exception;

        @Override
        public void onSuccess(T result) {
            // The callback should only be triggered once.
            assertFalse(done);
            this.result = result;
            this.success = true;
            this.done = true;
        }

        @Override
        public void onFailure(Throwable ex) {
            // The callback should only be triggered once.
            assertFalse(done);
            this.exception = ex;
            this.success = false;
            this.done = true;
        }

    }

    /**
     * Simple implementation of a success callback for Spring's
     * <code>ListenableFuture</code>. Used by
     * {@link FutureUtilsTest#testAsSpringListenableFuture()}
     * 
     * @author Sebastian Marsching
     */
    private static class StoringSuccessCallback<T> implements
            org.springframework.util.concurrent.SuccessCallback<T> {

        public boolean done;
        public T result;

        @Override
        public void onSuccess(T result) {
            // The callback should only be triggered once.
            assertFalse(done);
            this.result = result;
            this.done = true;
        }

    }

    /**
     * Tests the
     * {@link FutureUtils#asListenableFuture(org.springframework.util.concurrent.ListenableFuture)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testAsListenableFuture() throws Exception {
        ListenableFuture<String> source;
        // We use a settable future together with forward because this is the
        // easiest way to test that the callbacks are working correctly. This
        // allows us to simply convert to a Spring listenable future and back to
        // a Guava listenable future. This has the positive side effect that we
        // also test asSpringListenableFuture(...) indirectly.
        SettableFuture<String> target;
        // First, we test the success case.
        source = Futures.immediateFuture("test123");
        target = SettableFuture.create();
        FutureUtils.forward(FutureUtils.asListenableFuture(FutureUtils
                .asSpringListenableFuture(source)), target);
        assertFalse(target.isCancelled());
        assertTrue(target.isDone());
        assertEquals("test123", FutureUtils.getUnchecked(target));
        // Second, we test the failure case.
        Exception testException = new Exception("my test");
        source = Futures.immediateFailedFuture(testException);
        target = SettableFuture.create();
        FutureUtils.forward(FutureUtils.asListenableFuture(FutureUtils
                .asSpringListenableFuture(source)), target);
        assertFalse(target.isCancelled());
        assertTrue(target.isDone());
        Throwable caughtException = null;
        try {
            target.get();
        } catch (ExecutionException e) {
            caughtException = e.getCause();
        }
        assertEquals(testException, caughtException);
    }

    /**
     * Tests the {@link FutureUtils#asSpringListenableFuture(ListenableFuture)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testAsSpringListenableFuture() throws Exception {
        ListenableFuture<String> source;
        org.springframework.util.concurrent.ListenableFuture<String> target;
        StoringListenableFutureCallback<String> callback;
        StoringSuccessCallback<String> successCallback;
        StoringFailureCallback failureCallback;
        // First, we test the success case.
        source = Futures.immediateFuture("test123");
        target = FutureUtils.asSpringListenableFuture(source);
        callback = new StoringListenableFutureCallback<String>();
        successCallback = new StoringSuccessCallback<String>();
        failureCallback = new StoringFailureCallback();
        target.addCallback(callback);
        target.addCallback(successCallback, failureCallback);
        assertTrue(callback.done);
        assertTrue(callback.success);
        assertEquals("test123", callback.result);
        assertTrue(successCallback.done);
        assertEquals("test123", successCallback.result);
        assertFalse(failureCallback.done);
        // Second, we test the failure case.
        Exception testException = new Exception("my test");
        source = Futures.immediateFailedFuture(testException);
        target = FutureUtils.asSpringListenableFuture(source);
        callback = new StoringListenableFutureCallback<String>();
        successCallback = new StoringSuccessCallback<String>();
        failureCallback = new StoringFailureCallback();
        target.addCallback(callback);
        target.addCallback(successCallback, failureCallback);
        assertTrue(callback.done);
        assertFalse(callback.success);
        assertEquals(testException, callback.exception);
        assertFalse(successCallback.done);
        assertTrue(failureCallback.done);
        assertEquals(testException, failureCallback.exception);
    }

    /**
     * Tests the {@link FutureUtils#forward(ListenableFuture, SettableFuture)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testForward() throws Exception {
        ListenableFuture<String> source;
        SettableFuture<String> delayedSource;
        SettableFuture<String> target;
        // Test that the value from an immediate future is propagated.
        source = Futures.immediateFuture("test");
        target = SettableFuture.create();
        FutureUtils.forward(source, target);
        assertEquals("test", target.get());
        // Test that an exception is propagated.
        Exception testException = new Exception("test");
        source = Futures.immediateFailedFuture(testException);
        target = SettableFuture.create();
        FutureUtils.forward(source, target);
        Throwable caughtException = null;
        try {
            target.get();
        } catch (ExecutionException e) {
            caughtException = e.getCause();
        }
        assertEquals(testException, caughtException);
        // Test that the value from a future that completes later is propagated.
        delayedSource = SettableFuture.create();
        target = SettableFuture.create();
        FutureUtils.forward(delayedSource, target);
        assertFalse(target.isCancelled());
        assertFalse(target.isDone());
        delayedSource.set("123");
        assertFalse(target.isCancelled());
        assertTrue(target.isDone());
        assertEquals("123", target.get());
        // Test that the target is cancelled when the source is cancelled.
        delayedSource = SettableFuture.create();
        target = SettableFuture.create();
        FutureUtils.forward(delayedSource, target);
        assertFalse(target.isCancelled());
        assertFalse(target.isDone());
        delayedSource.cancel(false);
        assertTrue(target.isCancelled());
        assertTrue(target.isDone());
    }

    /**
     * Tests the {@link FutureUtils#getUnchecked(java.util.concurrent.Future)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testGetUnchecked() throws Exception {
        // Test the successful case.
        assertEquals("foo",
                FutureUtils.getUnchecked(Futures.immediateFuture("foo")));
        // Test the exception case with a checked exception.
        Exception checkedTestException = new Exception("xyz");
        Throwable caughtException = null;
        try {
            FutureUtils.getUnchecked(Futures
                    .immediateFailedFuture(checkedTestException));
        } catch (Throwable t) {
            caughtException = t;
        }
        assertTrue(caughtException instanceof RuntimeException);
        assertEquals(checkedTestException, caughtException.getCause());
        // Test the exception case with an unchecked exception.
        RuntimeException uncheckedTestException = new RuntimeException("abc");
        caughtException = null;
        try {
            FutureUtils.getUnchecked(Futures
                    .immediateFailedFuture(uncheckedTestException));
        } catch (Throwable t) {
            caughtException = t;
        }
        assertEquals(uncheckedTestException, caughtException);
        // Test the exception case with an error.
        Error testError = new Error("123");
        caughtException = null;
        try {
            FutureUtils.getUnchecked(Futures.immediateFailedFuture(testError));
        } catch (Throwable t) {
            caughtException = t;
        }
        assertEquals(testError, caughtException);
        // Test that interruption while waiting for the future to finish results
        // in the corresponding exception. We have to mark this thread as
        // interrupted so that get() returns immediately.
        boolean savedInterruptState = Thread.currentThread().isInterrupted();
        Thread.currentThread().interrupt();
        caughtException = null;
        try {
            FutureUtils.getUnchecked(SettableFuture.create());
        } catch (Throwable t) {
            caughtException = t;
        }
        assertTrue(Thread.currentThread().isInterrupted());
        assertTrue(caughtException instanceof RuntimeException);
        assertTrue(caughtException.getCause() instanceof InterruptedException);
        if (!savedInterruptState) {
            Thread.interrupted();
        }
    }

    /**
     * Tests the
     * {@link FutureUtils#transform(ListenableFuture, Function, Function)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testTransform() throws Exception {
        ListenableFuture<Integer> originalFuture;
        ListenableFuture<String> transformedFuture;
        SettableFuture<Integer> delayedFuture;
        Function<Integer, String> successFunction = new Function<Integer, String>() {
            @Override
            public String apply(Integer input) {
                if (input == null) {
                    return null;
                } else {
                    return input.toString();
                }
            }
        };
        Function<Throwable, String> errorFunction = new Function<Throwable, String>() {
            @Override
            public String apply(Throwable input) {
                return input.getMessage();
            }
        };
        // Test the successful case.
        originalFuture = Futures.immediateFuture(15);
        transformedFuture = FutureUtils.transform(originalFuture,
                successFunction, errorFunction);
        assertEquals("15", transformedFuture.get());
        // Test the error case.
        originalFuture = Futures.immediateFailedFuture(new Exception("abc"));
        transformedFuture = FutureUtils.transform(originalFuture,
                successFunction, errorFunction);
        assertEquals("abc", transformedFuture.get());
        // Test that cancellation propagates from the source to the target.
        delayedFuture = SettableFuture.create();
        transformedFuture = FutureUtils.transform(delayedFuture,
                successFunction, errorFunction);
        assertFalse(delayedFuture.isCancelled());
        assertFalse(delayedFuture.isDone());
        assertFalse(transformedFuture.isCancelled());
        assertFalse(transformedFuture.isDone());
        delayedFuture.cancel(false);
        assertTrue(delayedFuture.isCancelled());
        assertTrue(delayedFuture.isDone());
        assertTrue(transformedFuture.isCancelled());
        assertTrue(transformedFuture.isDone());
        // Test that cancellation propagates from the target to the source.
        delayedFuture = SettableFuture.create();
        transformedFuture = FutureUtils.transform(delayedFuture,
                successFunction, errorFunction);
        assertFalse(delayedFuture.isCancelled());
        assertFalse(delayedFuture.isDone());
        assertFalse(transformedFuture.isCancelled());
        assertFalse(transformedFuture.isDone());
        transformedFuture.cancel(false);
        assertTrue(delayedFuture.isCancelled());
        assertTrue(delayedFuture.isDone());
        assertTrue(transformedFuture.isCancelled());
        assertTrue(transformedFuture.isDone());
    }

    /**
     * Tests the {@link FutureUtils#transformAnyToVoid(ListenableFuture)}
     * method.
     * 
     * @throws Exception
     *             on error.
     */
    @Test
    public void testTransformAnyToVoid() throws Exception {
        // The transformAnyToVoid(...) method is so trivial, that there is not
        // really much that we can test (assuming that the backing
        // Futures.transform(...) works correctly. We just do a simple test for
        // coverage.
        assertNull(FutureUtils.transformAnyToVoid(
                Futures.immediateFuture("test")).get());
    }

}
