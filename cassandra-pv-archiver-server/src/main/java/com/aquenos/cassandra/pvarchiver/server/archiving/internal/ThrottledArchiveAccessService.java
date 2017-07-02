/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.aquenos.cassandra.pvarchiver.common.ObjectResultSet;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessService;
import com.aquenos.cassandra.pvarchiver.server.archiving.TimeStampLimitMode;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * <p>
 * Archive access service that enforces limits when fetching samples. This wraps
 * an {@link ArchiveAccessService} and limits how many samples may be fetched
 * concurrently. This helps to avoid a situation in which samples are fetched
 * more quickly than they can be processed, thus leading to memory exhaustion.
 * </p>
 * 
 * <p>
 * This class is intended to be used by the
 * {@link ArchivingServiceInternalImpl}, {@link ArchivedChannel}, and
 * {@link ArchivedChannelDecimatedSamplesDecimationLevel} classes only. For this
 * reason, it has been marked as package private.
 * </p>
 * 
 * <p>
 * As this class is designed for internal use only, most of its methods do not
 * verify that the state or the input parameters match the expectations. Some
 * verifications are made by assertions, but in general the calling code has to
 * ensure that the preconditions needed when calling the methods in this class
 * are met.
 * </p>
 * 
 * @author Sebastian Marsching
 */
class ThrottledArchiveAccessService {

    /**
     * <p>
     * Result set that allows skipping the remaining elements. The
     * {@link #skipRemaining()} method may be called to skip all elements that
     * are remaining in the result set. This can be used to free memory early.
     * After calling {@link #skipRemaining()}, the result set is exhausted.
     * </p>
     * 
     * @author Sebastian Marsching
     *
     * @param <V>
     *            type of the objects contained in the result set.
     */
    public static interface SkippableObjectResultSet<V>
            extends ObjectResultSet<V> {

        /**
         * Skip the remaining elements in this result set. This allows the
         * internal implementation to free memory that had been reserved for
         * elements that have already been fetched from the database, but have
         * not been consumed yet. After calling this method, the result set is
         * exhausted ({@link #isExhausted()} returns <code>true</code>).
         */
        void skipRemaining();

    }

    /**
     * Function for fetching more elements for a result set. This class is only
     * intended for internal use by
     * {@link InternalObjectResultSet#fetchMoreResults()}. The only reason why
     * it is not an anonymous class is to avoid having a strong reference from
     * the function to the result set. This function object only retains a weak
     * reference to its result set. This way, the function can be added to a
     * queue and the result set may still be garbage collected if no other code
     * is referring to it any longer.
     * 
     * @author Sebastian Marsching
     */
    private class FetchMoreResultsFunction
            implements AsyncFunction<Void, Void> {

        private final WeakReference<InternalObjectResultSet<?>> resultSetReference;

        public FetchMoreResultsFunction(InternalObjectResultSet<?> resultSet) {
            this.resultSetReference = new WeakReference<InternalObjectResultSet<?>>(
                    resultSet);
        }

        @Override
        public ListenableFuture<Void> apply(Void input) throws Exception {
            InternalObjectResultSet<?> resultSet = resultSetReference.get();
            if (resultSet == null) {
                // The fetch operation counter has been incremented before
                // calling this method, but we are never going to process any
                // results, so we have to decrement the operation counter now.
                decrementFetchOperationCounter();
                return Futures.immediateFuture(null);
            } else {
                // The referenced result set might have been destroyed
                // (remaining elements skipped) while we were waiting for the
                // fetch operation to be started. In this case, we do not want
                // to start the fetch operation and instead finish write away,
                // decrementing the operations counter.
                // If the result set is still active, there is still a chance
                // that it might be destroyed before the fetch operation is
                // finished. In this case, the result of the fetch operation
                // would never be processed and thus the operations counter
                // would never be decremented. Therefore, we set a flag
                // indicating that a fetch operation is now running. This way,
                // the counter will be decremented when the result set is
                // destroyed.
                ObjectResultSet<?> backingResultSet;
                synchronized (resultSet.lock) {
                    backingResultSet = resultSet.backingResultSet;
                    resultSet.fetchOperationRunning = backingResultSet != null;
                }
                if (backingResultSet == null) {
                    decrementFetchOperationCounter();
                    return Futures.immediateFuture(null);
                } else {
                    return resultSet.backingResultSet.fetchMoreResults();
                }
            }
        }

    }

    /**
     * <p>
     * Result set that wraps another result set and limits when more data may be
     * fetched. This result set has been specifically designed for internal use
     * by the {@link ThrottledArchiveAccessService}.
     * </p>
     * 
     * <p>
     * When trying to fetch more rows, it asks the surrounding
     * {@link ThrottledArchiveAccessService} to do so, so that the number of
     * rows concurrently kept in memory can be limited. When consuming rows (by
     * calling {@link #all()} or {@link #one()}, or using the iterator), it
     * updates the counters in the {@link ThrottledArchiveAccessService}
     * accordingly.
     * </p>
     * 
     * <p>
     * When the remaining rows in the result set are not needed, the
     * {@link #skipRemaining()} method should be called, so that data that has
     * already been fetched can be discarded and thus the memory is freed so
     * that other result sets can fetch rows. Eventually, this will also happen
     * when the result set is garbage collected, but it might take some time
     * until the result set is garbage collected and until then, its existence
     * might block other fetch operations.
     * </p>
     * 
     * @author Sebastian Marsching
     *
     * @param <V>
     *            type of the objects contained in the result set.
     */
    private class InternalObjectResultSet<V>
            implements SkippableObjectResultSet<V> {

        private ListenableFuture<Void> activeFetchFuture;
        private ObjectResultSet<V> backingResultSet;
        private boolean fetchOperationRunning;
        private final Object lock = new Object();
        private int numberOfRegisteredSamples;

        public InternalObjectResultSet(ObjectResultSet<V> backingResultSet) {
            this.backingResultSet = backingResultSet;
            // The samples that were automatically fetched when creating the
            // result set are already in memory, so we have to count them
            // towards the limit. We do not decrement the operation counter here
            // because this happens in the calling code.
            updateNumberOfRegisteredSamples();
        }

        @Override
        public List<V> all() {
            // Using all() will actually occupy a lot of memory because all rows
            // are fetched before returning. As we only use this class in a
            // well-known context, we know that all() will never be used when
            // there might actually be a lot of data. For this reason, we do not
            // count the data returned by this method towards the total limit.
            if (backingResultSet == null) {
                return Collections.emptyList();
            }
            // We do not have to call processActiveFetchFutureIfDone() here
            // because we reset the available rows anyway as we are going to
            // consume them all.
            // We fetch all samples from the result set, so the samples that
            // were previously remaining in the current page do not count
            // towards the limit any longer.
            decrementSamplesCounter(numberOfRegisteredSamples);
            numberOfRegisteredSamples = 0;
            boolean success = false;
            List<V> all;
            try {
                all = backingResultSet.all();
                success = true;
            } finally {
                // If all() throws an exception, there might still be rows in
                // the result set.
                if (!success) {
                    updateNumberOfRegisteredSamples();
                }
            }
            // We have to call cleanUp() because we want to lose the reference
            // to the backing result set and we also want a fetch operation that
            // might have been started before calling all() to be handled
            // correctly.
            cleanUp();
            return all;
        }

        @Override
        public ListenableFuture<Void> fetchMoreResults() {
            if (backingResultSet == null) {
                return Futures.immediateFuture(null);
            }
            processActiveFetchFutureIfDone();
            // We use the backing result set's isFullyFetched() method directly
            // because our version of isFullyFetched would again check whether
            // the backing result set is null, which is unnecessary.
            if (backingResultSet.isFullyFetched()) {
                return Futures.immediateFuture(null);
            }
            if (activeFetchFuture == null) {
                activeFetchFuture = runFetchOperation(
                        new FetchMoreResultsFunction(this));
            }
            return activeFetchFuture;
        }

        @Override
        public int getAvailableWithoutFetching() {
            processActiveFetchFutureIfDone();
            if (backingResultSet == null) {
                return 0;
            } else {
                return backingResultSet.getAvailableWithoutFetching();
            }
        }

        @Override
        public boolean isExhausted() {
            // We do not call backingResultSet.isExhausted() here because this
            // might trigger fetchMoreResults() in the backing result set, but
            // this method should never be called directly. Instead, our version
            // of fetchMoreResults() (that takes care of enforcing the limits)
            // has to be used.
            // We do not have to call processActiveFetchFuture() here because it
            // is called from getAvailableWithoutFetching().
            if (getAvailableWithoutFetching() != 0) {
                return false;
            }
            if (isFullyFetched()) {
                // If isFullyFetched() returns true, we have to check the number
                // of available rows again. The reason is, that new rows might
                // have been fetched between our calls to
                // getAvailableWithoutFetching() and isFullyFetched(). In this
                // case, getAvailableWithoutFetching() will return a non-zero
                // number now.
                return getAvailableWithoutFetching() == 0;
            }
            // If there are no more rows available without fetching but the
            // result set is not fully fetched either, we have to fetch a new
            // page. We do this explicitly by calling fetchMoreResults() because
            // this method will wait if the limit is currently exceeded.
            FutureUtils.getUnchecked(fetchMoreResults());
            return isExhausted();
        }

        @Override
        public boolean isFullyFetched() {
            if (backingResultSet == null) {
                return true;
            }
            return backingResultSet.isFullyFetched();
        }

        @Override
        public Iterator<V> iterator() {
            return new Iterator<V>() {
                @Override
                public boolean hasNext() {
                    return !isExhausted();
                }

                @Override
                public V next() {
                    if (isExhausted()) {
                        throw new NoSuchElementException();
                    }
                    V next = one();
                    return next;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public V one() {
            if (isExhausted()) {
                return null;
            }
            // isExhausted() automatically fetches the next page when
            // availableWithoutFetching is zero, so availableWithoutFetching
            // should always be non-zero when isExhausted returns false.
            assert (getAvailableWithoutFetching() > 0);
            try {
                return backingResultSet.one();
            } finally {
                updateNumberOfRegisteredSamples();
            }
        }

        @Override
        public void skipRemaining() {
            cleanUp();
        }

        @Override
        protected void finalize() throws Throwable {
            try {
                cleanUp();
            } finally {
                super.finalize();
            }
        }

        private void cleanUp() {
            if (backingResultSet == null) {
                return;
            }
            // We need to acquire a mutex while setting backingResultSet to
            // null and checking the fetchOperationRunning flag. This is needed
            // to avoid a race condition where the fetchOperationRunning flag is
            // set by another thread while we are cleaning up.
            boolean fetchOperationRunning;
            synchronized (lock) {
                backingResultSet = null;
                fetchOperationRunning = this.fetchOperationRunning;
                this.fetchOperationRunning = false;
            }
            // If a fetch operation is running, the operation counter has been
            // incremented. As the results are never going to be processed, we
            // have to decrement the counter now.
            if (fetchOperationRunning) {
                decrementFetchOperationCounter();
            }
            activeFetchFuture = null;
            if (numberOfRegisteredSamples != 0) {
                // We drop the reference to the result set which means that any
                // rows left in the current page do not count towards the limit
                // any longer because they can now be garbage collected.
                decrementSamplesCounter(numberOfRegisteredSamples);
                numberOfRegisteredSamples = 0;
            }
        }

        private void processActiveFetchFutureIfDone() {
            if (activeFetchFuture == null || !activeFetchFuture.isDone()) {
                return;
            }
            // If the fetch operation failed with an exception, the future will
            // throw this exception. This means that we have to call the
            // future's get() method here. If we did not, we would simply try to
            // fetch again and could end up in an endless loop if the fetch
            // operation keeps failing for some reason.
            // We check for the exception here because we need to know whether
            // there was an error when deciding whether to run another fetch
            // operation. However, we do not throw the exception here because we
            // have to clean up first, even if there is an error. The exception
            // is then thrown later in this method.
            RuntimeException fetchException = null;
            try {
                activeFetchFuture.get();
            } catch (InterruptedException e) {
                fetchException = new RuntimeException(
                        "The thread was interrupted while trying to fetch more samples.",
                        e);
            } catch (ExecutionException e) {
                fetchException = new RuntimeException(
                        "Fetching more samples failed:", e.getCause());
            }
            // If the number of available samples is zero or has not changed, it
            // is very likely that the last fetch operation returned zero
            // samples. This can happen because the underlying result-set
            // implementation might actually return a page of zero samples in
            // order to indicate that the method for fetching the next page
            // shall be called again. We cannot handle this situation in the
            // regular way, because the underlying implementation might actually
            // already keep these samples in memory, but not consider them as
            // available (in order to avoid issues with thread-safety). In this
            // case, the next fetch operation might be postponed significantly
            // and the memory usage might grow more than expected.
            // We only try to run another fetch operation if the backing result
            // set is not fully fetched yet and the last fetch operation did not
            // fail.
            int nowAvailable = backingResultSet.getAvailableWithoutFetching();
            if (fetchException == null && !backingResultSet.isFullyFetched()
                    && (nowAvailable == 0
                            || nowAvailable == numberOfRegisteredSamples)) {
                // As far as the number of concurrent fetch operations is
                // concerned, the last fetch operation still counts as running,
                // which is why we can start this one directly, bypassing the
                // regular queue.
                try {
                    activeFetchFuture = new FetchMoreResultsFunction(this)
                            .apply(null);
                } catch (Exception e) {
                    activeFetchFuture = Futures.immediateFailedFuture(e);
                }
                // The fetch operation might finish synchronously (actually,
                // this is the most likely case when the last one did not return
                // any samples). For this reason, we call this method again so
                // that an immediately finished future is processed right now.
                processActiveFetchFutureIfDone();
                return;
            }
            updateNumberOfRegisteredSamples();
            // We decrement the fetch operation counter after updating the row
            // counter. This way, a new fetch operation is only started if the
            // row limit has not been exceeded after taking the rows from the
            // last fetch operation into consideration.
            decrementFetchOperationCounter();
            activeFetchFuture = null;
            // This method may only be called by the same thread that might also
            // call cleanUp(), so we do not have to acquire the mutex here.
            fetchOperationRunning = false;
            // If the last fetch operation failed, we now throw the exception.
            // We do this here so that the active fetch future is cleaned up and
            // the fetch operation counter is decremented, even if there is an
            // error.
            if (fetchException != null) {
                throw fetchException;
            }
        }

        private void updateNumberOfRegisteredSamples() {
            int availableInBackingResultSet = backingResultSet
                    .getAvailableWithoutFetching();
            int difference = availableInBackingResultSet
                    - numberOfRegisteredSamples;
            if (difference < 0) {
                // There are less samples than we had registered, so we have to
                // decrement the counter.
                decrementSamplesCounter(-difference);
                numberOfRegisteredSamples = availableInBackingResultSet;
                return;
            }
            if (difference > 0) {
                // There are more samples than we had registered, so we have to
                // increment the counter.
                incrementSamplesCounter(difference);
                numberOfRegisteredSamples = availableInBackingResultSet;
                return;
            }
        }

    }

    private final ArchiveAccessService archiveAccessService;
    private final ExecutorService poolExecutor;
    private final AtomicInteger remainingRunningFetchOpsLimit;
    private final AtomicLong remainingSamplesLimit;
    private final int runningFetchOpsLimit;
    private final int samplesLimit;
    private final ConcurrentLinkedQueue<SettableFuture<Void>> waitingFetchOperations;

    /**
     * Creates a throttled archive access service that wraps the passed archive
     * access service. The passed pool executor is used for executing
     * asynchronous tasks (e.g. for processing internal queues). This throttles
     * archive access service limits the number of samples that may be fetched
     * (but not consumed yet) at a given point in time. The specified limit is
     * not a hard limit because number of rows that has been fetched is only
     * known after a fetch operation has completed. For this reason, other fetch
     * operations may have been started before the limit was exceeded, but might
     * finish after the limits has been exceeded, thus adding samples beyond the
     * specified limit. For this reason, the number of fetch operations that may
     * run concurrently (meaning they have been started but have not returned
     * data yet) can be limited, too.
     * 
     * @param archiveAccessService
     *            archive access service that is used to actually read samples.
     * @param poolExecutor
     *            executor service that provides the thread pool for executing
     *            asynchronous tasks.
     * @param runningFetchOperationLimit
     *            number of fetch operations that may run concurrently. This
     *            limit effectively also limits by how much the
     *            <code>samplesLimit</code> can be exceeded, because the maximum
     *            by which it can be exceeded is the number of concurrent fetch
     *            operations multiplied by the fetch size. This number must be
     *            greater than zero.
     * @param samplesLimit
     *            number of samples that may be kept in memory. If all the
     *            result sets provided by this service combined keep more
     *            samples (that have been fetched from the database, but have
     *            not yet been consumed), further fetch operations are delayed
     *            until enough of these samples have been consumed. This number
     *            must be greater than zero.
     * @throws IllegalArgumentException
     *             if <code>runningFetchOperationLimit</code> or
     *             <code>samplesLimit</code> is less than one.
     */
    public ThrottledArchiveAccessService(
            ArchiveAccessService archiveAccessService,
            ExecutorService poolExecutor, int runningFetchOperationLimit,
            int samplesLimit) {
        Preconditions.checkArgument(runningFetchOperationLimit > 0,
                "The pending fetch operations limit must be greater than zero.");
        Preconditions.checkArgument(samplesLimit > 0,
                "The row limit must be greater than zero.");
        this.runningFetchOpsLimit = runningFetchOperationLimit;
        this.samplesLimit = samplesLimit;
        this.archiveAccessService = archiveAccessService;
        this.poolExecutor = poolExecutor;
        this.remainingRunningFetchOpsLimit = new AtomicInteger(
                this.runningFetchOpsLimit);
        // The samples limit is an integer, but if a result set contains an
        // extremely large number of samples, the negative integer range might
        // be exceeded, so we use a long to be sure that the variable never
        // wraps around.
        this.remainingSamplesLimit = new AtomicLong(this.samplesLimit);
        this.waitingFetchOperations = new ConcurrentLinkedQueue<>();
    }

    /**
     * Returns the number of sample fetch operations that are running currently.
     * 
     * @return number of sample fetch operations that are currently running.
     */
    public int getCurrentRunningFetchOperations() {
        return runningFetchOpsLimit - remainingRunningFetchOpsLimit.get();
    }

    /**
     * Returns the number of samples that have been fetched (but not processed
     * yet). This is the number of samples that are currently kept in memory.
     * 
     * @return number of samples that have been fetched, but not processed yet.
     */
    public int getCurrentSamplesInMemory() {
        long currentSamplesInMemory = samplesLimit
                - remainingSamplesLimit.get();
        if (currentSamplesInMemory > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) currentSamplesInMemory;
        }
    }

    /**
     * Returns the max. number of sample fetch operations that may run
     * concurrently.
     * 
     * @return max. number of sample fetch operations that may run concurrently.
     */
    public int getMaxRunningFetchOperations() {
        return runningFetchOpsLimit;
    }

    /**
     * Returns the max. number of samples that may concurrently be kept in
     * memory.
     * 
     * @return max. number of samples that may be fetched into memory
     *         concurrently.
     */
    public int getMaxSamplesInMemory() {
        return samplesLimit;
    }

    /**
     * <p>
     * Retrieves samples for the specified channel and decimation level. The
     * result set containing the samples matching the query is returned through
     * a future. If the query fails, the future throws an exception.
     * </p>
     * 
     * <p>
     * This method basically does the same as
     * {@link ArchiveAccessService#getSamples(ChannelConfiguration, int, long, TimeStampLimitMode, long, TimeStampLimitMode, ControlSystemSupport)},
     * but it limits the number of rows that are cached in memory at any given
     * time. If the limit is exceeded, fetching more rows is postponed until
     * some of the rows have been consumed. This means that the initial query
     * (until the future completes) might take a very long time and that
     * operations fetching additional rows (when
     * <code>getAvailableWithoutFetching()</code> returns zero) might also take
     * very long to complete.
     * </p>
     * 
     * @param <SampleType>
     *            type of the sample objects from the specified
     *            <code>controlSystemSupport</code>.
     * @param channelConfiguration
     *            channel configuration for the channel for which samples shall
     *            be retrieved.
     * @param decimationLevel
     *            decimation level (identified by its decimation period) for
     *            which samples shall be received. A decimation period of zero
     *            specifies that raw samples shall be retrieved.
     * @param lowerTimeStampLimit
     *            lower limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>lowerTimeStampLimitMode</code>. This parameter must not
     *            be negative.
     * @param lowerTimeStampLimitMode
     *            mode for the <code>lowerTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the
     *            first sample returned has a time-stamp equal to or less than
     *            the <code>lowerTimeStampLimit</code> (if such a sample exists
     *            at all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the first
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>lowerTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @param upperTimeStampLimit
     *            upper limit of the interval for which samples shall be
     *            retrieved. The exact meaning depends on the
     *            <code>upperTimeStampLimitMode</code>. This parameter must be
     *            greater than or equal to the <code>lowerTimeStampLimit</code>.
     * @param upperTimeStampLimitMode
     *            mode for the <code>upperTimeStampLimit</code>. If
     *            {@link TimeStampLimitMode#AT_OR_BEFORE AT_OR_BEFORE}, the last
     *            sample returned has a time-stamp equal to or less than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all). If {@link TimeStampLimitMode#AT_OR_AFTER}, the last
     *            sample returned has a time-stamp equal to or greater than the
     *            <code>upperTimeStampLimit</code> (if such a sample exists at
     *            all).
     * @param controlSystemSupport
     *            control-system support for the channel for which samples shall
     *            be retrieved. The control-system support's type must match the
     *            control-system type specified by the supplied
     *            <code>channelConfiguration</code>.
     * @return future returning a result set which iterates over the samples
     *         matching the specified criteria. If there is an error, the future
     *         may throw an exception or the sample set returned by the future
     *         may throw an exception.
     * @see #getSamples(String, int, long, TimeStampLimitMode, long,
     *      TimeStampLimitMode)
     */
    public <SampleType extends Sample> ListenableFuture<SkippableObjectResultSet<SampleType>> getSamples(
            final ChannelConfiguration channelConfiguration,
            final int decimationLevel, final long lowerTimeStampLimit,
            final TimeStampLimitMode lowerTimeStampLimitMode,
            final long upperTimeStampLimit,
            final TimeStampLimitMode upperTimeStampLimitMode,
            final ControlSystemSupport<SampleType> controlSystemSupport) {
        AsyncFunction<Void, ObjectResultSet<SampleType>> getSamplesFunction = new AsyncFunction<Void, ObjectResultSet<SampleType>>() {
            @Override
            public ListenableFuture<ObjectResultSet<SampleType>> apply(
                    Void input) throws Exception {
                return archiveAccessService.getSamples(channelConfiguration,
                        decimationLevel, lowerTimeStampLimit,
                        lowerTimeStampLimitMode, upperTimeStampLimit,
                        upperTimeStampLimitMode, controlSystemSupport);
            }
        };
        return FutureUtils.transform(runFetchOperation(getSamplesFunction),
                new Function<ObjectResultSet<SampleType>, SkippableObjectResultSet<SampleType>>() {
                    @Override
                    public SkippableObjectResultSet<SampleType> apply(
                            ObjectResultSet<SampleType> input) {
                        try {
                            return new InternalObjectResultSet<SampleType>(
                                    input);
                        } finally {
                            // Now that the initial fetch operation has finished
                            // and the rows that have been fetched by this
                            // operation have been counted (this happens in the
                            // constructor), we can decrement the operation
                            // counter.
                            decrementFetchOperationCounter();
                        }
                    }
                },
                new Function<Throwable, SkippableObjectResultSet<SampleType>>() {
                    @Override
                    public SkippableObjectResultSet<SampleType> apply(
                            Throwable input) {
                        // Even if the fetch operation failed, we have to
                        // decrement the fetch operation counter.
                        decrementFetchOperationCounter();
                        if (input instanceof Error) {
                            throw (Error) input;
                        } else if (input instanceof RuntimeException) {
                            throw (RuntimeException) input;
                        } else {
                            throw new RuntimeException(
                                    "Fetch operation failed.", input);
                        }
                    }
                });
    }

    private void decrementFetchOperationCounter() {
        // We actually do not keep track of the number of operations currently
        // running, but of the number of operations which are still remaining
        // below the limit. For this reason, decrementing the counter actually
        // means incrementing the remaining limit.
        int oldLimit = remainingRunningFetchOpsLimit.getAndIncrement();
        // If the old remaining limit was at zero, fetch operations might have
        // been postponed. For this reason, we have to start them because the
        // remaining limit is now greater than zero.
        if (oldLimit == 0) {
            processFetchOperationQueue();
        }
    }

    private void decrementSamplesCounter(int decrement) {
        // This method is only used internally, so we only use an assertion.
        assert (decrement >= 0L);
        // We actually do not keep track of the number of rows currently in
        // memory, but of the number of rows which are still remaining below the
        // limit. For this reason, decrementing the counter actually means
        // incrementing the remaining limit.
        long oldLimit = remainingSamplesLimit.getAndAdd(decrement);
        long newLimit = oldLimit + decrement;
        // If the old remaining limit was at or below zero, fetch operations
        // might have been postponed. For this reason, we have to start them if
        // the remaining limit is now above zero.
        if (oldLimit <= 0L && newLimit > 0L) {
            processFetchOperationQueue();
        }
    }

    private void incrementSamplesCounter(int increment) {
        // This method is only used internally, so we only use an assertion.
        assert (increment >= 0L);
        // We actually do not keep track of the number of rows currently in
        // memory, but of the number of rows which are still remaining below the
        // limit. For this reason, increment the counter actually means
        // decrementing the remaining limit.
        remainingSamplesLimit.getAndAdd(-increment);
    }

    private void processFetchOperationQueue() {
        // We loop until there are no more fetch operations or one of the limits
        // is exceeded.
        while (!waitingFetchOperations.isEmpty()) {
            if (waitingFetchOperations.isEmpty()) {
                return;
            }
            // If we have already fetched a lot of rows that have not been
            // processed yet, we do not want to fetch any more data so that we
            // do not consume too much memory. This method is going to be called
            // again when enough data has been processed.
            // If there are already many fetch operations running, we do not to
            // start another one either. The reason is that each fetch operation
            // will add more rows and with these rows added, we might actually
            // exceed the row limit even if it is not exceeded currently.
            int remainingRunningFetchOpsLimit;
            if (remainingSamplesLimit.get() <= 0L
                    || (remainingRunningFetchOpsLimit = this.remainingRunningFetchOpsLimit
                            .get()) <= 0) {
                return;
            }
            // Before continuing, we decrement the remaining fetch operation
            // limit. This way, another thread will not start a fetch operation
            // concurrently when the limit has been exceeded.
            // We try to decrement the remaining limit in a loop because it
            // might be concurrently updated by another thread.
            while (!this.remainingRunningFetchOpsLimit.compareAndSet(
                    remainingRunningFetchOpsLimit,
                    remainingRunningFetchOpsLimit - 1)) {
                remainingRunningFetchOpsLimit = this.remainingRunningFetchOpsLimit
                        .get();
                if (remainingRunningFetchOpsLimit <= 0) {
                    return;
                }
            }
            SettableFuture<Void> fetchOperationFuture = waitingFetchOperations
                    .poll();
            if (fetchOperationFuture == null) {
                // Another thread beat us and processed the last fetch operation
                // in the queue. This means that we can increment the remaining
                // limit because we will not start a fetch operation.
                decrementFetchOperationCounter();
                return;
            } else {
                // Setting a value (even a null value) to the future will
                // trigger the callback that has been registered with the future
                // and thus start the fetch operation.
                fetchOperationFuture.set(null);
            }
        }
    }

    private <V> ListenableFuture<V> runFetchOperation(
            AsyncFunction<Void, V> fetchOperation) {
        // We always add a fetch operation to the queue: If the queue is
        // non-empty, this ensures that operations that have been queued earlier
        // get a chance to be run instead of running a newer operation. If the
        // queue is empty, we could skip it in theory, but this would mean that
        // we would have to implement two code-paths which would increase the
        // chances for bugs.
        // We use an unconventional way for triggering the operation in the
        // future. Instead of using a Callable and a FutureTask, we queue a
        // SettableFuture and use a transformation on that future, thus running
        // the supplied function when the future is completed. This might look
        // strange at first, but the operation that we want to run will return
        // a ListenableFuture and we only want the returned future to complete
        // when that future (that we do not have yet) has completed.
        SettableFuture<Void> triggerFuture = SettableFuture.create();
        ListenableFuture<V> resultFuture = Futures.transform(triggerFuture,
                fetchOperation, poolExecutor);
        // Now we add the trigger future to the queue so that it can be
        // processed.
        waitingFetchOperations.add(triggerFuture);
        // We have to process the fetch operation queue so that the operation
        // that we just added is processed in case the limits have not yet been
        // exceeded. If one of the limits has been exceeded, calling this method
        // will simply do nothing.
        processFetchOperationQueue();
        return resultFuture;
    }

}
