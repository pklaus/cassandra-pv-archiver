/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.AbstractSession;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * <p>
 * Cassandra session that limits the number of statements that are executed
 * concurrently. This class wraps a regular {@link Session} forwarding most of
 * the methods directly to the backing session. Methods that start the execution
 * of statements, however, are wrapped with a logic that limits the number of
 * statements that are executed in parallel. When the limit is reached, the
 * execution of further statements is paused until statements started earlier
 * have completed. This happens transparently to the calling code: When using
 * one of the asynchronous methods of this class, the calling code will still
 * receive a {@link ResultSetFuture} immediately, even when the execution has
 * not started yet.
 * </p>
 * 
 * <p>
 * The limit for the number of read statements and write statements that can run
 * in parallel is configured separately. Usually, read statements put more load
 * on the database cluster than write statements because executing them
 * typically involves scanning through several SSTables while write statements
 * only require a sequential append to the commit-log. This means that usually,
 * one will set a higher limit for write than for read statements. However, when
 * using write statements that are batched statements or even conditional
 * statements (also known as light-weight transactions), these statements
 * involve a significant overhead. A logged batch requires a considerable amount
 * of coordination and a light-weight transaction (LWT) implies read operations
 * that are needed to check whether the specified condition is met and also need
 * significant coordination across the replicas. For these reasons, when using
 * such statements one will typically set a lower limit for write statements
 * than for read statements.
 * </p>
 * 
 * <p>
 * If a {@link ResultSet} provided by one of the futures returned by this
 * session later tries to fetch more results (because the number of rows is
 * large and paging is enabled), this is treated like another read request and
 * queued accordingly. A request to fetch more results causes a similar load on
 * the database cluster as a fresh read statement, so it makes sense to apply
 * the same limits.
 * </p>
 * 
 * <p>
 * <strong>Limitations</strong>
 * </p>
 * 
 * <p>
 * Unfortunately, there is no good way to easily tell whether an
 * <code>INSERT</code>, <code>UPDATE</code>, or <code>DELETE</code> is a
 * conditional statement. Finding this out would involve a significant effort
 * for parsing the statement, leading to a huge overhead for each execution. For
 * this reason, this class treats conditional and unconditional writes the same
 * way, even though LWTs cause much more load on the database cluster. For the
 * same reason, batched statements are simply treated as one simple write
 * statement, even though such a batch might actually contain many statements.
 * </p>
 * 
 * <p>
 * Finally, this session does not implement the {@link #checkNotInEventLoop()}
 * method. In order to do so, it would need to call the corresponding method of
 * the backing session, which it cannot due to access restrictions (the method
 * is <code>protected</code>).
 * </p>
 * 
 * @author Sebastian Marsching
 * @see ThrottlingCassandraProvider
 */
public class ThrottlingSession extends AbstractSession {

    /**
     * Queue item for read or write requests. Such an item can either represent
     * a request to execute a statement, or it can represent a request to fetch
     * more results for an existing result set. From the involved
     * data-structures, these are very different things, but we have to keep
     * both in the same queue because requests to fetch more results shall be
     * queued like read statements.
     * 
     * @author Sebastian Marsching
     */
    private static class QueueItem {

        private ListenableFuture<ResultSet> fetchFuture;
        private ThrottlingResultSet resultSet;
        private SettableResultSetFuture resultSetFuture;
        private Statement statement;

        public QueueItem(Statement statement,
                SettableResultSetFuture resultSetFuture) {
            assert (statement != null);
            assert (resultSetFuture != null);
            this.statement = statement;
            this.resultSetFuture = resultSetFuture;
        }

        public QueueItem(ThrottlingResultSet resultSet,
                ListenableFuture<ResultSet> fetchFuture) {
            assert (resultSet != null);
            assert (fetchFuture != null);
            this.resultSet = resultSet;
            this.fetchFuture = fetchFuture;
        }

        public ListenableFuture<ResultSet> getFetchFuture() {
            assert (statement == null);
            assert (resultSetFuture == null);
            assert (resultSet != null);
            assert (fetchFuture != null);
            return fetchFuture;
        }

        public ThrottlingResultSet getResultSet() {
            assert (statement == null);
            assert (resultSetFuture == null);
            assert (resultSet != null);
            assert (fetchFuture != null);
            return resultSet;
        }

        public SettableResultSetFuture getResultSetFuture() {
            assert (resultSet == null);
            assert (fetchFuture == null);
            assert (statement != null);
            assert (resultSetFuture != null);
            return resultSetFuture;
        }

        public Statement getStatement() {
            assert (resultSet == null);
            assert (fetchFuture == null);
            assert (statement != null);
            assert (resultSetFuture != null);
            return statement;
        }

        public boolean isStatement() {
            return statement != null;
        }

    }

    /**
     * {@link ResultSet} future that can be set in order to complete it. This
     * class is package-private instead of private so that it can be used in the
     * tests for this class.
     * 
     * @author Sebastian Marsching
     */
    static class SettableResultSetFuture extends AbstractFuture<ResultSet>
            implements ResultSetFuture {

        // This is copied from com.datastax.driver.core.DriverThrowables which
        // unfortunately is package private.
        private static RuntimeException propagateCause(ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw ((Error) cause);
            }
            // We could just rethrow e.getCause(). However, the cause of the
            // ExecutionException has likely been created on the I/O thread
            // receiving the response. Which means that the stacktrace
            // associated with said cause will make no mention of the current
            // thread. This is painful for say, finding out which execute()
            // statement actually raised the exception. So instead, we re-create
            // the exception.
            if (cause instanceof DriverException)
                throw ((DriverException) cause).copy();
            else
                throw new DriverInternalError("Unexpected exception thrown",
                        cause);
        }

        @Override
        public ResultSet getUninterruptibly() {
            try {
                return Uninterruptibles.getUninterruptibly(this);
            } catch (ExecutionException e) {
                throw propagateCause(e);
            }
        }

        @Override
        public ResultSet getUninterruptibly(long timeout, TimeUnit unit)
                throws TimeoutException {
            try {
                return Uninterruptibles.getUninterruptibly(this, timeout, unit);
            } catch (ExecutionException e) {
                throw propagateCause(e);
            }
        }

        @Override
        public boolean set(ResultSet value) {
            // We only override this method to make it public.
            return super.set(value);
        }

        @Override
        public boolean setException(Throwable throwable) {
            // We only override this method to make it public.
            return super.setException(throwable);
        }

    }

    /**
     * Result set that limits when {@link #fetchMoreResults()} can be run. This
     * result set does not execute <code>fetchMoreResults</code> directly, but
     * actually uses the same queue that is also used for new read statements.
     * Regarding the load on the database, requests to fetch more results are
     * very similar to new queries.
     * 
     * @author Sebastian Marsching
     */
    private class ThrottlingResultSet implements ResultSet {

        private SettableFuture<ResultSet> activeFetchFuture;
        private ResultSet backingResultSet;

        public ThrottlingResultSet(ResultSet backingResultSet) {
            assert (backingResultSet != null);
            this.backingResultSet = backingResultSet;
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
            return backingResultSet.isFullyFetched();
        }

        @Override
        public int getAvailableWithoutFetching() {
            return backingResultSet.getAvailableWithoutFetching();
        }

        @Override
        public ListenableFuture<ResultSet> fetchMoreResults() {
            if (isFullyFetched()) {
                return Futures.<ResultSet> immediateFuture(this);
            }
            if (activeFetchFuture == null || activeFetchFuture.isDone()) {
                activeFetchFuture = SettableFuture.create();
                executeFetchMoreResultsOrAddToQueue(this, activeFetchFuture);
            }
            return activeFetchFuture;
        }

        @Override
        public List<Row> all() {
            // There must be at least as many rows as are available without
            // fetching, so using this number as the first size of the array
            // makes sense.
            List<Row> allRows = new ArrayList<Row>(
                    getAvailableWithoutFetching());
            while (!isExhausted()) {
                allRows.add(one());
            }
            return allRows;
        }

        @Override
        public Iterator<Row> iterator() {
            return new Iterator<Row>() {
                @Override
                public boolean hasNext() {
                    return !isExhausted();
                }

                @Override
                public Row next() {
                    // In order to comply with the Iterator interface, we
                    // should throw a NoSuchElementException if there are
                    // no more rows. On the other hand, the result set
                    // implementation of the Cassandra driver directly
                    // delegates to one(), thus returning null if there are
                    // no more rows.
                    // We choose to mimic the behavior of the Cassandra
                    // driver because we want our result set to behave as
                    // much like a regular result set as reasonably
                    // possible.
                    return one();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            return backingResultSet.getExecutionInfo();
        }

        @Override
        public List<ExecutionInfo> getAllExecutionInfo() {
            return backingResultSet.getAllExecutionInfo();
        }

        @Override
        public Row one() {
            // isExhausted() automatically fetches more results if
            // getAvailableWithoutFetching() returns zero and isFullyFetched()
            // returns false. This means that if isExhausted() returns false,
            // there must be at least one row that is available without
            // fetching.
            if (isExhausted()) {
                return null;
            } else {
                return backingResultSet.one();
            }
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return backingResultSet.getColumnDefinitions();
        }

        @Override
        public boolean wasApplied() {
            return backingResultSet.wasApplied();
        }

        protected void doFetchMoreResults() {
            try {
                final ListenableFuture<ResultSet> backingFuture = backingResultSet
                        .fetchMoreResults();
                backingFuture.addListener(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            FutureUtils.getUnchecked(backingFuture);
                            activeFetchFuture.set(ThrottlingResultSet.this);
                        } catch (Throwable t) {
                            activeFetchFuture.setException(t);
                        }
                    }
                }, MoreExecutors.sameThreadExecutor());
            } catch (Throwable t) {
                activeFetchFuture.setException(t);
            }
        }

    }

    private static Runnable forwardingListener(
            final ResultSetFuture sourceFuture,
            final SettableResultSetFuture targetFuture) {
        // If the source future is cancelled, we cancel the target future.
        // Unfortunately, there is no reasonable way to do it the other way
        // round: We could register a listener with the target future to find
        // out when it has been cancelled, but we cannot know the value of the
        // mayInterruptIfRunning flag and we would need to avoid causing an
        // infinite loop.
        return new Runnable() {
            @Override
            public void run() {
                try {
                    targetFuture.set(sourceFuture.get());
                } catch (CancellationException e) {
                    targetFuture.cancel(false);
                } catch (ExecutionException e) {
                    targetFuture.setException(e.getCause());
                } catch (Throwable t) {
                    targetFuture.setException(t);
                }
            }
        };
    }

    private static boolean isInsertUpdateOrDelete(String queryString) {
        int firstWordStart, firstWordEnd;
        for (firstWordStart = 0; firstWordStart < queryString
                .length(); ++firstWordStart) {
            if (!Character.isWhitespace(queryString.charAt(firstWordStart))) {
                break;
            }
        }
        for (firstWordEnd = firstWordStart + 1; firstWordEnd < queryString
                .length(); ++firstWordEnd) {
            if (Character.isWhitespace(queryString.charAt(firstWordEnd))) {
                break;
            }
        }
        if (firstWordStart >= queryString.length()) {
            // The string only contains white-space, so there is no way we can
            // analyze it. Such a string cannot be a valid query.
            return false;
        }
        String firstWord = queryString.substring(firstWordStart, firstWordEnd);
        switch (firstWord.toUpperCase(Locale.ENGLISH)) {
        case "DELETE":
        case "INSERT":
        case "UPDATE":
            // Even if the query starts with "INSERT", "UPDATE", or "DELETE", it
            // could imply a read operation because it might be a a conditional
            // update (LWT). However, checking this would be much more
            // complicated, so we do not want to do this for each query and use
            // this weak heuristic.
            return true;
        default:
            return false;
        }
    }

    private final int maxReadStatementsCount;
    private final int maxWriteStatementsCount;
    private ConcurrentLinkedQueue<QueueItem> pendingReadStatements = new ConcurrentLinkedQueue<>();
    private AtomicInteger pendingReadStatementsCount = new AtomicInteger();
    private ConcurrentLinkedQueue<QueueItem> pendingWriteStatements = new ConcurrentLinkedQueue<>();
    private AtomicInteger pendingWriteStatementsCount = new AtomicInteger();
    private AtomicInteger runningReadStatementsCount = new AtomicInteger();
    private AtomicInteger runningWriteStatementsCount = new AtomicInteger();
    private final Session session;

    /**
     * Creates a throttling session that is backed by the specified session. The
     * newly created session limits the number of concurrently running read and
     * write statements according to the specified limits. Please see the
     * {@linkplain ThrottlingSession class documentation} for more information
     * about how throttling works.
     * 
     * @param maxConcurrentReadStatements
     *            maximum number of read statements that may run concurrently.
     *            Must be greater than zero.
     * @param maxConcurrentWriteStatements
     *            maximum number of write statements that may run concurrently.
     *            Must be greater than zero.
     * @param session
     *            session that is used for actually executing the statements.
     *            That session is also used directly when other methods, that do
     *            not involve the execution of statements, are called.
     * @throws IllegalArgumentException
     *             if <code>maxConcurrentReadStatements</code> or
     *             <code>maxConcurrentWriteStatements</code> is less than one.
     */
    public ThrottlingSession(int maxConcurrentReadStatements,
            int maxConcurrentWriteStatements, Session session) {
        Preconditions.checkNotNull(session);
        Preconditions.checkArgument(maxConcurrentReadStatements > 0,
                "The maxConcurrentReadStatements parameter must not be less than one.");
        Preconditions.checkArgument(maxConcurrentWriteStatements > 0,
                "The maxConcurrentWriteStatements parameter must not be less than one.");
        this.maxReadStatementsCount = maxConcurrentReadStatements;
        this.maxWriteStatementsCount = maxConcurrentWriteStatements;
        this.session = session;
    }

    @Override
    public CloseFuture closeAsync() {
        return session.closeAsync();
    }

    @Override
    public ResultSetFuture executeAsync(Statement statement) {
        final boolean isWriteOperation;
        if (statement instanceof BatchStatement) {
            // Using batched statements for read operations does not make sense,
            // so this must be a write operation.
            return executeAsyncWrite(statement);
        } else if (statement instanceof BoundStatement) {
            PreparedStatement preparedStatement = ((BoundStatement) statement)
                    .preparedStatement();
            String queryString = preparedStatement.getQueryString();
            isWriteOperation = isInsertUpdateOrDelete(queryString);
        } else if (statement instanceof RegularStatement) {
            String queryString = ((RegularStatement) statement)
                    .getQueryString();
            isWriteOperation = isInsertUpdateOrDelete(queryString);
        } else {
            return executeAsyncRead(statement);
        }
        if (isWriteOperation) {
            return executeAsyncWrite(statement);
        } else {
            return executeAsyncRead(statement);
        }
    }

    @Override
    public Cluster getCluster() {
        return session.getCluster();
    }

    @Override
    public String getLoggedKeyspace() {
        return session.getLoggedKeyspace();
    }

    /**
     * Returns the current number of read statements that are waiting for
     * execution. Those statements have not been executed yet because the number
     * of running statements has already reached the limit. They are going to be
     * executed when statements that are currently running have finished their
     * execution.
     * 
     * @return number of read statements that are currently waiting for
     *         execution.
     */
    public int getPendingReadStatementsCount() {
        return pendingReadStatementsCount.get();
    }

    /**
     * Returns the current number of write statements that are waiting for
     * execution. Those statements have not been executed yet because the number
     * of running statements has already reached the limit. They are going to be
     * executed when statements that are currently running have finished their
     * execution.
     * 
     * @return number of write statements that are currently waiting for
     *         execution.
     */
    public int getPendingWriteStatementsCount() {
        return pendingWriteStatementsCount.get();
    }

    /**
     * Returns the session that backs this session. Most methods of this object
     * (except the ones dealing with the execution of statements) are delegated
     * to that backing session. Typically, the backing session is an unthrottled
     * session.
     * 
     * @return session that is used for actually executing statements.
     */
    public Session getRawSession() {
        return session;
    }

    /**
     * Returns the current number of read statements that are running. A
     * statement is running when the backing session's <code>executeAsync</code>
     * method has been called but the future returned by that method has not
     * completed yet.
     * 
     * @return number of read statements that are currently being executed.
     */
    public int getRunningReadStatementsCount() {
        return runningReadStatementsCount.get();
    }

    /**
     * Returns the current number of write statements that are running. A
     * statement is running when the backing session's <code>executeAsync</code>
     * method has been called but the future returned by that method has not
     * completed yet.
     * 
     * @return number of write statements that are currently being executed.
     */
    public int getRunningWriteStatementsCount() {
        return runningWriteStatementsCount.get();
    }

    @Override
    public State getState() {
        return session.getState();
    }

    @Override
    public Session init() {
        session.init();
        return this;
    }

    @Override
    public ListenableFuture<Session> initAsync() {
        return Futures.transform(session.initAsync(),
                new Function<Session, Session>() {
                    @Override
                    public Session apply(Session input) {
                        return ThrottlingSession.this;
                    }
                });
    }

    @Override
    public boolean isClosed() {
        return session.isClosed();
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        return session.prepareAsync(query);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(
            RegularStatement statement) {
        return session.prepareAsync(statement);
    }

    @Override
    protected ListenableFuture<PreparedStatement> prepareAsync(String query,
            Map<String, ByteBuffer> customPayload) {
        // This method should never be called by the parent class because we
        // override all variants of prepareAsync. However, if at some point in
        // the future more methods that call this method are added, this
        // implementation should protect us from failing unexpectedly.
        SimpleStatement statement = new SimpleStatement(query);
        statement.setOutgoingPayload(customPayload);
        return session.prepareAsync(statement);
    }

    private ResultSetFuture executeAsyncAndProcessQueueWhenFinished(
            Statement statement, final AtomicInteger counter,
            final ConcurrentLinkedQueue<QueueItem> queue,
            final AtomicInteger queueCounter) {
        final SettableResultSetFuture throttlingResultSetFuture = new SettableResultSetFuture();
        try {
            // If the query is successful, we have to convert the returned
            // result set into a throttling result set, so we cannot use the
            // result set future directly.
            final ResultSetFuture resultSetFuture = session
                    .executeAsync(statement);
            resultSetFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        ResultSet resultSet = FutureUtils
                                .getUnchecked(resultSetFuture);
                        throttlingResultSetFuture
                                .set(new ThrottlingResultSet(resultSet));
                    } catch (Throwable t) {
                        throttlingResultSetFuture.setException(t);
                    }
                    // If we do not start another execution, we can decrement
                    // the counter.
                    if (!executeAsyncFromQueue(counter, queue, queueCounter)) {
                        counter.decrementAndGet();
                    }
                }
            }, MoreExecutors.sameThreadExecutor());
        } catch (Throwable t) {
            throttlingResultSetFuture.setException(t);
            // If we do not start another execution, we can decrement
            // the counter. Actually, if the exception happened when adding the
            // listener, the execution might already have started, but we still
            // have to decrement the counter because the listener will not do
            // this if it has not been registered.
            if (!executeAsyncFromQueue(counter, queue, queueCounter)) {
                counter.decrementAndGet();
            }
        }
        return throttlingResultSetFuture;
    }

    private boolean executeAsyncFromQueue(AtomicInteger counter,
            ConcurrentLinkedQueue<QueueItem> queue,
            AtomicInteger queueCounter) {
        QueueItem queueItem = queue.poll();
        // If the queue is empty, we do not have to execute a statement.
        if (queueItem == null) {
            return false;
        }
        // Obviously, there is a race condition between removing an element from
        // the queue and decrementing the counter, but this is okay because we
        // only use that counter for diagnostics.
        queueCounter.decrementAndGet();
        // The queued item might represent a new statement or an existing result
        // set for which more results shall be fetched.
        if (queueItem.isStatement()) {
            Statement queuedStatement = queueItem.getStatement();
            SettableResultSetFuture queuedFuture = queueItem
                    .getResultSetFuture();
            try {
                ResultSetFuture resultFuture = executeAsyncAndProcessQueueWhenFinished(
                        queuedStatement, counter, queue, queueCounter);
                resultFuture.addListener(
                        forwardingListener(resultFuture, queuedFuture),
                        MoreExecutors.sameThreadExecutor());
            } catch (Throwable t) {
                queuedFuture.setException(t);
            }
        } else {
            ThrottlingResultSet resultSet = queueItem.getResultSet();
            ListenableFuture<ResultSet> fetchFuture = queueItem
                    .getFetchFuture();
            executeFetchMoreResultsAndProcessQueueWhenFinished(resultSet,
                    fetchFuture);
        }
        return true;
    }

    private ResultSetFuture executeAsyncOrAddToQueue(Statement statement,
            AtomicInteger counter, int limit,
            ConcurrentLinkedQueue<QueueItem> queue,
            AtomicInteger queueCounter) {
        Preconditions.checkNotNull(statement);
        // The counter and queue are provided internally, so we only use
        // assertions for them. The same applies to the limit.
        assert (counter != null);
        assert (queue != null);
        assert (limit > 0);
        for (;;) {
            int currentCounterValue = counter.get();
            if (currentCounterValue >= limit) {
                SettableResultSetFuture future = new SettableResultSetFuture();
                // Obviously, there is a race condition between incrementing the
                // counter and adding an element to the queue, but this is okay
                // because we only use that counter for diagnostics.
                queueCounter.incrementAndGet();
                QueueItem queueItem = new QueueItem(statement, future);
                queue.add(queueItem);
                // We have to check whether the counter is still at or above the
                // limit. If it is not, we execute the next statement from the
                // queue. We have to do this because the statement that we just
                // added might have been added after the counter was decremented
                // and in this case the statement that finished will not trigger
                // the execution of the next statement if the queue was empty
                // when it finished.
                // In any case, we return the future that we just created
                // because the statement we are going to execute is most likely
                // a different statement.
                for (;;) {
                    int newCounterValue = counter.get();
                    if (newCounterValue < limit) {
                        if (!counter.compareAndSet(newCounterValue,
                                newCounterValue + 1)) {
                            continue;
                        }
                        // If the queue is empty, we decrement the counter
                        // because no new execution has been started.
                        if (!executeAsyncFromQueue(counter, queue,
                                queueCounter)) {
                            counter.decrementAndGet();
                        }
                    }
                    break;
                }
                return future;
            } else {
                // If the counter is incremented concurrently, we have to try
                // again.
                if (!counter.compareAndSet(currentCounterValue,
                        currentCounterValue + 1)) {
                    continue;
                }
                return executeAsyncAndProcessQueueWhenFinished(statement,
                        counter, queue, queueCounter);
            }
        }
    }

    private ResultSetFuture executeAsyncRead(Statement statement) {
        return executeAsyncOrAddToQueue(statement, runningReadStatementsCount,
                maxReadStatementsCount, pendingReadStatements,
                pendingReadStatementsCount);
    }

    private ResultSetFuture executeAsyncWrite(Statement statement) {
        return executeAsyncOrAddToQueue(statement, runningWriteStatementsCount,
                maxWriteStatementsCount, pendingWriteStatements,
                pendingWriteStatementsCount);
    }

    private void executeFetchMoreResultsAndProcessQueueWhenFinished(
            ThrottlingResultSet resultSet, ListenableFuture<ResultSet> future) {
        resultSet.doFetchMoreResults();
        try {
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    // If we do not start another execution, we can
                    // decrement the counter.
                    if (!executeAsyncFromQueue(runningReadStatementsCount,
                            pendingReadStatements,
                            pendingReadStatementsCount)) {
                        runningReadStatementsCount.decrementAndGet();
                    }

                }
            }, MoreExecutors.sameThreadExecutor());
        } catch (Throwable t) {
            // It is extremely unlikely that the code in the try block
            // throws an exception. This can only happen if adding the
            // listener fails or the listener is executed immediately
            // and throws an exception (which it should not). In any
            // case, we do not want this exception
            // to bubble up into the calling code, so we catch it here.
        }
    }

    private void executeFetchMoreResultsOrAddToQueue(
            ThrottlingResultSet resultSet, ListenableFuture<ResultSet> future) {
        assert (resultSet != null);
        for (;;) {
            int currentCounterValue = runningReadStatementsCount.get();
            if (currentCounterValue >= maxReadStatementsCount) {
                // Obviously, there is a race condition between incrementing the
                // counter and adding an element to the queue, but this is okay
                // because we only use that counter for diagnostics.
                pendingReadStatementsCount.incrementAndGet();
                QueueItem queueItem = new QueueItem(resultSet, future);
                pendingReadStatements.add(queueItem);
                // We have to check whether the counter is still at or above the
                // limit. If it is not, we execute the next statement from the
                // queue. We have to do this because the statement that we just
                // added might have been added after the counter was decremented
                // and in this case the statement that finished will not trigger
                // the execution of the next statement if the queue was empty
                // when it finished.
                for (;;) {
                    int newCounterValue = runningReadStatementsCount.get();
                    if (newCounterValue < maxReadStatementsCount) {
                        if (!runningReadStatementsCount.compareAndSet(
                                newCounterValue, newCounterValue + 1)) {
                            continue;
                        }
                        // If the queue is empty, we decrement the counter
                        // because no new execution has been started.
                        if (!executeAsyncFromQueue(runningReadStatementsCount,
                                pendingReadStatements,
                                pendingReadStatementsCount)) {
                            runningReadStatementsCount.decrementAndGet();
                        }
                    }
                    break;
                }
            } else {
                // If the counter is incremented concurrently, we have to try
                // again.
                if (!runningReadStatementsCount.compareAndSet(
                        currentCounterValue, currentCounterValue + 1)) {
                    continue;
                }
                executeFetchMoreResultsAndProcessQueueWhenFinished(resultSet,
                        future);
            }
            return;
        }
    }

}
