/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.AbstractSession;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
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
 * Another limitation of this class is that will consider a statement to have
 * completed execution when the future returned by the backing session's
 * <code>executeAsync</code> method has completed. However, the
 * {@link ResultSet} returned by the future might actually trigger additional
 * database operations when the number of rows is large and paging is enabled.
 * In this case, the statement will not contribute towards the count of running
 * read statements, even though it ist still causing load on the database
 * cluster.
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
     * {@link ResultSet} future that can be set in order to complete it.
     * 
     * @author Sebastian Marsching
     */
    private static class SettableResultSetFuture extends
            AbstractFuture<ResultSet> implements ResultSetFuture {

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
        for (firstWordStart = 0; firstWordStart < queryString.length(); ++firstWordStart) {
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
    private ConcurrentLinkedQueue<Pair<Statement, SettableResultSetFuture>> pendingReadStatements = new ConcurrentLinkedQueue<Pair<Statement, SettableResultSetFuture>>();
    private AtomicInteger pendingReadStatementsCount = new AtomicInteger();
    private ConcurrentLinkedQueue<Pair<Statement, SettableResultSetFuture>> pendingWriteStatements = new ConcurrentLinkedQueue<Pair<Statement, SettableResultSetFuture>>();
    private AtomicInteger pendingWriteStatementsCount = new AtomicInteger();
    private AtomicInteger runningReadStatementsCount = new AtomicInteger();
    private AtomicInteger runningWriteStatementsCount = new AtomicInteger();
    private final Session session;

    /**
     * Creates a throttling session that is backed by the specified session. The
     * newly created session limits the number of concurrently running read and
     * write statements according to the specified limits. Please see the
     * {@linkplain} ThrottlingSession class documentation} for more information
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
        Preconditions
                .checkArgument(maxConcurrentReadStatements > 0,
                        "The maxConcurrentReadStatements parameter must not be less than one.");
        Preconditions
                .checkArgument(maxConcurrentWriteStatements > 0,
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
            Statement statement,
            final AtomicInteger counter,
            final ConcurrentLinkedQueue<Pair<Statement, SettableResultSetFuture>> queue,
            final AtomicInteger queueCounter) {
        ResultSetFuture resultFuture;
        try {
            resultFuture = session.executeAsync(statement);
        } catch (Throwable t) {
            resultFuture = new SettableResultSetFuture();
            ((SettableResultSetFuture) resultFuture).setException(t);
        }
        try {
            resultFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    // If we do not start another execution, we can decrement
                    // the counter.
                    if (!executeAsyncFromQueue(counter, queue, queueCounter)) {
                        counter.decrementAndGet();
                    }

                }
            }, MoreExecutors.sameThreadExecutor());
        } catch (Throwable t) {
            // It is extremely unlikely that the code in the try block throws an
            // exception. This can only happen if adding the listener fails or
            // the listener is executed immediately and throws an exception
            // (which it should not). In any case, we do not want this exception
            // to bubble up into the calling code, so we catch it here.
        }
        return resultFuture;
    }

    private boolean executeAsyncFromQueue(
            AtomicInteger counter,
            ConcurrentLinkedQueue<Pair<Statement, SettableResultSetFuture>> queue,
            AtomicInteger queueCounter) {
        Pair<Statement, SettableResultSetFuture> queuedStatementAndFuture = queue
                .poll();
        // Obviously, there is a race condition between removing an element from
        // the queue and decrementing the counter, but this is okay because we
        // only use that counter for diagnostics.
        // If the queue is empty, we do not have to execute a statement.
        if (queuedStatementAndFuture == null) {
            return false;
        }
        queueCounter.decrementAndGet();
        Statement queuedStatement = queuedStatementAndFuture.getLeft();
        SettableResultSetFuture queuedFuture = queuedStatementAndFuture
                .getRight();
        try {
            ResultSetFuture resultFuture = executeAsyncAndProcessQueueWhenFinished(
                    queuedStatement, counter, queue, queueCounter);
            resultFuture.addListener(
                    forwardingListener(resultFuture, queuedFuture),
                    MoreExecutors.sameThreadExecutor());
        } catch (Throwable t) {
            queuedFuture.setException(t);
        }
        return true;
    }

    private ResultSetFuture executeAsyncOrAddToQueue(
            Statement statement,
            AtomicInteger counter,
            int limit,
            ConcurrentLinkedQueue<Pair<Statement, SettableResultSetFuture>> queue,
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
                queue.add(Pair.of(statement, future));
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
                        if (!executeAsyncFromQueue(counter, queue, queueCounter)) {
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

}
