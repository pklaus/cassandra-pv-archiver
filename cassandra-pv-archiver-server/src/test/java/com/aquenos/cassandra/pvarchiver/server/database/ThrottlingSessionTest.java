/*
 * Copyright 2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.server.database.ThrottlingSession.SettableResultSetFuture;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.AbstractSession;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Tests for the {@link ThrottlingSession}.
 * 
 * @author Sebastian Marsching
 */
public class ThrottlingSessionTest {

    /**
     * Stub implementation of a result set. This implementation is specifically
     * designed for the {@link ThrottlingSessionTest} and only implements the
     * methods needed for this test. This implementation is not thread-safe.
     * 
     * @author Sebastian Marsching
     */
    private static class ResultSetStub implements ResultSet {

        private SettableFuture<ResultSet> activeFetchFuture;
        private int availableWithoutFetching;
        private final int fetchSize;
        private int totalCount;

        public ResultSetStub() {
            this(0, 5000);
        }

        public ResultSetStub(int totalCount, int fetchSize) {
            this.totalCount = totalCount;
            this.fetchSize = fetchSize;
            this.availableWithoutFetching = Math.min(totalCount, fetchSize);
        }

        @Override
        public boolean isExhausted() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isFullyFetched() {
            return availableWithoutFetching == totalCount;
        }

        @Override
        public int getAvailableWithoutFetching() {
            return availableWithoutFetching;
        }

        @Override
        public ListenableFuture<ResultSet> fetchMoreResults() {
            if (isFullyFetched()) {
                return Futures.<ResultSet> immediateFuture(this);
            } else if (activeFetchFuture != null
                    && !activeFetchFuture.isDone()) {
                return activeFetchFuture;
            } else {
                activeFetchFuture = SettableFuture.create();
                return activeFetchFuture;
            }
        }

        @Override
        public List<Row> all() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<Row> iterator() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ExecutionInfo> getAllExecutionInfo() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        @Override
        public Row one() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean wasApplied() {
            // We never call this method, so there is no need to implement it.
            throw new UnsupportedOperationException();
        }

        public void completeFetchMoreResults() {
            if (activeFetchFuture == null) {
                return;
            }
            int fetchedElements = Math
                    .min(totalCount - availableWithoutFetching, fetchSize);
            availableWithoutFetching += fetchedElements;
            activeFetchFuture.set(this);
            activeFetchFuture = null;
        }

        public boolean isFetchMoreResultsWaiting() {
            return activeFetchFuture != null;
        }

    }

    /**
     * Stub implementation of a Cassandra session. This implementation is
     * specifically designed for the {@link ThrottlingSessionTest} and only
     * implements the methods needed for this test. This implementation is not
     * thread-safe.
     * 
     * @author Sebastian Marsching
     */
    private static class SessionStub extends AbstractSession {

        public LinkedList<SettableResultSetFuture> waitingResultSetFutures = new LinkedList<>();

        @Override
        public String getLoggedKeyspace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Session init() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Session> initAsync() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResultSetFuture executeAsync(Statement statement) {
            SettableResultSetFuture future = new SettableResultSetFuture();
            // At the moment we do not use multiple threads in our tests, so we
            // do not need synchronization.
            waitingResultSetFutures.add(future);
            return future;
        }

        @Override
        public CloseFuture closeAsync() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClosed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cluster getCluster() {
            throw new UnsupportedOperationException();
        }

        @Override
        public State getState() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected ListenableFuture<PreparedStatement> prepareAsync(String query,
                Map<String, ByteBuffer> customPayload) {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * Tests that limits for read and write statements are enforced correctly.
     */
    @Test
    public void testLimits() {
        SessionStub sessionStub = new SessionStub();
        @SuppressWarnings("resource")
        ThrottlingSession session = new ThrottlingSession(2, 3, sessionStub);
        // We have not made any queries, so the queues should be empty.
        assertEquals(0, sessionStub.waitingResultSetFutures.size());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(0, session.getRunningReadStatementsCount());
        assertEquals(0, session.getRunningWriteStatementsCount());
        // Now we execute a read statement. As the limit has not been reached,
        // the statement should be executed right away.
        ResultSetFuture read1 = session.executeAsync("SELECT * FROM x;");
        assertFalse(read1.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        assertEquals(1, sessionStub.waitingResultSetFutures.size());
        // Now we add another read statement. Its execution should also be
        // started immediately.
        ResultSetFuture read2 = session.executeAsync("SELECT * FROM x;");
        assertFalse(read2.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(2, session.getRunningReadStatementsCount());
        assertEquals(2, sessionStub.waitingResultSetFutures.size());
        // We add a third read statement. The execution of this statement should
        // not be started because we have reached the limit. The string that we
        // use for the statement is actually not valid, but we treat any
        // statement that cannot be certainly identified as a write statement as
        // a read statement.
        ResultSetFuture read3 = session.executeAsync("FOO;");
        assertFalse(read3.isDone());
        assertEquals(1, session.getPendingReadStatementsCount());
        assertEquals(2, session.getRunningReadStatementsCount());
        assertEquals(2, sessionStub.waitingResultSetFutures.size());
        // Now we add a write statement. The limits for write statements are
        // separate, so it should be started right away.
        ResultSetFuture write1 = session
                .executeAsync("INSERT INTO x (a, b) VALUES (1, 2);");
        assertFalse(write1.isDone());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(1, session.getRunningWriteStatementsCount());
        assertEquals(3, sessionStub.waitingResultSetFutures.size());
        // We add two more write statements. As the limit is three, they should
        // also be started.
        ResultSetFuture write2 = session
                .executeAsync("UPDATE X SET b = 3 WHERE A = 1;");
        assertFalse(write2.isDone());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(2, session.getRunningWriteStatementsCount());
        assertEquals(4, sessionStub.waitingResultSetFutures.size());
        ResultSetFuture write3 = session
                .executeAsync("UPDATE X SET b = 3 WHERE A = 1;");
        assertFalse(write3.isDone());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(3, session.getRunningWriteStatementsCount());
        assertEquals(5, sessionStub.waitingResultSetFutures.size());
        // A fourth and a fifth write statement should not be started because
        // this would exceed the limit.
        ResultSetFuture write4 = session
                .executeAsync("INSERT INTO x (a, b) VALUES (1, 2);");
        assertFalse(write4.isDone());
        assertEquals(1, session.getPendingWriteStatementsCount());
        assertEquals(3, session.getRunningWriteStatementsCount());
        assertEquals(5, sessionStub.waitingResultSetFutures.size());
        ResultSetFuture write5 = session
                .executeAsync("INSERT INTO x (a, b) VALUES (1, 2);");
        assertFalse(write5.isDone());
        assertEquals(2, session.getPendingWriteStatementsCount());
        assertEquals(3, session.getRunningWriteStatementsCount());
        assertEquals(5, sessionStub.waitingResultSetFutures.size());
        // Now, we complete the second future that we added (read2).
        sessionStub.waitingResultSetFutures.remove(1).set(new ResultSetStub());
        // The future should now be done and the read statement that we added
        // third should now have been started.
        assertTrue(read2.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(2, session.getRunningReadStatementsCount());
        assertEquals(5, sessionStub.waitingResultSetFutures.size());
        // Now we complete the first write statement. Again, the execution of
        // another statement should be started right away.
        sessionStub.waitingResultSetFutures.remove(1).set(new ResultSetStub());
        assertTrue(write1.isDone());
        assertEquals(1, session.getPendingWriteStatementsCount());
        assertEquals(3, session.getRunningWriteStatementsCount());
        assertEquals(5, sessionStub.waitingResultSetFutures.size());
        // Now, we complete the other two read statements.
        sessionStub.waitingResultSetFutures.poll().set(new ResultSetStub());
        assertTrue(read1.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        assertEquals(4, sessionStub.waitingResultSetFutures.size());
        // We complete the future with null instead of a ResultSet. This may
        // lead to an exception, but the future should still be completed and
        // the counters should be decremented.
        sessionStub.waitingResultSetFutures.remove(2).set(null);
        assertTrue(read3.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(0, session.getRunningReadStatementsCount());
        assertEquals(3, sessionStub.waitingResultSetFutures.size());
        // We complete another write future. This time, we make it fail in order
        // to test that the next statement is still executed.
        sessionStub.waitingResultSetFutures.poll()
                .setException(new Exception());
        assertTrue(write2.isDone());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(3, session.getRunningWriteStatementsCount());
        assertEquals(3, sessionStub.waitingResultSetFutures.size());
        // Finally, we complete the remaining statements.
        sessionStub.waitingResultSetFutures.poll()
                .setException(new Exception());
        assertTrue(write3.isDone());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(2, session.getRunningWriteStatementsCount());
        assertEquals(2, sessionStub.waitingResultSetFutures.size());
        sessionStub.waitingResultSetFutures.poll()
                .setException(new Exception());
        assertTrue(write4.isDone());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(1, session.getRunningWriteStatementsCount());
        assertEquals(1, sessionStub.waitingResultSetFutures.size());
        sessionStub.waitingResultSetFutures.poll()
                .setException(new Exception());
        assertTrue(write5.isDone());
        assertEquals(0, session.getPendingWriteStatementsCount());
        assertEquals(0, session.getRunningWriteStatementsCount());
        assertEquals(0, sessionStub.waitingResultSetFutures.size());
    }

    /**
     * Tests that paging of results works correctly. In particular, it is
     * expected that the same limits are applied when fetching more results as
     * when executing a read statement.
     */
    @Test
    public void testPaging() {
        SessionStub sessionStub = new SessionStub();
        @SuppressWarnings("resource")
        ThrottlingSession session = new ThrottlingSession(1, 1, sessionStub);
        // First, we need a result set that we can use for our tests. In order
        // to get a wrapped result set, we simply execute a dummy statement.
        ResultSetFuture read1 = session.executeAsync("");
        ResultSetStub resultSetStub1 = new ResultSetStub(123000, 5000);
        sessionStub.waitingResultSetFutures.poll().set(resultSetStub1);
        assertTrue(read1.isDone());
        ResultSet resultSet1 = FutureUtils.getUnchecked(read1);
        assertFalse(resultSet1.isExhausted());
        assertFalse(resultSet1.isFullyFetched());
        assertEquals(5000, resultSet1.getAvailableWithoutFetching());
        // If we request a fetch operation now, it should be started right away.
        ListenableFuture<ResultSet> fetch1 = resultSet1.fetchMoreResults();
        assertFalse(fetch1.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        assertTrue(resultSetStub1.isFetchMoreResultsWaiting());
        // After completing the fetch future, there should be no more running
        // statements.
        resultSetStub1.completeFetchMoreResults();
        assertTrue(fetch1.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(0, session.getRunningReadStatementsCount());
        assertFalse(resultSet1.isExhausted());
        assertFalse(resultSet1.isFullyFetched());
        assertEquals(10000, resultSet1.getAvailableWithoutFetching());
        // Now we start another read statement.
        ResultSetFuture read2 = session.executeAsync("");
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        // If we request a fetch now, it should not be started because the read
        // statement is still running.
        ListenableFuture<ResultSet> fetch2 = resultSet1.fetchMoreResults();
        assertFalse(fetch2.isDone());
        assertFalse(resultSetStub1.isFetchMoreResultsWaiting());
        assertEquals(1, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        // After we complete the read statement, the fetch operation should be
        // started.
        ResultSetStub resultSetStub2 = new ResultSetStub(33000, 1000);
        sessionStub.waitingResultSetFutures.poll().set(resultSetStub2);
        assertTrue(read2.isDone());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        ResultSet resultSet2 = FutureUtils.getUnchecked(read2);
        assertTrue(resultSetStub1.isFetchMoreResultsWaiting());
        // Now we start a fetch operation for the second result set. It should
        // be blocked because of the fetch operation for the first result set.
        ListenableFuture<ResultSet> fetch3 = resultSet2.fetchMoreResults();
        assertFalse(fetch3.isDone());
        assertFalse(resultSetStub2.isFetchMoreResultsWaiting());
        assertEquals(1, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        // After completing the fetch for the first result set, the fetch for
        // the second result set should be started.
        resultSetStub1.completeFetchMoreResults();
        assertTrue(fetch2.isDone());
        assertFalse(resultSet1.isExhausted());
        assertFalse(resultSet1.isFullyFetched());
        assertEquals(15000, resultSet1.getAvailableWithoutFetching());
        assertFalse(fetch3.isDone());
        assertTrue(resultSetStub2.isFetchMoreResultsWaiting());
        assertEquals(0, session.getPendingReadStatementsCount());
        assertEquals(1, session.getRunningReadStatementsCount());
        // If we call fetchMoreResults() again, the same future should be
        // returned.
        assertEquals(fetch3, resultSet2.fetchMoreResults());
        // Now we complete the fetch operation for the second result set.
        resultSetStub2.completeFetchMoreResults();
        assertTrue(fetch3.isDone());
        assertFalse(resultSet2.isExhausted());
        assertFalse(resultSet2.isFullyFetched());
        assertEquals(2000, resultSet2.getAvailableWithoutFetching());
    }

    /**
     * Tests that a an empty result set's <code>isExhausted</code> method
     * actually returns true.
     */
    @Test
    public void testEmptyResultSet() {
        SessionStub sessionStub = new SessionStub();
        @SuppressWarnings("resource")
        ThrottlingSession session = new ThrottlingSession(1, 1, sessionStub);
        // First, we need a result set that we can use for our tests. In order
        // to get a wrapped result set, we simply execute a dummy statement.
        ResultSetFuture read1 = session.executeAsync("");
        ResultSetStub resultSetStub1 = new ResultSetStub();
        sessionStub.waitingResultSetFutures.poll().set(resultSetStub1);
        assertTrue(read1.isDone());
        ResultSet resultSet1 = FutureUtils.getUnchecked(read1);
        assertTrue(resultSet1.isExhausted());
    }

}
