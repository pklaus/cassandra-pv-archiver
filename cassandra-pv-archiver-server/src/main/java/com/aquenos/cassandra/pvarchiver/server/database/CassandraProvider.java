/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Provides access to a Cassandra cluster through the {@link Cluster} and
 * {@link Session} objects.
 * </p>
 * 
 * <p>
 * Once initialized, the provider publishes a
 * {@link CassandraProviderInitializedEvent} in the application context, which
 * can be used by other beans that need access to the Cassandra cluster in order
 * to get a reference to the provider when it is ready to be used.
 * </p>
 * 
 * <p>
 * Alternatively, the futures provided by the {@link #getClusterFuture()} and
 * {@link #getSessionFuture()} methods can be used. These futures complete once
 * the respective object becomes available.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public interface CassandraProvider {

    /**
     * Returns a reference to the Cassandra cluster.
     * 
     * @return reference to the Cassandra cluster.
     * @throws AuthenticationException
     *             if the connection to the cluster could not be established
     *             (yet) because there is an authentication problem.
     * @throws IllegalStateException
     *             if the provider has not been fully initialized yet or has
     *             already been destroyed.
     * @throws NoHostAvailableException
     *             if the connection to the cluster could not be established
     *             (yet) because none of the configured hosts is available at
     *             the moment.
     * @see #getClusterFuture()
     */
    Cluster getCluster();

    /**
     * Returns a future reference to the Cassandra cluster. The returned future
     * completes when the cluster instance become available. The future might
     * not complete for a very long time if a connection to the cluster cannot
     * be established. The returned future will only throw an exception if the
     * Cassandra provider is destroyed before the cluster instance becomes
     * available. In all other cases, the Cassandra provider will wait for a
     * later attempt to create a cluster instance to succeed.
     * 
     * @return future that provides access to the Cassandra cluster instance
     *         once it becomes available. The cluster will be the same one as
     *         the one returned by {@link #getCluster()}.
     * @see #getCluster()
     */
    ListenableFuture<Cluster> getClusterFuture();

    /**
     * <p>
     * Tells whether this provider has been completely initialized. If
     * <code>true</code>, {@link #getCluster()} and {@link #getSession()} will
     * return a result and not throw an exception.
     * </p>
     * 
     * <p>
     * Please note that the provider might be destroyed after calling this
     * method. In this case, {@link #getCluster()} and {@link #getSession()}
     * might throw, even if this method returned <code>true</code> previously.
     * </p>
     * 
     * @return <code>true</code> if this provider is ready for operation,
     *         <code>false</code> otherwise.
     */
    boolean isInitialized();

    /**
     * Returns a reference to the Cassandra session. The session can be used to
     * run queries on the Cassandra cluster. The provider might bind the session
     * to the correct keyspace so that other components can automatically
     * operate on the keyspace without having to know the keyspace name.
     * 
     * @return reference to the Cassandra session.
     * @throws AuthenticationException
     *             if the connection to the cluster could not be established
     *             (yet) because there is an authentication problem.
     * @throws IllegalStateException
     *             if the provider has not been fully initialized yet or has
     *             already been destroyed.
     * @throws InvalidQueryException
     *             if the provider tried to bind the session to a keyspace, but
     *             the keyspace did not exist (yet).
     * @throws NoHostAvailableException
     *             if the connection to the cluster could not be established
     *             (yet) because none of the configured hosts is available at
     *             the moment.
     * @see #getSessionFuture()
     */
    Session getSession();

    /**
     * Returns a future reference to the Cassandra session. The returned future
     * completes when the session instance become available. The future might
     * not complete for a very long time if a connection to the cluster cannot
     * be established or the keyspace cannot be accessed. The returned future
     * will only throw an exception if the Cassandra provider is destroyed
     * before the session instance becomes available. In all other cases, the
     * Cassandra provider will wait for a later attempt to create a session
     * instance to succeed.
     * 
     * @return future that provides access to the Cassandra session instance
     *         once it becomes available. The session will be the same one as
     *         the one returned by {@link #getSession()}.
     * @see #getSession()
     */
    ListenableFuture<Session> getSessionFuture();

}
