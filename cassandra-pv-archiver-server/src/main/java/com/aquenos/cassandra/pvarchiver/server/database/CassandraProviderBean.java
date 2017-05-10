/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

import com.aquenos.cassandra.pvarchiver.server.spring.CassandraProperties;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Cassandra provider that reads its configuration from
 * {@link CassandraProperties}. Initialization of the cluster provided by this
 * class is first tried in {@link #afterPropertiesSet()}. If the initialization
 * fails, a background thread is created that retries the initialization process
 * every 30 seconds. The {@link Cluster} and the {@link Session} are closed when
 * this bean is destroyed (usually when the application context is closed).
 * 
 * @author Sebastian Marsching
 */
public class CassandraProviderBean implements ApplicationEventPublisherAware,
        CassandraProvider, DisposableBean, InitializingBean,
        SmartInitializingSingleton {

    private static class State {
        public boolean destroyed;
        public Cluster cluster;
        public Session session;
        public RuntimeException initializationException;
    }

    /**
     * Logger for this class.
     */
    protected final Log log = LogFactory.getLog(getClass());

    private ApplicationEventPublisher applicationEventPublisher;
    private CassandraProperties cassandraProperties;
    private Cluster.Builder clusterBuilder;
    private SettableFuture<Cluster> clusterFuture = SettableFuture.create();
    private Timer initializationTimer;
    private SettableFuture<Session> sessionFuture = SettableFuture.create();
    private AtomicReference<State> state = new AtomicReference<State>(
            new State());

    @Override
    public Cluster getCluster() {
        State state = this.state.get();
        if (state.cluster != null) {
            return state.cluster;
        }
        if (state.initializationException != null) {
            throw state.initializationException;
        }
        if (state.destroyed) {
            throw new IllegalStateException(
                    "The cluster provider has already been destroyed.");
        } else {
            throw new IllegalStateException(
                    "The cluster provider has not been initialized yet.");
        }
    }

    @Override
    public ListenableFuture<Cluster> getClusterFuture() {
        return clusterFuture;
    }

    @Override
    public boolean isInitialized() {
        State state = this.state.get();
        // It is sufficient to check the session. If we have a session, we also
        // have a cluster.
        return state.session != null;
    }

    @Override
    public Session getSession() {
        State state = this.state.get();
        if (state.session != null) {
            return state.session;
        }
        if (state.initializationException != null) {
            throw state.initializationException;
        }
        if (state.destroyed) {
            throw new IllegalStateException(
                    "The cluster provider has already been destroyed.");
        } else {
            throw new IllegalStateException(
                    "The cluster provider has not been initialized yet.");
        }
    }

    @Override
    public ListenableFuture<Session> getSessionFuture() {
        return sessionFuture;
    }

    @Autowired
    public void setCassandraProperties(CassandraProperties cassandraProperties) {
        this.cassandraProperties = cassandraProperties;
    }

    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        clusterBuilder = Cluster.builder();
        for (String host : cassandraProperties.getHosts()) {
            clusterBuilder.addContactPoint(host);
        }
        clusterBuilder.withPort(cassandraProperties.getPort());
        String username = cassandraProperties.getUsername();
        String password = cassandraProperties.getPassword();
        if (username != null && !username.isEmpty()) {
            if (password == null) {
                password = "";
            }
            clusterBuilder.withCredentials(username, password);
        }
        // We prefer LZ4 over Snappy because it is slightly faster.
        clusterBuilder.withCompression(ProtocolOptions.Compression.LZ4);
        ConsistencyLevel quorumConsistencyLevel;
        ConsistencyLevel serialConsistencyLevel;
        if (cassandraProperties.isUseLocalConsistencyLevel()) {
            quorumConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
            serialConsistencyLevel = ConsistencyLevel.LOCAL_SERIAL;
        } else {
            quorumConsistencyLevel = ConsistencyLevel.QUORUM;
            serialConsistencyLevel = ConsistencyLevel.SERIAL;
        }
        QueryOptions queryOptions = new QueryOptions()
                .setConsistencyLevel(quorumConsistencyLevel)
                .setSerialConsistencyLevel(serialConsistencyLevel);
        int fetchSize = cassandraProperties.getFetchSize();
        if (fetchSize != 0) {
            queryOptions.setFetchSize(fetchSize);
        }
        clusterBuilder.withQueryOptions(queryOptions);
        // We use a reconnection policy that is similar to the default policy
        // but limits the maximum delay to 2 minutes instead of 10 minutes. The
        // default policy is fine when a single node goes down, but if we lose
        // the connection to the whole cluster because our network connection is
        // interrupted, 10 minutes seems like an awfully long time.
        clusterBuilder
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(
                        1000L, 16000L));
    }

    @Override
    public void afterSingletonsInstantiated() {
        // Initialize the cluster and create the session. We try this
        // synchronously first because it is best if the session is available
        // before other components are started. If the cluster initialization
        // fails (maybe there is a temporary connection problem), we will
        // automatically retry later.
        // We do this from afterSingletonsInstantiated instead of
        // afterPropertiesSet because starting asynchronous tasks from
        // afterPropertiesSet is not the best idea in general and can actually
        // cause problems if these asynchronous tasks send events. These events
        // might not be received by all beans because these beans might not have
        // been registered as listeners yet. This method, on the other hand, is
        // called in the same phase in which EventListenerMethodProcessor is
        // notified, which also is a SmartInitializingSingleton. The beans are
        // notified in the order in which they are registered, and the
        // EventListenerMethodProcessor is registered very early, so before this
        // bean is registered.
        // Implementing SmartLifecycle might be a viable alternative, but that
        // interface comes with a lot of baggage (six methods that must be
        // implemented, one of which is not even used at all).
        tryInitialize();
    }

    private void tryInitialize() {
        State oldState = this.state.get();
        Cluster cluster = oldState.cluster;
        Session session = oldState.session;
        // If the provider has been destroyed, we do not want to continue and
        // simply cancel the timer.
        if (oldState.destroyed) {
            cancelInitializationTimer();
            return;
        }
        try {
            if (cluster == null) {
                cluster = clusterBuilder.build();
                cluster.init();
            }
        } catch (RuntimeException e) {
            // Log exception and schedule retry.
            log.error(
                    "Could not initialize connection to the Cassandra cluster. Will try again in 30 seconds.",
                    e);
            State newState = new State();
            newState.initializationException = e;
            if (!this.state.compareAndSet(oldState, newState)) {
                // There is only one thread that tries to initialize, so the
                // only reason why the state has changed is that the provider
                // has been destroyed. In this case, we cancel the timer and
                // return.
                cancelInitializationTimer();
                return;
            }
            scheduleTryInitialize();
            return;
        } catch (Throwable e) {
            // Log exception but do not schedule a retry. The exception that we
            // caught is most likely fatal.
            log.error(
                    "Could not initialize connection to the Cassandra cluster. Will not try again.",
                    e);
            cancelInitializationTimer();
            return;
        }
        try {
            if (session == null) {
                session = cluster.connect(cassandraProperties.getKeyspace());
            }
        } catch (RuntimeException e) {
            // Log exception and schedule retry.
            log.error("Could not initialize session for keyspace \""
                    + cassandraProperties.getKeyspace()
                    + "\". Will try again in 30 seconds.", e);
            State newState = new State();
            newState.cluster = cluster;
            newState.initializationException = e;
            if (!this.state.compareAndSet(oldState, newState)) {
                // There is only one thread that tries to initialize, so the
                // only reason why the state has changed is that the provider
                // has been destroyed. In this case, we close the cluster,
                // cancel the timer, and return.
                cluster.close();
                cancelInitializationTimer();
                return;
            }
            // If we did not have a cluster before, we want to complete the
            // future with the cluster that we have now.
            if (oldState.cluster == null) {
                clusterFuture.set(newState.cluster);
            }
            scheduleTryInitialize();
            return;
        } catch (Throwable e) {
            // Log exception but do not schedule a retry. The exception that we
            // caught is most likely fatal.
            log.error("Could not initialize session for keyspace \""
                    + cassandraProperties.getKeyspace()
                    + "\". Will not try again.", e);
            cancelInitializationTimer();
            return;
        }
        State newState = new State();
        newState.cluster = cluster;
        newState.session = session;
        if (!this.state.compareAndSet(oldState, newState)) {
            // There is only one thread that tries to initialize, so the
            // only reason why the state has changed is that the provider
            // has been destroyed. In this case, we close the cluster and
            // session, cancel the timer, and return.
            session.close();
            cluster.close();
            cancelInitializationTimer();
            return;
        }
        // If we did not have a cluster before, we want to complete the
        // future with the cluster that we have now.
        if (oldState.cluster == null) {
            clusterFuture.set(newState.cluster);
        }
        // We never had a session before (we only set a state that has a session
        // once), so we always want to complete the session future.
        sessionFuture.set(newState.session);
        // Finally, we can cancel the initialization timer because we do not
        // need it any longer.
        cancelInitializationTimer();
        log.info("Sucessfully connected to Cassandra cluster \""
                + StringEscapeUtils.escapeJava(cluster.getMetadata()
                        .getClusterName()) + "\".");
        applicationEventPublisher
                .publishEvent(new CassandraProviderInitializedEvent(this));
    }

    private void scheduleTryInitialize() {
        // We only touch the initialization timer from the initialization thread
        // and the timer thread. Therefore, we can do everything without
        // synchronizing on a mutex.
        if (initializationTimer == null) {
            initializationTimer = new Timer("cassandra-provider-connect-timer",
                    true);
        }
        initializationTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                tryInitialize();
            }
        }, 30000L);
    }

    private void cancelInitializationTimer() {
        // We only touch the initialization timer from the initialization thread
        // and the timer thread. Therefore, we can do everything without
        // synchronizing on a mutex.
        if (initializationTimer != null) {
            initializationTimer.cancel();
            initializationTimer = null;
        }
    }

    @Override
    public void destroy() throws Exception {
        // We do not touch the initialization timer here because we would need
        // a mutex to protect access to it. Changing the state to destroyed is
        // sufficient because the timer thread will eventually find that we are
        // in the destroyed state and cancel the timer. As the timer thread is
        // marked as a daemon thread, this will not prevent the JVM from
        // shutting down.
        State oldState = this.state.get();
        if (oldState.destroyed) {
            return;
        }
        if (oldState.session != null) {
            oldState.session.close();
        }
        if (oldState.cluster != null) {
            oldState.cluster.close();
        }
        State newState = new State();
        newState.destroyed = true;
        if (!this.state.compareAndSet(oldState, newState)) {
            // The state has changed because the initialization progressed. We
            // can simply call destroy again because this should deal with the
            // updated state.
            destroy();
            return;
        }
        // If we never got a cluster or session, we should complete the
        // corresponding futures with an exception so that code waiting for
        // their completion is finally executed.
        if (oldState.cluster == null) {
            clusterFuture
                    .setException(new RuntimeException(
                            "The Cassandra provider has been destroyed before a cluster instance could be created."));
        }
        if (oldState.session == null) {
            sessionFuture
                    .setException(new RuntimeException(
                            "The Cassandra provider has been destroyed before a session instace could be created."));
        }
    }

}
