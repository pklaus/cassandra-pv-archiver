/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.event.EventListener;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;

import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>
 * Cassandra provider that wraps another Cassandra Provider and supplies a
 * {@link ThrottlingSession}.
 * </p>
 * 
 * <p>
 * This implementation of the {@link CassandraProvider} interface takes another
 * Cassandra provider (that is set through the
 * {@link #setCassandraProvider(CassandraProvider)}) method and wraps the
 * {@link Session} provided by this Cassandra provider in a
 * {@link ThrottlingSession}. This implementation is useful when a component
 * expects a Cassandra provider and not a session to be injected.
 * </p>
 * 
 * <p>
 * For convenience, this class is annotated with the {@link ManagedResource}
 * annotation and thus exposes information about its configuration and the
 * current status of the {@link ThrottlingSession} through JMX.
 * </p>
 * 
 * <p>
 * Please refer to the documentation of {@link ThrottlingSession} for
 * limitations of the throttling logic.
 * </p>
 * 
 * @author Sebastian Marsching
 */
@ManagedResource(description = "provides a throttling Cassandra session")
public class ThrottlingCassandraProvider
        implements ApplicationEventPublisherAware, BeanNameAware,
        CassandraProvider, InitializingBean, SelfNaming {

    private ApplicationEventPublisher applicationEventPublisher;
    private String beanName;
    private CassandraProvider cassandraProvider;
    private int maxConcurrentReadStatements = Integer.MAX_VALUE;
    private int maxConcurrentWriteStatements = Integer.MAX_VALUE;
    private ListenableFuture<Session> sessionFuture;

    @Override
    public void afterPropertiesSet() throws Exception {
        Preconditions
                .checkState(cassandraProvider != null,
                        "The original Cassandra provider has to be set before calling this method.");
        // We transform the session future in order to create the throttling
        // session. This is better than using a settable future and initializing
        // it when we receive an event because it will always work, even if no
        // event is sent (e.g. in a test environment or when this object is
        // created after the original provider has already been initialized).
        this.sessionFuture = Futures.transform(
                cassandraProvider.getSessionFuture(),
                new Function<Session, Session>() {
                    @Override
                    public Session apply(Session input) {
                        return new ThrottlingSession(
                                maxConcurrentReadStatements,
                                maxConcurrentWriteStatements, input);
                    }
                });
    }

    @Override
    public Cluster getCluster() {
        return cassandraProvider.getCluster();
    }

    @Override
    public ListenableFuture<Cluster> getClusterFuture() {
        return cassandraProvider.getClusterFuture();
    }

    /**
     * Returns the maximum number of read statements that are executed
     * concurrently. This is the value of the configuration parameter that is
     * used when creating the {@link ThrottlingSession}. The default value is
     * {@link Integer#MAX_VALUE}, effectively not limiting the number of
     * concurrent statements. This property is exposed through JMX (read only).
     * 
     * @return maximum number of read statements run concurrently.
     * @see #setMaxConcurrentReadStatements(int)
     */
    @ManagedAttribute(description = "maximum number of read statements that are executed concurrently")
    public int getMaxConcurrentReadStatements() {
        return maxConcurrentReadStatements;
    }

    /**
     * Returns the maximum number of write statements that are executed
     * concurrently. This is the value of the configuration parameter that is
     * used when creating the {@link ThrottlingSession}. The default value is
     * {@link Integer#MAX_VALUE}, effectively not limiting the number of
     * concurrent statements. This property is exposed through JMX (read only).
     * 
     * @return maximum number of write statements run concurrently.
     * @see #setMaxConcurrentWriteStatements(int)
     */
    @ManagedAttribute(description = "maximum number of write statements that are executed concurrently")
    public int getMaxConcurrentWriteStatements() {
        return maxConcurrentWriteStatements;
    }

    @Override
    public ObjectName getObjectName() throws MalformedObjectNameException {
        String instanceName = beanName.endsWith("CassandraProvider")
                ? beanName.substring(0,
                        beanName.length() - "CassandraProvider".length())
                : beanName;
        instanceName.toUpperCase();
        if (!Character.isUpperCase(instanceName.codePointAt(0))) {
            int upperCaseCodePoint = Character
                    .toUpperCase(instanceName.codePointAt(0));
            instanceName = new StringBuilder()
                    .appendCodePoint(upperCaseCodePoint)
                    .append(instanceName.subSequence(
                            Character.charCount(upperCaseCodePoint),
                            instanceName.length()))
                    .toString();
        }
        return ObjectName.getInstance(
                "com.aquenos.cassandra.pvarchiver.server:type=ThrottlingCassandraProvider,name="
                        + instanceName);
    }

    /**
     * Returns the current number of read statements that are waiting for
     * execution. This method delegates to
     * {@link ThrottlingSession#getPendingReadStatementsCount()}. If the session
     * has not been initialized yet, this method returns zero. This property is
     * exposed through JMX (read only).
     * 
     * @return number of read statements that are currently waiting for
     *         execution.
     */
    @ManagedAttribute(description = "number of read statements that are currently waiting for execution")
    public int getPendingReadStatements() {
        if (sessionFuture == null || !sessionFuture.isDone()) {
            return 0;
        }
        try {
            return ((ThrottlingSession) FutureUtils.getUnchecked(sessionFuture))
                    .getPendingReadStatementsCount();
        } catch (Throwable t) {
            return 0;
        }
    }

    /**
     * Returns the current number of write statements that are waiting for
     * execution. This method delegates to
     * {@link ThrottlingSession#getPendingWriteStatementsCount()}. If the
     * session has not been initialized yet, this method returns zero. This
     * property is exposed through JMX (read only).
     * 
     * @return number of write statements that are currently waiting for
     *         execution.
     */
    @ManagedAttribute(description = "number of write statements that are currently waiting for execution")
    public int getPendingWriteStatements() {
        if (sessionFuture == null || !sessionFuture.isDone()) {
            return 0;
        }
        try {
            return ((ThrottlingSession) FutureUtils.getUnchecked(sessionFuture))
                    .getPendingWriteStatementsCount();
        } catch (Throwable t) {
            return 0;
        }
    }

    /**
     * Returns the current number of read statements that are running. A
     * statement is running when the backing session's <code>executeAsync</code>
     * method has been called but the future returned by that method has not
     * completed yet. This method delegates to
     * {@link ThrottlingSession#getRunningReadStatementsCount()}. If the session
     * has not been initialized yet, this method returns zero. This property is
     * exposed through JMX (read only).
     * 
     * @return number of read statements that are currently being executed.
     */
    @ManagedAttribute(description = "number of read statements that are currently being executed")
    public int getRunningReadStatements() {
        if (sessionFuture == null || !sessionFuture.isDone()) {
            return 0;
        }
        try {
            return ((ThrottlingSession) FutureUtils.getUnchecked(sessionFuture))
                    .getRunningReadStatementsCount();
        } catch (Throwable t) {
            return 0;
        }
    }

    /**
     * Returns the current number of write statements that are running. A
     * statement is running when the backing session's <code>executeAsync</code>
     * method has been called but the future returned by that method has not
     * completed yet. This method delegates to
     * {@link ThrottlingSession#getRunningReadStatementsCount()}. If the session
     * has not been initialized yet, this method returns zero. This property is
     * exposed through JMX (read only).
     * 
     * @return number of write statements that are currently being executed.
     */
    @ManagedAttribute(description = "number of write statements that are currently being executed")
    public int getRunningWriteStatements() {
        if (sessionFuture == null || !sessionFuture.isDone()) {
            return 0;
        }
        try {
            return ((ThrottlingSession) FutureUtils.getUnchecked(sessionFuture))
                    .getRunningWriteStatementsCount();
        } catch (Throwable t) {
            return 0;
        }
    }

    @Override
    public Session getSession() {
        // Calling the backing Cassandra provider's getSession method ensures
        // that exceptions are propagated correctly.
        cassandraProvider.getSession();
        // When we can get the session from the backing Cassandra provider, our
        // future should also have completed.
        return FutureUtils.getUnchecked(sessionFuture);
    }

    @Override
    public ListenableFuture<Session> getSessionFuture() {
        return sessionFuture;
    }

    @Override
    public boolean isInitialized() {
        return cassandraProvider.isInitialized();
    }

    /**
     * Handles {@link CassandraProviderInitializedEvent}s. If such an event is
     * sent by the backing Cassandra provider (that has been set through
     * {@link #setCassandraProvider(CassandraProvider)}), it is resent,
     * replacing the source with this provider. This ensures that components
     * that use this Cassandra provider instead of the original one are notified
     * correctly.
     * 
     * @param event
     *            initialization event sent by the original
     *            {@link CassandraProvider}.
     */
    @EventListener
    public void onCassandraProviderInitializedEvent(
            CassandraProviderInitializedEvent event) {
        if (event.getSource() != this.cassandraProvider) {
            return;
        }
        if (applicationEventPublisher != null) {
            applicationEventPublisher
                    .publishEvent(new CassandraProviderInitializedEvent(this));
        }
    }

    @Override
    public void setApplicationEventPublisher(
            ApplicationEventPublisher applicationEventPublisher) {
        Preconditions.checkState(sessionFuture == null,
                "This property must not be changed after calling afterPropertiesSet().");
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    /**
     * Sets the backing Cassandra provider that provides access to the Apache
     * Cassandra database. That Cassandra provider is used to back this
     * Cassandra provider. Most methods of the {@link CassandraProvider}
     * interface are simply forwarded to the original Cassandra provider. The
     * {@link #getSession()} and {@link #getSessionFuture()} methods however do
     * not forward directly, but return a {@link ThrottlingSession} instead.
     * 
     * @param cassandraProvider
     *            original provider that provides a connection to the Apache
     *            Cassandra database.
     * @throws IllegalStateException
     *             if this object has already been initialized (
     *             {@link #afterPropertiesSet()} has been called).
     */
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        Preconditions.checkState(sessionFuture == null,
                "This property must not be changed after calling afterPropertiesSet().");
        this.cassandraProvider = cassandraProvider;
    }

    /**
     * Set the maximum number of read statements that are executed concurrently.
     * When more statements are submitted to the session before the previously
     * submitted statements have been executed, those statements are queued for
     * later execution. This is the value of the configuration parameter that is
     * used when creating the {@link ThrottlingSession}. The default value is
     * {@link Integer#MAX_VALUE}, effectively not limiting the number of
     * concurrent statements.
     * 
     * @param maxConcurrentReadStatements
     *            maximum number of read statements that can run concurrently
     *            (must be greater than zero).
     * @throws IllegalArgumentException
     *             if the specified value is less than one.
     * @throws IllegalStateException
     *             if this object has already been initialized (
     *             {@link #afterPropertiesSet()} has been called).
     */
    public void setMaxConcurrentReadStatements(
            int maxConcurrentReadStatements) {
        Preconditions.checkState(sessionFuture == null,
                "This property must not be changed after calling afterPropertiesSet().");
        this.maxConcurrentReadStatements = maxConcurrentReadStatements;
    }

    /**
     * Set the maximum number of write statements that are executed
     * concurrently. When more statements are submitted to the session before
     * the previously submitted statements have been executed, those statements
     * are queued for later execution. This is the value of the configuration
     * parameter that is used when creating the {@link ThrottlingSession}. The
     * default value is {@link Integer#MAX_VALUE}, effectively not limiting the
     * number of concurrent statements.
     * 
     * @param maxConcurrentWriteStatements
     *            maximum number of write statements that can run concurrently.
     * @throws IllegalArgumentException
     *             if the specified value is less than one.
     * @throws IllegalStateException
     *             if this object has already been initialized (
     *             {@link #afterPropertiesSet()} has been called).
     */
    public void setMaxConcurrentWriteStatements(
            int maxConcurrentWriteStatements) {
        Preconditions.checkState(sessionFuture == null,
                "This property must not be changed after calling afterPropertiesSet().");
        this.maxConcurrentWriteStatements = maxConcurrentWriteStatements;
    }

}
