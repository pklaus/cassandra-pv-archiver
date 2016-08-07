/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.controlsystem;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.context.config.RandomValuePropertySource;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupportFactory;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.util.FactoriesLoader;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Implementation of a {@link ControlSystemSupportRegistry} that can be used as
 * a Spring bean. This implementation automatically discovers control-system
 * supports on the class-path using the discovery mechanism provided by
 * {@link FactoriesLoader} in order to find implementations of
 * {@link ControlSystemSupportFactory}.
 * 
 * @author Sebastian Marsching
 */
public class ControlSystemSupportRegistryBean implements
        ControlSystemSupportRegistry, DisposableBean, EnvironmentAware,
        SmartInitializingSingleton {

    private static final String CONTROL_SYSTEM_PROPERTY_PREFIX = "controlSystem.";

    /**
     * Logger for this class.
     */
    protected final Log logger = LogFactory.getLog(this.getClass());

    private AtomicBoolean destroyed = new AtomicBoolean();
    private CassandraProvider cassandraProvider;
    private AtomicReference<Map<String, ControlSystemSupport<?>>> controlSystemSupports = new AtomicReference<Map<String, ControlSystemSupport<?>>>();
    private Environment environment;

    @Override
    public void afterSingletonsInstantiated() {
        // The initialization involves triggering background tasks. For this
        // reason, we run it in afterSingletonsInstantiated() instead of
        // afterPropertiesSet().
        final TreeSet<String> availablePropertyNames = new TreeSet<String>();
        if (environment instanceof ConfigurableEnvironment) {
            ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) environment;
            MutablePropertySources propertySources = configurableEnvironment
                    .getPropertySources();
            for (PropertySource<?> propertySource : propertySources) {
                if (propertySource instanceof EnumerablePropertySource<?>) {
                    EnumerablePropertySource<?> enumerablePropertySource = (EnumerablePropertySource<?>) propertySource;
                    for (String propertyName : enumerablePropertySource
                            .getPropertyNames()) {
                        if (propertyName
                                .startsWith(CONTROL_SYSTEM_PROPERTY_PREFIX)) {
                            availablePropertyNames.add(propertyName);
                        }
                    }
                } else if (propertySource instanceof RandomValuePropertySource) {
                    // The random-value property source is not enumerable, but
                    // we know that it does not contain any interesting
                    // properties.
                } else {
                    logger.warn("Environment property source \""
                            + StringEscapeUtils.escapeJava(propertySource
                                    .getName())
                            + "\" does not implement "
                            + EnumerablePropertySource.class.getSimpleName()
                            + ". Therefore, its properties cannot be passed to the control-system factories.");
                }
            }
        } else {
            logger.warn("The environment injected into "
                    + this.getClass().getName()
                    + " does not implement "
                    + ConfigurableEnvironment.class.getSimpleName()
                    + ". This means that all control-system factories will get an empty map of configuration options passed because the list of available configuration options cannot be determined.");
        }
        // We do the initialization of the control-system supports in a
        // new thread because this might take some time (control-system
        // supports might want to create database structures, etc.).
        final ListenableFuture<Session> sessionFuture = cassandraProvider
                .getSessionFuture();
        sessionFuture.addListener(new Runnable() {
            @Override
            public void run() {
                initializeControlSystemSupports(availablePropertyNames,
                        FutureUtils.getUnchecked(sessionFuture));
            }
        }, new Executor() {
            @Override
            public void execute(Runnable command) {
                Thread thread = new Thread(command);
                thread.setDaemon(true);
                thread.start();
            }
        });
    }

    @Override
    public void destroy() throws Exception {
        // If destroyed has already been called before, we do not want to want
        // to run it again.
        if (destroyed.compareAndSet(false, true)) {
            return;
        }
        Map<String, ControlSystemSupport<?>> controlSystemSupports = this.controlSystemSupports
                .getAndSet(null);
        destroyControlSystemSupports(controlSystemSupports);
    }

    /**
     * Sets the Cassandra provider that provides access to the Apache Cassandra
     * database. Typically, this should be initialized with a Cassandra provider
     * that provides a throttled session.
     * 
     * @param cassandraProvider
     *            provider that provides a connection to the Apache Cassandra
     *            database.
     */
    public void setCassandraProvider(CassandraProvider cassandraProvider) {
        this.cassandraProvider = cassandraProvider;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public boolean isAvailable() {
        return (controlSystemSupports.get() != null && !destroyed.get());
    }

    @Override
    public ControlSystemSupport<?> getControlSystemSupport(String id) {
        Map<String, ControlSystemSupport<?>> controlSystemSupports = this.controlSystemSupports
                .get();
        if (controlSystemSupports == null) {
            return null;
        } else {
            return controlSystemSupports.get(id);
        }
    }

    @Override
    public Collection<? extends ControlSystemSupport<?>> getControlSystemSupports() {
        Map<String, ControlSystemSupport<?>> controlSystemSupports = this.controlSystemSupports
                .get();
        if (controlSystemSupports == null) {
            return Collections.emptyList();
        } else {
            // The map is unmodifiable, so it is safe to return a reference to
            // its value set.
            return controlSystemSupports.values();
        }
    }

    private void destroyControlSystemSupports(
            Map<String, ControlSystemSupport<?>> controlSystemSupports) {
        if (controlSystemSupports != null) {
            for (ControlSystemSupport<?> support : controlSystemSupports
                    .values()) {
                try {
                    support.destroy();
                } catch (Exception e) {
                    logger.error(
                            "Control-system support for " + support.getName()
                                    + " threw an exception on destruction.", e);
                }
            }
        }
    }

    private void initializeControlSystemSupports(
            TreeSet<String> availablePropertyNames, Session session) {
        // If the registry has been destroyed, we do not want to initialize it.
        if (destroyed.get()) {
            return;
        }
        Map<String, ControlSystemSupport<?>> controlSystemSupports = new HashMap<String, ControlSystemSupport<?>>();
        List<ControlSystemSupportFactory> factories = FactoriesLoader
                .loadFactories(ControlSystemSupportFactory.class,
                        ControlSystemSupportRegistryBean.class.getClassLoader());
        for (ControlSystemSupportFactory factory : factories) {
            // getConfigurationPrefix() could throw an exception, but we only
            // want to check for reasonably stupid implementations, not for
            // completely stupid ones. :-)
            String configurationPrefix = factory.getConfigurationPrefix();
            Map<String, String> configuration;
            if (configurationPrefix == null || configurationPrefix.isEmpty()) {
                configuration = Collections.emptyMap();
            } else {
                configuration = new HashMap<String, String>();
                String propertyPrefix = CONTROL_SYSTEM_PROPERTY_PREFIX
                        + configurationPrefix + ".";
                for (String propertyName : availablePropertyNames.tailSet(
                        propertyPrefix, false)) {
                    if (!propertyName.startsWith(propertyPrefix)) {
                        break;
                    }
                    configuration.put(
                            propertyName.substring(propertyPrefix.length()),
                            environment.getProperty(propertyName));
                }
            }
            ControlSystemSupport<?> support = null;
            while (support == null) {
                try {
                    support = factory.createControlSystemSupport(configuration,
                            session);
                } catch (NoHostAvailableException e) {
                    // If the exception is a NoHostAvailableException, we treat
                    // this as a transient error. We wait for a moment and then
                    // try to initialize the control-system support again.
                    // We wait for five seconds. This seems like a reasonable
                    // compromise between finishing the initialization quickly
                    // once the database become available again and wasting
                    // resources trying to initialize the control-system
                    // support.
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException interruptedException) {
                        // If this thread got interrupted, we want to finish the
                        // initialization as soon as possible.
                        Thread.currentThread().interrupt();
                        logger.error(
                                "Error while creating control-system support from factory "
                                        + factory.getClass().getName() + ": "
                                        + e.getMessage(), e);
                        break;
                    }
                } catch (Throwable t) {
                    // If there is an error problem, we expect that it is
                    // permanent and continue by initializing the remaining
                    // control-system supports.
                    logger.error(
                            "Error while creating control-system support from factory "
                                    + factory.getClass().getName() + ": "
                                    + t.getMessage(), t);
                    break;
                }
            }
            if (support == null) {
                continue;
            }
            if (controlSystemSupports.containsKey(support.getId())) {
                logger.warn("Found two control-system supports identified by the identifier \""
                        + StringEscapeUtils.escapeJava(support.getId())
                        + "\". The first one of type "
                        + controlSystemSupports.get(support.getId()).getClass()
                                .getName()
                        + " is used. The second one of type "
                        + support.getClass().getName() + " is ignored.");
            } else {
                controlSystemSupports.put(support.getId(), support);
                logger.info("Found and registered control-system support for "
                        + support.getName() + ".");
            }
        }
        // Typically, there should be only one thread initializing the
        // control-system supports. However, this extra check will save us in
        // case afterPropertiesSet() is called twice.
        if (!this.controlSystemSupports.compareAndSet(null,
                Collections.unmodifiableMap(controlSystemSupports))) {
            destroyControlSystemSupports(controlSystemSupports);
        }
        // If the registry has been destroyed in the meantime, we might have to
        // destroy the control-system supports.
        if (destroyed.get()) {
            controlSystemSupports = this.controlSystemSupports.getAndSet(null);
            destroyControlSystemSupports(controlSystemSupports);
        }
    }

}
