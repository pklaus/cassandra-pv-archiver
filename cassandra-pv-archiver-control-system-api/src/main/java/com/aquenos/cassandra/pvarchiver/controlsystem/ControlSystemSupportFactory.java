/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

import java.util.Map;

import com.datastax.driver.core.Session;

/**
 * <p>
 * Factory that can create a {@link ControlSystemSupport}. Each control-system
 * support module must provide such a factory which is responsible for creating
 * its respective control-system support when requested by the archive server.
 * </p>
 * 
 * <p>
 * In order to register the factory with the server, a properties file with the
 * name <code>META-INF/cassandra-pv-archiver.factories</code> has to be added to
 * the class-path. Within this property file, the fully qualified name of this
 * interface has to be used as the property key and the fully qualified name of
 * the class implementing the interface has to be used as the property value. If
 * there are multiple classes implementing this interface within the same
 * module, the names of those classes can be separated by commas.
 * </p>
 * 
 * <p>
 * When there are multiple control-system support modules in the class-path,
 * multiple instances of the property file might exist in the class-path. The
 * discovery mechanism is able to deal with such a situation, automatically
 * taking all these files into consideration when trying to find the factory
 * implementations.
 * </p>
 * 
 * <p>
 * Classes implementing this interface <strong>must</strong> have a default
 * constructor. Otherwise the discovery mechanism is not able to instantiate the
 * class, resulting in a fatal error.
 * </p>
 * 
 * <p>
 * Classes implementing this interface must also be thread-safe. This means that
 * they must allow for multiple threads calling its method in parallel. The
 * <code>ControlSystemSupport</code>s returned by implementations of this
 * interface must also be thread-safe.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public interface ControlSystemSupportFactory {

    /**
     * Creates an instance of the control-system support associated with this
     * factory. This method may block in order to initialize the control-system
     * support (e.g. create database structures, allocate network resources).
     * 
     * @param configuration
     *            map containing configuration options. The configuration
     *            options are properties that have been extracted from the
     *            server configuration file according to the prefix specified by
     *            {@link #getConfigurationPrefix()}. Values might be
     *            <code>null</code>, but keys are never <code>null</code>. The
     *            meaning of the configuration options is
     *            implementation-specific, but the naming scheme should use
     *            camel-case syntax, starting with a lower-case letter. The keys
     *            in the configuration map are specified with the prefix
     *            removed.
     * @param session
     *            Cassandra session used for database operations.
     * @return control-system support provided by this factory, never
     *         <code>null</code>.
     */
    ControlSystemSupport<?> createControlSystemSupport(
            Map<String, String> configuration, Session session);

    /**
     * <p>
     * Returns the prefix used for the configuration options associated with
     * this control-system support. This prefix should use camel-case syntax and
     * start with a lower-case letter. For example, a control-system support
     * using the identifier "my_control_system" should use a prefix of
     * "myControlSystem".
     * </p>
     * 
     * <p>
     * Within the server configuration file, the prefix "controlSystem" has to
     * be prepended to all properties associated with a control-system support.
     * For example, if a factory specified a configuration prefix of
     * "myControlSystem", the property "controlSystem.myControlSystem.myOption"
     * would be passed to {@link #createControlSystemSupport(Map, Session)}
     * using the key "myOption".
     * </p>
     * 
     * @return prefix used for configuration options associated with this
     *         control-system support.
     */
    String getConfigurationPrefix();

}
