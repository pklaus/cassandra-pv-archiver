/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.util;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.util.StringUtils;

/**
 * <p>
 * Loader for automatically discovered factories. This loader provides a
 * pluggable mechanism for discovering factories that are in the class-path.
 * Despite its name, this mechanisms is not limited to factories, but can be
 * used with any interfaces (or parent class). This class has been inspired by
 * the <code>SpringFactoriesLoader</code> from Spring Boot.
 * </p>
 * 
 * <p>
 * This class loads the file specified by {@link #FACTORIES_RESOURCE_LOCATION}
 * from the class-path. Multiple instances of this file (originating from
 * different modules) might exist in the class-path. This is intended and the
 * entries found in all those files are merged to get a list of all available
 * implementations in the class-path. The file itself follows the Java
 * properties syntax, using interface names as keys and implementation names as
 * values.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public abstract class FactoriesLoader {

    /**
     * Location of the factories configuration file in the class-path. Each
     * module that provides one or several factories contributes an instance of
     * this file, listing the factories provided by the module.
     */
    public static final String FACTORIES_RESOURCE_LOCATION = "META-INF/cassandra-pv-archiver.factories";

    private static ClassLoader getDefaultClassLoader() {
        return FactoriesLoader.class.getClassLoader();
    }

    private static <FactoryType> FactoryType createFactory(
            String factoryClassName, Class<FactoryType> factoryType,
            ClassLoader classLoader) {
        try {
            Class<?> factoryClass = classLoader.loadClass(factoryClassName);
            if (!factoryType.isAssignableFrom(factoryClass)) {
                throw new IllegalArgumentException("Class "
                        + factoryClassName
                        + " does not "
                        + (factoryType.isInterface() ? "implement interface "
                                : "extend class ") + factoryType.getName()
                        + ".");
            }
            @SuppressWarnings("unchecked")
            FactoryType factory = (FactoryType) factoryClass.newInstance();
            return factory;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class " + factoryClassName
                    + " was not found.", e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Class " + factoryClassName
                    + " could not be instantiated.", e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Class " + factoryClassName
                    + " could not be instantiated.", e);
        }
    }

    /**
     * Loads the list of class names of implementations for a specific interface
     * or base class. This method scans all instances of the
     * {@link #FACTORIES_RESOURCE_LOCATION} file and looks up the property whose
     * key is equal to the fully-qualified class name of
     * <code>factoryType</code>. It builds a list containing the values listed
     * under those keys. A single property file might contain multiple values
     * for the same key if the values are separated by commas. The values are
     * not verified in any way (for example, they might not represent the names
     * of loadable classes) and simply returned.
     * 
     * @param factoryType
     *            type of the interface or class whose fully qualified name
     *            should be used as the lookup key.
     * @param classLoader
     *            class loader to be used when loading the
     *            {@link #FACTORIES_RESOURCE_LOCATION} files. If
     *            <code>null</code>, the class loader that loaded this class is
     *            used.
     * @return list of values (typically fully-qualified class-names) associated
     *         with the specified factory type. This list might be empty, but it
     *         is never <code>null</code>.
     * @throws RuntimeException
     *             if there is an I/O error while trying to load one of the
     *             configuration files from the class-path.
     */
    public static List<String> loadFactoryNames(Class<?> factoryType,
            ClassLoader classLoader) {
        String factoryClassName = factoryType.getName();
        try {
            Enumeration<URL> resourceUrls = (classLoader != null ? classLoader
                    .getResources(FACTORIES_RESOURCE_LOCATION)
                    : getDefaultClassLoader().getResources(
                            FACTORIES_RESOURCE_LOCATION));
            List<String> factoryClassNames = new ArrayList<String>();
            while (resourceUrls.hasMoreElements()) {
                URL url = resourceUrls.nextElement();
                Properties properties = PropertiesLoaderUtils
                        .loadProperties(new UrlResource(url));
                factoryClassNames.addAll(Arrays.asList(StringUtils
                        .commaDelimitedListToStringArray(properties
                                .getProperty(factoryClassName))));
            }
            return factoryClassNames;
        } catch (IOException ex) {
            // We do not expect an I/O exception because everything is loaded
            // from the classpath. If we still do get such an exception, we
            // throw a RuntimeException because there is no reasonable way to
            // deal with such a problem.
            throw new RuntimeException("Unable to load " + factoryClassName
                    + " factories from location " + FACTORIES_RESOURCE_LOCATION
                    + ".", ex);
        }
    }

    /**
     * Loads and instantiates all factories implementing a given base type. This
     * method uses the discovery mechanism provided by
     * {@link #loadFactoryNames(Class, ClassLoader)} to find all implementations
     * of the specified base type. It then tries to create instances using the
     * values returned by {@link #loadFactoryNames(Class, ClassLoader)} as class
     * names. This means that each implementation listed for the specified base
     * type must have a default constructor.
     * 
     * @param <FactoryType>
     *            type of the interface or base class for which factories are
     *            created. Typically, this is automatically inferred from
     *            <code>factoryType</code>.
     * @param factoryType
     *            interface or base class that has to be implemented by all
     *            discovered implementations. The fully qualified name of this
     *            type is used when discovering the implementations.
     * @param classLoader
     *            class loader used for discovering implementations and loading
     *            their classes. If <code>null</code>, the class loader that
     *            loaded this class is used.
     * @return list of instances implementing <code>factoryType</code>. This
     *         list might be empty, but it is never <code>null</code>.
     * @throws IllegalArgumentException
     *             if one of the discovered classes cannot be instantiated (e.g.
     *             because it cannot be loaded or it does not have a default
     *             constructor).
     * @throws RuntimeException
     *             if there is an I/O problem while trying to discover the
     *             implementations.
     */
    public static <FactoryType> List<FactoryType> loadFactories(
            Class<? extends FactoryType> factoryType, ClassLoader classLoader) {
        if (classLoader == null) {
            classLoader = getDefaultClassLoader();
        }
        List<String> factoryClassNames = loadFactoryNames(factoryType,
                classLoader);
        ArrayList<FactoryType> factories = new ArrayList<FactoryType>(
                factoryClassNames.size());
        for (String factoryClassName : factoryClassNames) {
            factories.add(createFactory(factoryClassName, factoryType,
                    classLoader));
        }
        return factories;
    }

}
