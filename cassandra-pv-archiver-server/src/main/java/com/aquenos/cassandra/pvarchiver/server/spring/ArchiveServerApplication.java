/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.spring;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.config.ConfigFileApplicationListener;
import org.springframework.boot.env.PropertySourcesLoader;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.StopWatch;

import com.aquenos.cassandra.pvarchiver.server.web.admin.spring.AdminWebApplication;
import com.aquenos.cassandra.pvarchiver.server.web.archiveaccess.spring.ArchiveAccessWebApplication;
import com.aquenos.cassandra.pvarchiver.server.web.internode.spring.InterNodeCommunicationWebApplication;
import com.sun.jna.platform.win32.KnownFolders;
import com.sun.jna.platform.win32.Shell32Util;

/**
 * <p>
 * Configuration for the main {@link SpringApplication} and central point of
 * entry.
 * </p>
 * 
 * <p>
 * The {@link #main(String[])} method is called when the application is started
 * and takes care of creating a {@link SpringApplication} for the main server
 * and child application for the various web contexts.
 * </p>
 * 
 * <p>
 * In order to keep things simple, each server port is delegated to a separate
 * application that runs an embedded servlet container. This way, the standard
 * methods provided by Spring Boot can be used to initialize the embedded
 * servlet container.
 * </p>
 * 
 * <p>
 * Instances of this class are safe for concurrent read access but are not safe
 * for concurrent write access. Typically, this should not be a problem because
 * an instance of this class is initialized once at application startup and then
 * only used for read access.
 * </p>
 * 
 * <p>
 * In order to keep this class comprehensible, most of the actual configuration
 * is delegated to {@link ArchiveServerApplicationConfiguration}, which is
 * imported by this class. This class mainly takes care of preparing the
 * environment and starting the application context.
 * </p>
 * 
 * @author Sebastian Marsching
 */
@Configuration
@Import(ArchiveServerApplicationConfiguration.class)
public class ArchiveServerApplication {

    /**
     * Internal banner implementation that is used to show the application
     * banner on sFStartup.
     * 
     * @author Sebastian Marsching
     */
    private static class ArchiveServerBanner implements Banner {

        private final static String[] LOGO_LINES = new String[] {
                "   ______                                __                ",
                "  / ____/___ _______________ _____  ____/ /________ _      ",
                " / /   / __ `/ ___/ ___/ __ `/ __ \\/ __  / ___/ __ `/      ",
                "/ /___/ /_/ (__  |__  ) /_/ / / / / /_/ / /  / /_/ /       ",
                "\\____/\\__,_/____/____/\\__,_/_/ /_/\\__,_/_/   \\__,_/        ",
                "    ____ _    __   ___              __    _                 ",
                "   / __ \\ |  / /  /   |  __________/ /_  (_)   _____  _____",
                "  / /_/ / | / /  / /| | / ___/ ___/ __ \\/ / | / / _ \\/ ___/",
                " / ____/| |/ /  / ___ |/ /  / /__/ / / / /| |/ /  __/ /    ",
                "/_/     |___/  /_/  |_/_/   \\___/_/ /_/_/ |___/\\___/_/     ",
                "===========================================================", };

        @Override
        public void printBanner(Environment environment, Class<?> sourceClass,
                PrintStream out) {
            for (String line : LOGO_LINES) {
                out.println(line);
            }
            StringBuilder versionString = new StringBuilder();
            String implementationVersion = getClass().getPackage()
                    .getImplementationVersion();
            if (implementationVersion == null
                    || implementationVersion.trim().isEmpty()) {
                versionString.append("DEVELOPMENT SNAPSHOT");
            } else {
                versionString.append("Version ");
                versionString.append(implementationVersion.trim());
            }
            int lineLength = 59;
            StringBuilder versionLine = new StringBuilder(
                    "Cassandra PV Archiver   ");
            int paddingLength = lineLength - versionLine.length()
                    - versionString.length();
            for (int i = 0; i < paddingLength; ++i) {
                versionLine.append(' ');
            }
            versionLine.append(versionString);
            out.println(versionLine);
            out.println();
        }

    }

    /**
     * Internal specialization of the {@link SpringApplication} class. This
     * class add the {@link ArchiveServerApplication} class as a configuration
     * source and writes a log message before creating the application context
     * (even when <code>logStartupInfo</code> is <code>false</code>).
     * 
     * @author Sebastian Marsching
     */
    private static class ArchiveServerApplicationImpl extends SpringApplication {

        public ArchiveServerApplicationImpl() {
            super(ArchiveServerApplication.class);
        }

        @Override
        protected ConfigurableApplicationContext createApplicationContext() {
            // We log the starting message before creating the application
            // context. This way, it is displayed after the banner but before
            // any log messages that might be triggered while initializing the
            // application context.
            logStarting();
            return super.createApplicationContext();
        }

    }

    private static final String APPLICATION_NAME = "Cassandra PV Archiver";
    private static final Log LOG = LogFactory
            .getLog(ArchiveServerApplication.class);

    private static void logStarted(double applicationStartTimeSeconds) {
        StringBuilder message = new StringBuilder();
        message.append("Started ");
        message.append(APPLICATION_NAME);
        message.append(" in ");
        message.append(applicationStartTimeSeconds);
        try {
            double uptime = ManagementFactory.getRuntimeMXBean().getUptime() / 1000.0;
            message.append(" seconds (JVM running for " + uptime + " seconds)");
        } catch (Throwable ex) {
            // No JVM time available
        }
        LOG.info(message.toString());
    }

    private static void logStarting() {
        StringBuilder message = new StringBuilder();
        message.append("Starting ");
        message.append(APPLICATION_NAME);
        String version = ArchiveServerApplication.class.getPackage()
                .getImplementationVersion();
        if (version != null) {
            message.append(" v");
            message.append(version);
        }
        String pid = System.getProperty("PID");
        if (pid != null) {
            message.append(" with PID ");
            message.append(pid);
        }
        LOG.info(message.toString());
    }

    /**
     * Central point of entry. This method is called when the JVM is started and
     * takes care of initializing and starting the application.
     * 
     * @param args
     *            command-line arguments.
     */
    public static void main(String[] args) {
        // We need a stop-watch for measuring the application startup time.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // We set the en_US locale so that language related options are
        // consistent across the application. Using the system locale does not
        // make a lot of sense for a server application anyway. We explicitly
        // allow to use different locales for application parts where we want
        // localization.
        Locale.setDefault(Locale.US);
        Options commandLineOptions = new Options();
        commandLineOptions.addOption(Option.builder("h").longOpt("help")
                .desc("Displays this usage information").build());
        commandLineOptions
                .addOption(Option
                        .builder()
                        .longOpt("config-file")
                        .hasArg()
                        .valueSeparator()
                        .desc("Specifies the path to the configuration file. This option overrides the --use-config-file-default-location option.")
                        .argName("configuration file").build());
        commandLineOptions
                .addOption(Option
                        .builder()
                        .longOpt("no-banner")
                        .desc("Suppresses the output of the version banner at startup. Combined with a custom logging configuration, this can be used to suppress all output.")
                        .build());
        commandLineOptions
                .addOption(Option
                        .builder()
                        .longOpt("server-uuid")
                        .hasArg()
                        .valueSeparator()
                        .desc("Specifies the UUID of the server. Overrides any configuration file settings.")
                        .argName("UUID").build());
        CommandLine commandLine;
        try {
            commandLine = new DefaultParser().parse(commandLineOptions, args);
        } catch (ParseException e) {
            System.out.println("Invalid command line: " + e.getMessage());
            System.out.println();
            printHelp(commandLineOptions);
            System.exit(1);
            // System.exit(...) never returns, but we get a compiler error
            // because of the uninitialized commandLine if we do not return
            // explicitly.
            return;
        }
        if (commandLine.hasOption("help")) {
            printHelp(commandLineOptions);
            System.exit(0);
        }
        String explicitConfigFileLocation = commandLine
                .getOptionValue("config-file");
        if (explicitConfigFileLocation != null
                && !(new File(explicitConfigFileLocation).exists())) {
            System.out
                    .println("Error: The specified configuration file does not exist.");
            System.exit(1);
        }
        String serverUuid = commandLine.getOptionValue("server-uuid");
        if (serverUuid != null) {
            try {
                UUID.fromString(serverUuid);
            } catch (IllegalArgumentException e) {
                System.out
                        .println("Error: The specified server UUID is invalid.");
                System.exit(1);
            }
        }

        // When the path to the configuration file has been specified on the
        // command line, we always use that path.
        String configFileLocation = explicitConfigFileLocation;
        // If the path has not been specified explicitly, we use the path from
        // a system property. This allows the start script to specify a
        // configuration file that can be found relative to the script's
        // location which is useful for distributing a default configuration
        // file that is going to be used automatically.
        if (configFileLocation == null) {
            String defaultConfigFileLocation = System
                    .getProperty(ArchiveServerApplication.class.getName()
                            + ".defaultConfigurationFileLocation");
            if (defaultConfigFileLocation != null
                    && !defaultConfigFileLocation.isEmpty()) {
                configFileLocation = defaultConfigFileLocation;
            }
        }
        // When the system property is not set, we look in a platform-specific
        // default location. On UNIX-like systems, the default location for the
        // configuration file is
        // /etc/cassandra-pv-archiver/cassandra-pv-archiver.yaml. On Windows,
        // this location is in the special program data directory.
        // The default configuration file location can be overridden with a
        // system property so that the startup script can specify a
        // configuration file relative to the script location. This is useful
        // for the binary (non-packaged) distribution so that we can ship an
        // example configuration file that is going to be picked up
        // automatically.
        if (configFileLocation == null) {
            if (SystemUtils.IS_OS_UNIX) {
                configFileLocation = "/etc/cassandra-pv-archiver/cassandra-pv-archiver.yaml";
            }
            if (SystemUtils.IS_OS_WINDOWS) {
                try {
                    String programDataPath = Shell32Util
                            .getKnownFolderPath(KnownFolders.FOLDERID_ProgramData);
                    configFileLocation = programDataPath
                            + "\\aquenos\\Cassandra PV Archiver\\cassandra-pv-archiver.yaml";
                } catch (Throwable t) {
                    // We ignore any exception. Such an exception might occur if
                    // we
                    // are not really running on Windows and we misdetected the
                    // operating system or if we are running on Windows but the
                    // version of Windows does not support the API call.
                }
            }
        }
        File configFile = (configFileLocation != null) ? new File(
                configFileLocation) : null;

        SpringApplication app = new ArchiveServerApplicationImpl();
        // Creating a StandardEnvironment would create undesired output if we
        // did not call beforeInitialize().
        LoggingSystem.get(app.getClassLoader()).beforeInitialize();
        StandardEnvironment environment = new StandardEnvironment();
        // We do not want to use environment variables - this is simply too
        // error prone. We cannot remove the property source, because other code
        // relies on a property source with this special name to exist.
        // Therefore, we replace it with a source that is empty. Actually, we
        // could even white list some environment variables if we wanted to
        // allow these environment variables to have an effect on the
        // configuration.
        environment
                .getPropertySources()
                .replace(
                        StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        new SystemEnvironmentPropertySource(
                                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                                Collections.<String, Object> emptyMap()));
        // We also replace the system properties. Name clashes are much less
        // likely for those ones, but it is better to be on the safe side.
        // Changing properties through the configuration files and command-line
        // options should be sufficient.
        environment
                .getPropertySources()
                .replace(
                        StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                        new MapPropertySource(
                                StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                                Collections.<String, Object> emptyMap()));
        ResourceLoader resourceLoader = app.getResourceLoader();
        if (resourceLoader == null) {
            resourceLoader = new DefaultResourceLoader();
        }
        PropertySourcesLoader configFileSourcesLoader = new PropertySourcesLoader();
        // First, we load the default values. This allows the files loaded later
        // to override these values.
        try {
            configFileSourcesLoader
                    .load(resourceLoader
                            .getResource("classpath:/META-INF/cassandra-pv-archiver-defaults.yaml"));
        } catch (IOException e) {
            // The default configuration file in the classpath should always
            // exist. Therefore, we treat this error as fatal.
            e.printStackTrace(System.out);
            System.exit(1);
        }
        // When we have a configuration file, we try to load it.
        if (configFile != null) {
            try {
                configFileSourcesLoader.load(resourceLoader
                        .getResource(configFile.toURI().toString()));
            } catch (FileNotFoundException e) {
                // When the configuration file has been specified explicitly and
                // it cannot be found, we consider this an error. Otherwise, we
                // simply continue without a configuration file
                if (explicitConfigFileLocation != null) {
                    e.printStackTrace(System.out);
                    System.exit(1);
                }
            } catch (IOException e) {
                // We already caught FileNotFoundExceptions, so an IOException
                // indicates a more serious problem and we do not want to
                // continue.
                e.printStackTrace(System.out);
                System.exit(1);
            }
        }
        // Finally, we want to add the property sources to our environment. We
        // use the opposite order by intention, because we actually loaded the
        // property sources in ascending order.
        for (PropertySource<?> propertySource : configFileSourcesLoader
                .getPropertySources()) {
            environment.getPropertySources().addFirst(propertySource);
        }
        // We add a property source that overrides all other property sources
        // in which we set the properties that come from the command line.
        if (serverUuid != null) {
            HashMap<String, Object> map = new HashMap<String, Object>();
            map.put("server.uuid", serverUuid);
            environment.getPropertySources().addFirst(
                    new MapPropertySource("commandLineOverrides", map));
        }
        // The path to the UUID file can be specified relative to the
        // configuration file. Obviously, this can only work when we have a
        // configuration file. As the corresponding property is not set by
        // default, we should always have a configuration file when it is
        // specified
        if (environment.containsProperty("server.uuidFile")) {
            String uuidFileLocation = environment
                    .getProperty("server.uuidFile");
            if (!uuidFileLocation.isEmpty()
                    && !(new File(uuidFileLocation).isAbsolute())) {
                if (configFile != null) {
                    File configFileParent;
                    try {
                        configFileParent = configFile.getCanonicalFile()
                                .getParentFile();
                    } catch (IOException e) {
                        // Continuing without resolving the path does not make
                        // a lot of sense because this would mean that
                        // resolution behavior would depend on whether an error
                        // occurs or not.
                        e.printStackTrace(System.out);
                        System.exit(1);
                        // The compiler does not now that System.exit never
                        // returns, so we need a return statement here in order
                        // avoid an error because of configFileParent not being
                        // initialized.
                        return;
                    }
                    HashMap<String, Object> map = new HashMap<String, Object>();
                    map.put("server.uuidFile", new File(configFileParent,
                            uuidFileLocation).getAbsolutePath());
                    environment.getPropertySources().addFirst(
                            new MapPropertySource("serverUuidFile", map));
                }
            }
        }
        // If we are running in development mode, we want to set a few
        // properties if they have not been set explicitly. We add the property
        // source last, so that any other property source can override these
        // properties.
        if (environment.getProperty("developmentMode.enabled", Boolean.class,
                false)) {
            HashMap<String, Object> map = new HashMap<String, Object>();
            // This property should enable some administrative MBeans if we
            // decide to use auto configuration.
            map.put("spring.application.admin.enabled", "true");
            // Setting this property will make the list of beans available for
            // the Live Beans view in the IDE. Actually, setting this property
            // to an empty value will result in the right settings, it just
            // needs to be defined.
            map.put("spring.liveBeansView.mbeanDomain", "");
            environment.getPropertySources().addLast(
                    new MapPropertySource("developmentModeProperties", map));
        }
        if (!environment.containsProperty("server.uuid")
                && !environment.containsProperty("server.uuidFile")) {
            System.out
                    .println("At least one of the server.uuid or server.uuidFile configuration properties or the --server-uuid command line option has to be specified.");
            System.exit(1);
        }
        app.setEnvironment(environment);
        // We do not want to use all standard locations for configuration files.
        // Including the current working directory (the default) seems a bit
        // unreliable. Therefore, we only use two well defined locations and the
        // standard location within the class path (which will result in
        // application.properties to be loaded). An additional file that will
        // override these options can be specified on the command line (or
        // through a Java system property), setting the property
        // spring.config.location.
        for (ApplicationListener<?> listener : app.getListeners()) {
            if (listener instanceof ConfigFileApplicationListener) {
                // We restrict the listener to look for configuration files in
                // the default classpath locations. This allows us to use
                // profiles if we want to, but there is no risk that files in
                // the current working directory will mess up the configuration.
                ((ConfigFileApplicationListener) listener)
                        .setSearchLocations("classpath:/,classpath:/config/");
            }
        }
        app.setWebEnvironment(false);
        app.setAddCommandLineProperties(false);
        // We disable the log startup info option because this would result in
        // undesired messages about the active profiles. In addition to that, it
        // would result in an "application started" message when actually only
        // the parent context has been initialized. Our custom application class
        // takes care of logging a different message when the application is
        // starting and we log a message about the application having been
        // started later in this method.
        app.setLogStartupInfo(false);
        if (commandLine.hasOption("no-banner")) {
            app.setBannerMode(Banner.Mode.OFF);
        } else {
            app.setBanner(new ArchiveServerBanner());
        }
        ConfigurableApplicationContext parentContext = app.run(args);

        // We create a separate (child) application context for the
        // administrator's interface. This makes it much easier to handle the
        // separate server port and servlet context.
        new SpringApplicationBuilder(AdminWebApplication.class)
                .parent(parentContext).environment(environment)
                .bannerMode(Banner.Mode.OFF).logStartupInfo(false).web(true)
                .addCommandLineProperties(false).run(args);
        // We need another child application context for the archive-access
        // communication interface.
        new SpringApplicationBuilder(ArchiveAccessWebApplication.class)
                .parent(parentContext).environment(environment)
                .bannerMode(Banner.Mode.OFF).logStartupInfo(false).web(true)
                .addCommandLineProperties(false).run(args);
        // We need another child application context for the inter-node
        // communication interface.
        new SpringApplicationBuilder(InterNodeCommunicationWebApplication.class)
                .parent(parentContext).environment(environment)
                .bannerMode(Banner.Mode.OFF).logStartupInfo(false).web(true)
                .addCommandLineProperties(false).run(args);

        // Now that all application contexts have been initialized, we consider
        // the application to have been started.
        stopWatch.stop();
        logStarted(stopWatch.getTotalTimeSeconds());
    }

    private static void printHelp(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setLongOptSeparator("=");
        helpFormatter.printHelp("cassandra-pv-archiver", options, true);
    }

}
