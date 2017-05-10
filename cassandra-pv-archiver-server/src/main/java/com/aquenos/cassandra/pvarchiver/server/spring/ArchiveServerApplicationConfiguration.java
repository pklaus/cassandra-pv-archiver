/*
 * Copyright 2015-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.spring;

import java.util.List;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.context.annotation.Primary;
import org.springframework.http.client.AsyncClientHttpRequestFactory;
import org.springframework.http.client.OkHttp3ClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.client.AsyncRestTemplate;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessServiceImpl;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchivingService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCacheImpl;
import com.aquenos.cassandra.pvarchiver.server.cluster.ClusterManagementService;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistryBean;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.database.CassandraProviderBean;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAOImpl;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAO;
import com.aquenos.cassandra.pvarchiver.server.database.ClusterServersDAOImpl;
import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAO;
import com.aquenos.cassandra.pvarchiver.server.database.GenericDataStoreDAOImpl;
import com.aquenos.cassandra.pvarchiver.server.database.ThrottlingCassandraProvider;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationAuthenticationService;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationService;
import com.aquenos.cassandra.pvarchiver.server.internode.InterNodeCommunicationServiceImpl;
import com.aquenos.cassandra.pvarchiver.server.user.ArchiveUserDetailsManager;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * Configuration for the {@link ApplicationContext} created by the
 * {@link ArchiveServerApplication}. The configuration has been separated into
 * this class in order to keep it simple and not clutter it with all the static
 * methods that the main application class needs for preparing the application
 * context.
 * 
 * @author Sebastian Marsching
 */
@Configuration
@EnableConfigurationProperties({ CassandraProperties.class,
        DevelopmentModeProperties.class, ServerProperties.class })
@EnableMBeanExport
public class ArchiveServerApplicationConfiguration {

    /**
     * Creates the archive access service. The archive access service provides
     * access to samples stored in the archive.
     * 
     * @return archive access service.
     */
    @Bean
    public ArchiveAccessService archiveAccessService() {
        return new ArchiveAccessServiceImpl();
    }

    /**
     * Creates the archive configuration service. The archive configuration
     * service takes care of processing requests to modify the archive
     * configuration.
     * 
     * @return archive configuration service.
     */
    @Bean
    public ArchiveConfigurationService archiveConfigurationService() {
        ArchiveConfigurationService archiveConfigurationService = new ArchiveConfigurationService();
        int maxConcurrentArchiveConfigurationCommands;
        if (throttlingProperties()
                .getMaxConcurrentChannelMetaDataWriteStatements() <= Integer.MAX_VALUE / 4) {
            // Experiments have show that allow about four times the number of
            // configuration commands compared to the number of allows
            // concurrent write queries is the sweet spot. When choosing a
            // smaller number, the limit of the write queries does not become
            // effective. When choosing a larger number, the backlog of commands
            // waiting to run just becomes larger and the risk of a command
            // timing out is increased.
            maxConcurrentArchiveConfigurationCommands = throttlingProperties()
                    .getMaxConcurrentChannelMetaDataWriteStatements() * 4;
        } else {
            maxConcurrentArchiveConfigurationCommands = Integer.MAX_VALUE;
        }
        archiveConfigurationService
                .setMaxConcurrentArchiveConfigurationCommands(maxConcurrentArchiveConfigurationCommands);
        return archiveConfigurationService;
    }

    /**
     * Creates the archive user-details manager. This manager takes care of
     * retrieving user details from the database for authentication and also
     * allows user information stored inside the database to be modified.
     * 
     * @return archive user-details manager.
     */
    @Bean
    public ArchiveUserDetailsManager archiveUserDetailsManager() {
        ArchiveUserDetailsManager archiveUserDetailsManager = new ArchiveUserDetailsManager();
        archiveUserDetailsManager.setPasswordEncoder(bCryptPasswordEncoder());
        return archiveUserDetailsManager;
    }

    /**
     * Creates the archiving service. The archiving service is responsible for
     * the actual archiving (creating control-system channels, processing new
     * samples, etc.).
     * 
     * @return archiving service.
     */
    @Bean
    public ArchivingService archivingService() {
        return new ArchivingService();
    }

    /**
     * Creates the async client HTTP request factory. This factory is used for
     * making asynchronous requests to HTTP servers. In particular, it is used
     * by the inter-node communication service (see
     * {@link #interNodeCommunicationService()}).
     * 
     * @return async client HTTP request factory.
     */
    @Bean
    public AsyncClientHttpRequestFactory asyncClientHttpRequestFactory() {
        OkHttp3ClientHttpRequestFactory factory = new OkHttp3ClientHttpRequestFactory();
        // We leave the connection and write timeouts at their default settings,
        // but set a long read timeout. The reason for this is that a server
        // should quickly accept a connection and read the request body, but
        // depending on the operations requested, the response might only be
        // received after quite a long amount of time (for example when changing
        // the configuration for thousands of channels).
        // It would be preferable to set this timeout on a per-request basis, so
        // that a shorter timeout is used for requests which should be answered
        // quickly and a longer one for requests that might take a lot of time.
        // However, connections might get reused for different requests and we
        // can only set the timeout on the socket / connection level.
        factory.setReadTimeout(
                serverProperties().getInterNodeCommunicationRequestTimeout());
        return factory;
    }

    /**
     * Creates the password encoder used for encoding password with the BCrypt
     * algorithm and to verify such password hashes. This is the password
     * encoder that is also used by the {@link #archiveUserDetailsManager()}.
     * 
     * @return BCrypt password encoder.
     */
    @Bean
    public BCryptPasswordEncoder bCryptPasswordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * Creates the Cassandra database provider. This provider provides the
     * <code>Cluster</code> and <code>Session</code> objects through which the
     * Cassandra database can be accessed. This bean is annotated with
     * <code>@Primary</code> so that this Cassandra provider is used for
     * autowiring.
     * 
     * @return Cassandra provider bean.
     */
    @Bean
    @Primary
    public CassandraProvider cassandraProvider() {
        return new CassandraProviderBean();
    }

    /**
     * Creates the channel information cache. The channel information cache
     * stores channel information objects for all cluster channels in memory.
     * This way, queries for these objects can be processed quickly without
     * having to query the database.
     * 
     * @return channel information cache.
     */
    @Bean
    public ChannelInformationCache channelInformationCache() {
        return new ChannelInformationCacheImpl();
    }

    /**
     * Creates the cluster management service. The cluster management service
     * takes care of managing this server's state within the cluster.
     * 
     * @return cluster manager.
     */
    @Bean
    public ClusterManagementService clusterManagementService() {
        return new ClusterManagementService();
    }

    /**
     * Creates the Cassandra database provider used for the
     * {@link #channelMetaDataDAO()}. This provider is a throttling Cassandra
     * provider that wraps the {@link #cassandraProvider()}. The throttling is
     * configured with the values specified by the
     * {@link #throttlingProperties()}.
     * 
     * @return Cassandra provider bean used by the the
     *         {@link #channelMetaDataDAO()}.
     */
    @Bean
    public ThrottlingCassandraProvider channelMetaDataDAOCassandraProvider() {
        ThrottlingCassandraProvider cassandraProvider = new ThrottlingCassandraProvider();
        cassandraProvider.setCassandraProvider(cassandraProvider());
        cassandraProvider.setMaxConcurrentReadStatements(throttlingProperties()
                .getMaxConcurrentChannelMetaDataReadStatements());
        cassandraProvider
                .setMaxConcurrentWriteStatements(throttlingProperties()
                        .getMaxConcurrentChannelMetaDataWriteStatements());
        return cassandraProvider;
    }

    /**
     * Creates the DAO for accessing channel meta-data.
     * 
     * @return channel meta-data DAO.
     */
    @Bean
    public ChannelMetaDataDAO channelMetaDataDAO() {
        ChannelMetaDataDAOImpl dao = new ChannelMetaDataDAOImpl();
        dao.setCassandraProvider(channelMetaDataDAOCassandraProvider());
        return dao;
    }

    /**
     * Creates the DAO for accessing information about the archiving servers
     * within the cluster.
     * 
     * @return cluster-servers DAO.
     */
    @Bean
    public ClusterServersDAO clusterServersDAO() {
        return new ClusterServersDAOImpl();
    }

    /**
     * Creates the Cassandra database provider used for the
     * {@link #controlSystemSupportRegistry()}. This provider is a throttling
     * Cassandra provider that wraps the {@link #cassandraProvider()}. The
     * throttling is configured with the values specified by the
     * {@link #throttlingProperties()}.
     * 
     * @return Cassandra provider bean used by the the
     *         {@link #controlSystemSupportRegistry()}.
     */
    @Bean
    public ThrottlingCassandraProvider controlSystemSupportRegistryCassandraProvider() {
        ThrottlingCassandraProvider cassandraProvider = new ThrottlingCassandraProvider();
        cassandraProvider.setCassandraProvider(cassandraProvider());
        cassandraProvider.setMaxConcurrentReadStatements(throttlingProperties()
                .getMaxConcurrentControlSystemSupportReadStatements());
        cassandraProvider
                .setMaxConcurrentWriteStatements(throttlingProperties()
                        .getMaxConcurrentControlSystemSupportWriteStatements());
        return cassandraProvider;
    }

    /**
     * Creates the control-system support registry. This registry is responsible
     * for providing access to the various control-systems for which adapters
     * are available in the class path.
     * 
     * @return control-system support registry bean.
     */
    @Bean
    public ControlSystemSupportRegistry controlSystemSupportRegistry() {
        ControlSystemSupportRegistryBean controlSystemSupportRegistry = new ControlSystemSupportRegistryBean();
        controlSystemSupportRegistry
                .setCassandraProvider(controlSystemSupportRegistryCassandraProvider());
        return controlSystemSupportRegistry;
    }

    /**
     * Creates the DAO for accessing the generic data store. The generic data
     * store can be used by other components to share arbitrary information (as
     * long as the data size is not too large) within the cluster.
     * 
     * @return generic data store DAO.
     */
    @Bean
    public GenericDataStoreDAO genericDataStoreDAO() {
        return new GenericDataStoreDAOImpl();
    }

    /**
     * Creates the inter-node-communication authentication service. The
     * inter-node-communication authentication service takes care of
     * authenticating requests toe the {@link InterNodeCommunicationService}. It
     * provides methods for generating credentials that can then be checked by
     * another instance of the service on a different host.
     * 
     * @return inter-node-communication authentication service.
     */
    @Bean
    public InterNodeCommunicationAuthenticationService interNodeCommunicationAuthenticationService() {
        return new InterNodeCommunicationAuthenticationService();
    }

    /**
     * Creates the inter-node communication service. The inter-node
     * communication service provides access to other (running) servers within
     * the cluster.
     * 
     * @return inter-node communication service.
     */
    @Bean
    public InterNodeCommunicationService interNodeCommunicationService() {
        AsyncRestTemplate asyncRestTemplate = new AsyncRestTemplate(
                asyncClientHttpRequestFactory());
        extendAsyncRestTemplateMessageConverters(asyncRestTemplate
                .getMessageConverters());
        InterNodeCommunicationServiceImpl interNodeCommunicationService = new InterNodeCommunicationServiceImpl();
        interNodeCommunicationService.setAsyncRestTemplate(asyncRestTemplate);
        return interNodeCommunicationService;
    }

    /**
     * Creates the server properties. The server properties specify the
     * configuration options used by the server components and the
     * {@link #asyncClientHttpRequestFactory()}. This method only creates the
     * object. The actual property values are injected by the Spring container.
     * 
     * @return throttling configuration properties.
     */
    @Bean
    public ServerProperties serverProperties() {
        return new ServerProperties();
    }

    /**
     * Creates the throttling properties. The throttling properties specify the
     * configuration options used by the
     * {@link #channelMetaDataDAOCassandraProvider()} and
     * {@link #controlSystemSupportRegistryCassandraProvider()}. This method
     * only creates the object. The actual property values are injected by the
     * Spring container.
     * 
     * @return throttling configuration properties.
     */
    @Bean
    public ThrottlingProperties throttlingProperties() {
        return new ThrottlingProperties();
    }

    private void extendAsyncRestTemplateMessageConverters(
            List<HttpMessageConverter<?>> converters) {
        // Spring uses some inconvenient options when configuring Jackson's
        // ObjectMapper. Therefore, we want to change a few of these options.
        Optional<HttpMessageConverter<?>> jackson2Converter = Iterables
                .tryFind(converters, Predicates
                        .instanceOf(MappingJackson2HttpMessageConverter.class));
        if (jackson2Converter.isPresent()) {
            MappingJackson2HttpMessageConverter converter = (MappingJackson2HttpMessageConverter) jackson2Converter
                    .get();
            ObjectMapper objectMapper = ((MappingJackson2HttpMessageConverter) jackson2Converter
                    .get()).getObjectMapper();
            // We want to enable FAIL_ON_NULL_FOR_PRIMITIVES so that an
            // exception is thrown if a client sends malformed input.
            objectMapper.configure(
                    DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);
            // Spring sets FAIL_ON_UNKNOWN_PROPERTIES to false, but we like
            // Jackson's default (true) better.
            converter.getObjectMapper().configure(
                    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        }
    }

}
