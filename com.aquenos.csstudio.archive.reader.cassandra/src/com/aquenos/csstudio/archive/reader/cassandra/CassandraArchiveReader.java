/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.reader.cassandra;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.text.CollationKey;
import java.text.Collator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.TreeSet;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.oro.io.GlobFilenameFilter;
import org.apache.oro.text.GlobCompiler;
import org.csstudio.archive.reader.ArchiveInfo;
import org.csstudio.archive.reader.ArchiveReader;
import org.csstudio.archive.reader.UnknownChannelException;
import org.csstudio.archive.reader.ValueIterator;
import org.csstudio.data.values.ITimestamp;

import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.util.TimestampArithmetics;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.aquenos.csstudio.archive.reader.cassandra.internal.Cancelable;
import com.aquenos.csstudio.archive.reader.cassandra.internal.CancelationProvider;
import com.aquenos.csstudio.archive.reader.cassandra.internal.CassandraMultiCompressionLevelValueIterator;
import com.aquenos.csstudio.archive.reader.cassandra.internal.CassandraValueIterator;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Reader for the Cassandra archive.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveReader implements ArchiveReader {

    private ArchiveInfo[] archiveInfo = new ArchiveInfo[] { new ArchiveInfo(
            "Cassandra", "Cassandra Archive", 1) };
    private AstyanaxContext<Cluster> context;
    private Cluster cluster;
    private Keyspace keyspace;
    private ConsistencyLevel readDataConsistencyLevel;
    private ConsistencyLevel writeDataConsistencyLevel;
    private ConsistencyLevel readMetaDataConsistencyLevel;
    private ConsistencyLevel writeMetaDataConsistencyLevel;
    private String url;
    private CassandraArchiveConfig config;
    private SampleStore sampleStore;
    private CancelationProviderImpl cancelationProvider = new CancelationProviderImpl();

    private class CancelationProviderImpl implements CancelationProvider {
        private WeakHashMap<Cancelable, Object> cancelableObjects = new WeakHashMap<Cancelable, Object>();
        private final Object lock = new Object();

        @Override
        public void register(Cancelable cancelable) {
            synchronized (lock) {
                cancelableObjects.put(cancelable, null);
            }
        }

        @Override
        public void unregister(Cancelable cancelable) {
            synchronized (lock) {
                cancelableObjects.remove(cancelable);
            }
        }

        public void cancel() {
            synchronized (lock) {
                // We build a list of the objects in the map, because calling
                // cancel on an object might modify the map and thus would cause
                // a ConcurrentModificationException.
                LinkedList<Cancelable> tempCancelableObjects = new LinkedList<Cancelable>(
                        cancelableObjects.keySet());
                for (Cancelable cancelable : tempCancelableObjects) {
                    try {
                        cancelable.cancel();
                    } finally {
                        cancelableObjects.remove(cancelable);
                    }

                }
            }
        }
    }

    /**
     * Creates a Cassandra archive reader using the specified URL.
     * 
     * @param url
     *            URL of the Cassandra database (e.g.
     *            "cassandra://localhost:9160/cssArchive?username=read-user&password=changeme"
     *            )
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param retryPolicy
     *            retry policy to use for the Cassandra client.
     */
    public CassandraArchiveReader(String url,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            RetryPolicy retryPolicy) {
        this.url = url;
        String hostsList;
        int port = 9160;
        String keyspaceName;
        String username = null;
        String password = null;
        try {
            URI uri = new URI(url);
            String scheme = uri.getScheme();
            if (scheme == null || !scheme.equals("cassandra")) {
                throw new IllegalArgumentException("Invalid URL: " + url);
            }
            String authority = uri.getRawAuthority();
            if (authority == null) {
                throw new IllegalArgumentException("Invalid URL: " + url);
            }
            String[] hostAndPort = authority.split(":");
            hostsList = URLDecoder.decode(hostAndPort[0], "UTF-8");
            String portRaw = null;
            if (hostAndPort.length > 1) {
                portRaw = hostAndPort[1];
            }
            if (portRaw != null) {
                port = Integer.valueOf(portRaw);
            }
            String path = uri.getPath();
            keyspaceName = path.startsWith("/") ? path.substring(1) : path;
            if (keyspaceName.length() == 0) {
                throw new IllegalArgumentException("Invalid URL: " + url);
            }
            String query = uri.getRawQuery();
            if (query != null) {
                String[] queryFragments = query.split("&");
                for (String queryFragment : queryFragments) {
                    String[] nameAndValue = queryFragment.split("=");
                    String name = URLDecoder.decode(nameAndValue[0], "UTF-8");
                    String value = null;
                    if (nameAndValue.length > 1) {
                        value = URLDecoder.decode(nameAndValue[1], "UTF-8");
                    } else {
                        throw new IllegalArgumentException(
                                "Missing value for parameter " + name
                                        + " in URL " + url);
                    }
                    if (name.equals("username")) {
                        username = value;
                    } else if (name.equals("password")) {
                        password = value;
                    } else {
                        throw new IllegalArgumentException("Unknown parameter "
                                + name + " in URL " + url);
                    }
                }
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URL: " + url, e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        initialize(hostsList, port, keyspaceName, readDataConsistencyLevel,
                writeDataConsistencyLevel, readMetaDataConsistencyLevel,
                writeMetaDataConsistencyLevel, retryPolicy, username, password);
    }

    /**
     * Creates a Cassandra archive reader using the specified parameters.
     * 
     * @param cassandraHosts
     *            host or comma-seperated list of hosts that provide the
     *            Cassandra database.
     * @param cassandraPort
     *            TCP port to contact the Cassandra server(s) on.
     * @param keyspaceName
     *            name of the keyspace in the Cassandra database.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param retryPolicy
     *            retry policy to use for the Cassandra client.
     * @param username
     *            username to use for authentication or <code>null</code> to use
     *            no authentication.
     * @param password
     *            password to use for authentication or <code>null</code> to use
     *            no authentication.
     */
    public CassandraArchiveReader(String cassandraHosts, int cassandraPort,
            String keyspaceName, ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            RetryPolicy retryPolicy, String username, String password) {
        initialize(cassandraHosts, cassandraPort, keyspaceName,
                readDataConsistencyLevel, writeDataConsistencyLevel,
                readMetaDataConsistencyLevel, writeMetaDataConsistencyLevel,
                retryPolicy, username, password);
    }

    /**
     * Creates an archive reader backed by the specified keyspace. The
     * connection to the database is not closed when the {@link #close()} method
     * is called.
     * 
     * @param cluster
     *            Cassandra cluster that stores the database.
     * @param keyspace
     *            keyspace that stores the configuration and samples.
     * @param readDataConsistencyLevel
     *            consistency level used when reading sample data.
     * @param writeDataConsistencyLevel
     *            consistency level used when writing sample data.
     * @param readMetaDataConsistencyLevel
     *            consistency level used when reading meta-data (that means
     *            configuration data and sample bucket sizes).
     * @param writeMetaDataConsistencyLevel
     *            consistency level used when writing meta-data (that means
     *            configuration data and sample bucket sizes).
     */
    public CassandraArchiveReader(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel) {
        initialize(cluster, keyspace, readDataConsistencyLevel,
                writeDataConsistencyLevel, readMetaDataConsistencyLevel,
                writeMetaDataConsistencyLevel);
    }

    private void initialize(String cassandraHosts, int cassandraPort,
            String keyspaceName, ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel,
            RetryPolicy retryPolicy, String username, String password) {
        // We use a random identifier for the cluster, because there might be
        // several instances of the archive reader. If we used the same
        // cluster for all of them, a cluster still being used might be shutdown
        // when one of them is closed.
        String uuid = UUID.randomUUID().toString();
        ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl(
                uuid).setSeeds(cassandraHosts).setPort(cassandraPort);
        if (username != null && password != null) {
            cpConfig.setAuthenticationCredentials(new SimpleAuthenticationCredentials(
                    username, password));
        }
        AstyanaxConfiguration asConfig = new AstyanaxConfigurationImpl()
                .setDefaultReadConsistencyLevel(readDataConsistencyLevel)
                .setDefaultWriteConsistencyLevel(writeDataConsistencyLevel)
                .setRetryPolicy(retryPolicy)
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE);
        this.context = new AstyanaxContext.Builder().forCluster(uuid)
                .forKeyspace(keyspaceName)
                .withConnectionPoolConfiguration(cpConfig)
                .withAstyanaxConfiguration(asConfig)
                .buildCluster(ThriftFamilyFactory.getInstance());
        this.context.start();
        Cluster cluster = this.context.getEntity();
        initialize(cluster, cluster.getKeyspace(keyspaceName),
                readDataConsistencyLevel, writeDataConsistencyLevel,
                readMetaDataConsistencyLevel, writeMetaDataConsistencyLevel);
    }

    private void initialize(Cluster cluster, Keyspace keyspace,
            ConsistencyLevel readDataConsistencyLevel,
            ConsistencyLevel writeDataConsistencyLevel,
            ConsistencyLevel readMetaDataConsistencyLevel,
            ConsistencyLevel writeMetaDataConsistencyLevel) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.readDataConsistencyLevel = readDataConsistencyLevel;
        this.writeDataConsistencyLevel = writeDataConsistencyLevel;
        this.readMetaDataConsistencyLevel = readMetaDataConsistencyLevel;
        this.writeMetaDataConsistencyLevel = writeMetaDataConsistencyLevel;
        this.sampleStore = new SampleStore(this.cluster, this.keyspace,
                this.readDataConsistencyLevel, this.writeDataConsistencyLevel,
                this.readMetaDataConsistencyLevel,
                this.writeMetaDataConsistencyLevel, false, false);
        this.config = this.sampleStore.getArchiveConfig();
    }

    @Override
    public String getServerName() {
        return "Cassandra Archiver";
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public String getDescription() {
        return "Archive Reader for the Apache Cassandra database system.";
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public ArchiveInfo[] getArchiveInfos() {
        return archiveInfo;
    }

    @Override
    public String[] getNamesByPattern(int key, String globPattern)
            throws Exception {
        GlobFilenameFilter filter = new GlobFilenameFilter(globPattern,
                GlobCompiler.CASE_INSENSITIVE_MASK);
        Collator collator = getCollator();
        TreeSet<CollationKey> matches = new TreeSet<CollationKey>();
        String[] allChannelNames = config.getChannelNames();
        for (String channelName : allChannelNames) {
            if (filter.accept(null, channelName)) {
                matches.add(collator.getCollationKey(channelName));
            }
        }
        String[] channelNames = new String[matches.size()];
        int i = 0;
        for (CollationKey collationKey : matches) {
            channelNames[i] = collationKey.getSourceString();
            i++;
        }
        return channelNames;
    }

    @Override
    public String[] getNamesByRegExp(int key, String regularExpression)
            throws Exception {
        Pattern pattern = Pattern.compile(regularExpression);
        Matcher matcher = pattern.matcher("");
        Collator collator = getCollator();
        TreeSet<CollationKey> matches = new TreeSet<CollationKey>();
        String[] allChannelNames = config.getChannelNames();
        for (String channelName : allChannelNames) {
            matcher.reset(channelName);
            if (matcher.matches()) {
                matches.add(collator.getCollationKey(channelName));
            }
        }
        String[] channelNames = new String[matches.size()];
        int i = 0;
        for (CollationKey collationKey : matches) {
            channelNames[i] = collationKey.getSourceString();
            i++;
        }
        return channelNames;
    }

    private Collator getCollator() {
        Collator collator = Collator.getInstance(Locale.US);
        collator.setStrength(Collator.IDENTICAL);
        collator.setDecomposition(Collator.NO_DECOMPOSITION);
        return collator;
    }

    @Override
    public ValueIterator getRawValues(int key, String name, ITimestamp start,
            ITimestamp end) throws UnknownChannelException, Exception {
        return new CassandraValueIterator(sampleStore, config, name, start,
                end, true, 0L, cancelationProvider);
    }

    @Override
    public ValueIterator getOptimizedValues(int key, String name,
            ITimestamp start, ITimestamp end, int count)
            throws UnknownChannelException, Exception {
        CompressionLevelConfig[] compressionLevelConfigs = config
                .findCompressionLevelConfigs(name);
        TreeSet<Long> compressionPeriods = new TreeSet<Long>();
        for (CompressionLevelConfig compressionLevelConfig : compressionLevelConfigs) {
            compressionPeriods.add(compressionLevelConfig
                    .getCompressionPeriod());
        }
        // Ensure that "raw" compression level is always included in map
        compressionPeriods.add(0L);
        if (compressionPeriods.size() == 1) {
            // There are only raw samples
            return getRawValues(key, name, start, end);
        }
        double requestedPeriod = TimestampArithmetics.substract(end, start)
                .toDouble();
        long requestedResolution = Math.round(requestedPeriod / count);
        return new CassandraMultiCompressionLevelValueIterator(sampleStore,
                config, name, start, end, compressionPeriods,
                requestedResolution, cancelationProvider);
    }

    @Override
    public void cancel() {
        // We cancel all registered iterators. This will cause the iterators to
        // throw an exception the next time their corresponding next() method is
        // called.
        cancelationProvider.cancel();
    }

    @Override
    public void close() {
        if (context != null) {
            context.shutdown();
            context = null;
        }
    }

}
