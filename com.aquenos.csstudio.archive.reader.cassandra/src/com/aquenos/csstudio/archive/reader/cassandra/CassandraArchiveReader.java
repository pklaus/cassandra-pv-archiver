/*
 * Copyright 2012 aquenos GmbH.
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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.oro.io.GlobFilenameFilter;
import org.apache.oro.text.GlobCompiler;
import org.csstudio.archive.reader.ArchiveInfo;
import org.csstudio.archive.reader.ArchiveReader;
import org.csstudio.archive.reader.UnknownChannelException;
import org.csstudio.archive.reader.ValueIterator;
import org.csstudio.data.values.ITimestamp;

import com.aquenos.csstudio.archive.cassandra.SampleStore;
import com.aquenos.csstudio.archive.cassandra.TimestampArithmetics;
import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;
import com.aquenos.csstudio.archive.config.cassandra.CompressionLevelConfig;
import com.aquenos.csstudio.archive.reader.cassandra.internal.Cancelable;
import com.aquenos.csstudio.archive.reader.cassandra.internal.CancelationProvider;
import com.aquenos.csstudio.archive.reader.cassandra.internal.CassandraMultiCompressionLevelValueIterator;
import com.aquenos.csstudio.archive.reader.cassandra.internal.CassandraValueIterator;

/**
 * Reader for the Cassandra archive.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveReader implements ArchiveReader {

	private final static TreeMap<Long, String> DEFAULT_COMPRESSION_LEVELS;
	static {
		DEFAULT_COMPRESSION_LEVELS = new TreeMap<Long, String>();
		DEFAULT_COMPRESSION_LEVELS.put(0L,
				CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME);
	}

	private ArchiveInfo[] archiveInfo = new ArchiveInfo[] { new ArchiveInfo(
			"Cassandra", "Cassandra Archive", 1) };
	private Cluster cluster;
	private Keyspace keyspace;
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
	 * @param consistencyLevelPolicy
	 *            consistency-level policy to use for the Cassandra client.
	 * @param failoverPolicy
	 *            fail-over policy to use for the Cassandra client.
	 */
	public CassandraArchiveReader(String url,
			ConsistencyLevelPolicy consistencyLevelPolicy,
			FailoverPolicy failoverPolicy) {
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
		initialize(hostsList, port, keyspaceName, consistencyLevelPolicy,
				failoverPolicy, username, password);
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
	 * @param consistencyLevelPolicy
	 *            consistency-level policy to use.
	 * @param failoverPolicy
	 *            fail-over policy to user.
	 * @param username
	 *            username to use for authentication or <code>null</code> to use
	 *            no authentication.
	 * @param password
	 *            password to use for authentication or <code>null</code> to use
	 *            no authentication.
	 */
	public CassandraArchiveReader(String cassandraHosts, int cassandraPort,
			String keyspaceName, ConsistencyLevelPolicy consistencyLevelPolicy,
			FailoverPolicy failoverPolicy, String username, String password) {
		initialize(cassandraHosts, cassandraPort, keyspaceName,
				consistencyLevelPolicy, failoverPolicy, username, password);
	}

	/**
	 * Creates an archive reader backed by the specified keyspace. The
	 * connection to the database is not closed when the {@link #close()} method
	 * is called.
	 * 
	 * @param keyspace
	 *            keyspace that stores the configuration and samples.
	 */
	public CassandraArchiveReader(Keyspace keyspace) {
		initialize(keyspace);
	}

	private void initialize(String cassandraHosts, int cassandraPort,
			String keyspaceName, ConsistencyLevelPolicy consistencyLevelPolicy,
			FailoverPolicy failoverPolicy, String username, String password) {
		CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator();
		hostConfigurator.setHosts(cassandraHosts);
		hostConfigurator.setPort(cassandraPort);
		// We use a random identifier for the cluster, because there might be
		// several instances of the archive reader, which use the same URL.
		// If we used the URL as the cluster name, the cluster shared by all
		// readers would be shut down, when one reader is closed.
		this.cluster = HFactory.getOrCreateCluster(
				UUID.randomUUID().toString(), hostConfigurator);
		TreeMap<String, String> credentials = new TreeMap<String, String>();
		if (username != null && password != null) {
			credentials.put("username", username);
			credentials.put("password", password);
		}
		Keyspace keyspace = HFactory.createKeyspace(keyspaceName, this.cluster,
				consistencyLevelPolicy, failoverPolicy, credentials);
		initialize(keyspace);
	}

	private void initialize(Keyspace keyspace) {
		this.keyspace = keyspace;
		this.config = new CassandraArchiveConfig(this.keyspace);
		this.sampleStore = new SampleStore(this.keyspace);
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
				end, CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME,
				cancelationProvider);
	}

	@Override
	public ValueIterator getOptimizedValues(int key, String name,
			ITimestamp start, ITimestamp end, int count)
			throws UnknownChannelException, Exception {
		CompressionLevelConfig[] compressionLevelConfigs = config
				.findCompressionLevelConfigs(name);
		TreeMap<Long, String> compressionPeriodToName = new TreeMap<Long, String>();
		for (CompressionLevelConfig compressionLevelConfig : compressionLevelConfigs) {
			compressionPeriodToName.put(
					compressionLevelConfig.getCompressionPeriod(),
					compressionLevelConfig.getCompressionLevelName());
		}
		// Ensure that "raw" compression level is always included in map
		compressionPeriodToName.put(0L,
				CompressionLevelConfig.RAW_COMPRESSION_LEVEL_NAME);
		if (compressionPeriodToName.size() == 1) {
			// There are only raw samples
			return getRawValues(key, name, start, end);
		}
		double requestedPeriod = TimestampArithmetics.substract(end, start)
				.toDouble();
		long requestedResolution = Math.round(requestedPeriod / count);
		return new CassandraMultiCompressionLevelValueIterator(sampleStore,
				config, name, start, end, compressionPeriodToName,
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
		if (cluster != null) {
			HFactory.shutdownCluster(cluster);
			cluster = null;
		}
	}

}
