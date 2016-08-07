/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.spring;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.UUID;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * <p>
 * Configuration properties for configuring the archive server. These properties
 * define settings that may be different for different servers. This object is
 * injected with properties having the <code>server.*</code> prefix.
 * </p>
 * 
 * <p>
 * The <code>server.uuid</code> property allows setting various options that are
 * only desirable during development (e.g. turning off caches) in a single
 * place.
 * </p>
 * 
 * <p>
 * Instances of this class are safe for concurrent read access but are not safe
 * for concurrent write access. Typically, this should not be a problem because
 * an instance of this class is initialized once at application startup and then
 * only used for read access.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 */
@ConfigurationProperties(prefix = "server", ignoreUnknownFields = false)
public class ServerProperties implements InitializingBean {

    /**
     * Logger for this class. The logger is responsible for writing diagnostic
     * messages related to activities of this class.
     */
    protected final Log logger = LogFactory.getLog(getClass());

    private InetAddress listenAddress;
    private int adminPort = 4812;
    private int archiveAccessPort = 9812;
    private int interNodeCommunicationPort = 9813;
    private UUID uuid;
    private File uuidFile;

    /**
     * Returns the address on which the server is supposed to listen. If the
     * address is not specified explicitly, it defaults to address returned by
     * {@link InetAddress#getLocalHost()}. If that method returns a loop-back
     * address, the first non-loop-back address found on any interface is used.
     * If the first interface with valid addresses has both an IPv4 and an IPv6
     * address, the IPv4 address is preferred.
     * 
     * @return address on which the server is supposed to listen (never
     *         <code>null</code>).
     */
    public InetAddress getListenAddress() {
        return listenAddress;
    }

    /**
     * Sets the address on which the server is supposed to listen. If the
     * address is not specified explicitly, it defaults to address returned by
     * {@link InetAddress#getLocalHost()}. If that method returns a loop-back
     * address, the first non-loop-back address found on any interface is used.
     * If the first interface with valid addresses has both an IPv4 and an IPv6
     * address, the IPv4 address is preferred.
     * 
     * @param listenAddress
     *            address on which the server is supposed to listen. May be
     *            specified in the form of a numeric address or a resolvable
     *            host-name. This should not be a loop-back address because
     *            other servers will try to contact this server on this address.
     *            If <code>null</code> or the empty string, the listen address
     *            is going to be determined automatically.
     * @throws IllegalArgumentException
     *             if the specified <code>listenAddress</code> is neither
     *             <code>null</code> nor an empty string but cannot be converted
     *             to an {@link InetAddress}.
     */
    public void setListenAddress(String listenAddress)
            throws IllegalArgumentException {
        // If the specified address is empty, we set null. This ensures that we
        // will try to determine the address in afterPropertiesSet().
        if (listenAddress == null || listenAddress.trim().isEmpty()) {
            this.listenAddress = null;
            return;
        }
        try {
            this.listenAddress = InetAddress.getByName(listenAddress);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Address \""
                    + StringEscapeUtils.escapeJava(listenAddress)
                    + "\" cannot be converted to an InetAddress.", e);
        }
    }

    /**
     * Returns the TCP port number on which the web server for the
     * administrative interface listens. This server offers access through a
     * graphical user-interface and through a web-service API.
     * 
     * @return TCP port number for the administrative interface.
     */
    public int getAdminPort() {
        return adminPort;
    }

    /**
     * Sets the TCP port number on which the web server for the administrative
     * interface listens. This server offers access through a graphical
     * user-interface and through a web-service API. The default is 4812.
     * 
     * @param adminPort
     *            TCP port number for the administrative interface.
     * @throws IllegalArgumentException
     *             if the specified number is not a valid TCP port number.
     */
    public void setAdminPort(int adminPort) {
        if (adminPort < 1 || adminPort > 65535) {
            throw new IllegalArgumentException(
                    "The port number must be between 1 and 65535");
        }
        this.adminPort = adminPort;
    }

    /**
     * Returns the TCP port number on which the web server for the access to the
     * archive listens. This server offers access to the data stored in the
     * archive through a web-service API.
     * 
     * @return TCP port number for the archive-access interface.
     */
    public int getArchiveAccessPort() {
        return archiveAccessPort;
    }

    /**
     * Sets the TCP port number on which the web server for the access to the
     * archive listens. This server offers access to the data stored in the
     * archive through a web-service API. The default is 9812.
     * 
     * @param archiveAccessPort
     *            TCP port number for the archive-access interface.
     * @throws IllegalArgumentException
     *             if the specified number is not a valid TCP port number.
     */
    public void setArchiveAccessPort(int archiveAccessPort) {
        if (archiveAccessPort < 1 || archiveAccessPort > 65535) {
            throw new IllegalArgumentException(
                    "The port number must be between 1 and 65535");
        }
        this.archiveAccessPort = archiveAccessPort;
    }

    /**
     * Returns the TCP port number on which the web server for the inter-node
     * communication listens. This server is used by the archive servers
     * belonging to the same cluster in order to communicate with each other.
     * 
     * @return TCP port number for the inter-node communication.
     */
    public int getInterNodeCommunicationPort() {
        return interNodeCommunicationPort;
    }

    /**
     * Sets the TCP port number on which the web server for the inter-node
     * communication listens. This server is used by the archive servers
     * belonging to the same cluster in order to communicate with each other.
     * The default is 9813.
     * 
     * @param interNodeCommunicationPort
     *            TCP port number for the inter-node communication interface.
     * @throws IllegalArgumentException
     *             if the specified number is not a valid TCP port number.
     */
    public void setInterNodeCommunicationPort(int interNodeCommunicationPort) {
        if (interNodeCommunicationPort < 1
                || interNodeCommunicationPort > 65535) {
            throw new IllegalArgumentException(
                    "The port number must be between 1 and 65535");
        }
        this.interNodeCommunicationPort = interNodeCommunicationPort;
    }

    /**
     * Returns the UUID identifying this server or <code>null</code> if the UUID
     * has not been set. The UUID must be set in order for a server to operate
     * correctly and must be unique within the whole cluster.
     * 
     * @return UUID identifying this server or <code>null</code> if the UUID has
     *         not been set.
     * @see #setUuid(String)
     * @see #setUuidFile(File)
     */
    public UUID getUuid() {
        return uuid;
    }

    /**
     * Sets the UUID identifying this server. The UUID must be set in order for
     * a server to operate correctly and must be unique within the whole
     * cluster. Setting the UUID through this method overwrites any UUID set
     * previously through {@link #setUuid(String)} or {@link #setUuidFile(File)}
     * . If the specified string does not represent a valid UUID, the UUID is
     * set to <code>null</code>.
     * 
     * @param uuid
     *            string representing the UUID to be set (may be
     *            <code>null</code>).
     * @see #setUuidFile(File)
     */
    public void setUuid(String uuid) {
        // We reset the reference to the UUID file because we want the UUID file
        // to refer to the file from which the current UUID has been read and we
        // are now going to change the UUID.
        this.uuidFile = null;
        if (uuid == null || uuid.isEmpty()) {
            this.uuid = null;
            return;
        }
        try {
            this.uuid = UUID.fromString(uuid);
        } catch (IllegalArgumentException e) {
            logger.error(
                    "Cannot set UUID from string \""
                            + StringEscapeUtils.escapeJava(uuid) + "\".", e);
            this.uuid = null;
        }
    }

    /**
     * Returns the file from which the current UUID has been read or
     * <code>null</code> if the current UUID has not been read from a file.
     * 
     * @return file from which the current UUID has been read or
     *         <code>null</code> if the current UUID has not been read from a
     *         file.
     */
    public File getUuidFile() {
        return uuidFile;
    }

    /**
     * Sets the current UUID by reading it from a file. If the specified file
     * does not exist but can be created, a new random UUID is generated and
     * written to the file. If the file does not exist and cannot be created or
     * if the file exists but does not contain a valid UUID, this method does
     * nothing.
     * 
     * @param uuidFile
     *            file from which the UUID shall be read.
     */
    public void setUuidFile(File uuidFile) {
        // We do not want to read the UUID from a file if it has been set
        // explicitly.
        if (uuid != null) {
            return;
        }
        try {
            if (uuidFile.createNewFile()) {
                FileOutputStream outputStream = new FileOutputStream(uuidFile);
                BufferedWriter writer = null;
                try {
                    writer = new BufferedWriter(new OutputStreamWriter(
                            outputStream, Charset.forName("UTF-8")));
                    UUID uuid = UUID.randomUUID();
                    writer.write(uuid.toString());
                    // We close the writer so that the file handle is closed.
                    // This also flushes the writer, so that the file is
                    // updated as intended.
                    writer.close();
                    // Now that we have written a UUID to the file, we should be
                    // able to read it when we call this method again.
                    setUuidFile(uuidFile);
                } finally {
                    if (writer != null) {
                        writer.close();
                    }
                    outputStream.close();
                }
            } else {
                if (uuidFile.exists()) {
                    if (uuidFile.length() == 0L) {

                    }
                    FileInputStream inputStream = new FileInputStream(uuidFile);
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(
                                inputStream, Charset.forName("UTF-8")));
                        String line = reader.readLine();
                        if (line == null || line.isEmpty()) {
                            // The file is not completely empty (we checked
                            // earlier), but we could not read a line, so there
                            // is nothing we can do.
                            logger.error("Cannot read UUID from file \""
                                    + StringEscapeUtils.escapeJava(uuidFile
                                            .getPath())
                                    + "\": The file has an invalid format.");
                            return;
                        }
                        // We set our UUID to the UUID read from the file.
                        setUuid(line.trim());
                        if (getUuid() == null) {
                            // Setting the UUID failed - most likely because the
                            // UUID in the file is malformed. The problem has
                            // already been logged by the setUuid(...) method,
                            // so we can simply return.
                            return;
                        }
                        // If the UUID has been set successfully, we also want
                        // to remember which UUID file we used.
                        this.uuidFile = uuidFile;
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                        inputStream.close();
                    }
                } else {
                    // The UUID file was not created and does not exist either.
                    // There is nothing we can do, so we simply log the error
                    // and return.
                    logger.error("Creation of UUID file \""
                            + StringEscapeUtils.escapeJava(uuidFile.getPath())
                            + "\" failed.");
                    return;
                }
            }
        } catch (IOException e) {
            logger.error("Error while trying to read or write UUID file \""
                    + StringEscapeUtils.escapeJava(uuidFile.getPath()) + "\".",
                    e);
            return;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // Check that we have a server UUID. The corresponding properties are
        // already checked before this bean is initialized, but it is possible
        // that the UUID file is set but the UUID cannot be read for some
        // reason.
        if (getUuid() == null) {
            throw new IllegalStateException(
                    "At least one of the server.uuid and server.uuidFile properties has to be set.");
        }
        // If the listen address has not been set explicitly, we try to
        // determine it.
        if (listenAddress == null) {
            this.listenAddress = determineListenAddress();
        }
    }

    private static InetAddress determineListenAddress() {
        try {
            InetAddress address;
            address = InetAddress.getLocalHost();
            // We cannot use the loop-back address because other hosts in the
            // cluster would try to connect to themselves.
            if (!address.isLoopbackAddress()) {
                return address;
            } else {
                address = null;
            }
        } catch (UnknownHostException e) {
            // We ignore this exception and carry on with the next step.
        }
        // Either resolving the hosts name did not work or we got the loop-back
        // address. Either way, we have to try to determine the correct address
        // in a different way. We iterate over all interfaces and take the first
        // address from the first interface that is up and where the address is
        // not a loop-back address.
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface
                    .getNetworkInterfaces();
            while (networkInterfaceEnumeration.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaceEnumeration
                        .nextElement();
                // We ignore interfaces that are not up.
                if (!networkInterface.isUp()) {
                    continue;
                }
                // An interface may have multiple addresses. We check every
                // address whether it is suitable (is a non-loop-back unicast
                // address) and add it to a candidate list. If the candidate
                // list contains an IPv4 address, we use that address.
                // Otherwise, we use the first address that we found.
                LinkedList<InetAddress> candidateAddresses = new LinkedList<InetAddress>();
                for (InterfaceAddress interfaceAddress : networkInterface
                        .getInterfaceAddresses()) {
                    InetAddress address = interfaceAddress.getAddress();
                    // We ignore loopback, link-local, and multicast addresses.
                    if (!address.isLoopbackAddress()
                            && !address.isLinkLocalAddress()
                            && !address.isMulticastAddress()) {
                        candidateAddresses.add(address);
                    }
                }
                // We prefer an IPv4 over an IPv6 address. This is simply due to
                // the fact that there are more networks where IPv4 works
                // correctly but IPv6 does not than there are networks where
                // IPv6 works correctly but IPv4 does not. However, we only
                // consider the first interface that has any valid address (IPv4
                // or IPv6). This makes the results more consistent when
                // changing from a dual-stack to an IPv6-only configuration.
                for (InetAddress candidateAddress : candidateAddresses) {
                    if (candidateAddress instanceof Inet4Address) {
                        return candidateAddress;
                    }
                }
                // If there is no IPv4 address but we found another address, we
                // use that address.
                if (!candidateAddresses.isEmpty()) {
                    return candidateAddresses.getFirst();
                }
            }
        } catch (SocketException e) {
            // A SocketException simply means that we could not determine the
            // listen address automatically. We handle this the same way as when
            // we do not find a suitable address.
        }
        // If we got here, we did not find a suitable address. This is a fatal
        // problem because we cannot continue.
        throw new IllegalStateException(
                "The listen address has not been configured explicitly and could not be determined automatically.");
    }

}
