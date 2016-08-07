/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Data access object for accessing the servers within the archiving cluster.
 * 
 * @author Sebastian Marsching
 */
public interface ClusterServersDAO {

    /**
     * Value object representing a server within the archiving cluster.
     * 
     * @author Sebastian Marsching
     *
     */
    public class ClusterServer {

        private String interNodeCommunicationUrl;
        private Date lastOnlineTime;
        private UUID serverId;
        private String serverName;

        /**
         * Creates a cluster server object.
         * 
         * @param interNodeCommunicationUrl
         *            URL of the web-service interface for the server. This
         *            interface is used by other servers in the cluster to
         *            communicate with the server.
         * @param lastOnlineTime
         *            time the server has last updated its registration in the
         *            database. This time can be used to determine whether the
         *            server is online.
         * @param serverId
         *            unique identifier of the server.
         * @param serverName
         *            human-readable identifier for the server. Typically, this
         *            is the host-name of the computer on which the server is
         *            running. For technical reasons, it is not guaranteed that
         *            this identifier is unique within the cluster.
         */
        public ClusterServer(String interNodeCommunicationUrl,
                Date lastOnlineTime, UUID serverId, String serverName) {
            this.interNodeCommunicationUrl = interNodeCommunicationUrl;
            this.lastOnlineTime = lastOnlineTime;
            this.serverId = serverId;
            this.serverName = serverName;
        }

        /**
         * Returns the URL of the web-service interface for the server. This
         * interface is used by other servers in the cluster to communicate with
         * the server.
         * 
         * @return URL of the web-service interface used for internal
         *         communication.
         */
        public String getInterNodeCommunicationUrl() {
            return interNodeCommunicationUrl;
        }

        /**
         * Returns the time the server has last updated its registration in the
         * database. This time can be used to determine whether the server is
         * online.
         * 
         * @return most recent time at which the server's registration has been
         *         updated.
         */
        public Date getLastOnlineTime() {
            return lastOnlineTime;
        }

        /**
         * Returns the unique identifier of the server.
         * 
         * @return unique identifier of the server.
         */
        public UUID getServerId() {
            return serverId;
        }

        /**
         * Returns a human-readable identifier for the server. Typically, this
         * is the host-name of the computer on which the server is running. For
         * technical reasons, it is not guaranteed that this identifier is
         * unique within the cluster.
         * 
         * @return human-readable identifier for the server.
         */
        public String getServerName() {
            return serverName;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(getInterNodeCommunicationUrl())
                    .append(getLastOnlineTime()).append(getServerId())
                    .append(getServerName()).toHashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ClusterServer)) {
                return false;
            }
            ClusterServer other = (ClusterServer) obj;
            return new EqualsBuilder()
                    .append(this.getInterNodeCommunicationUrl(),
                            other.getInterNodeCommunicationUrl())
                    .append(this.getLastOnlineTime(), other.getLastOnlineTime())
                    .append(this.getServerId(), other.getServerId())
                    .append(this.getServerName(), other.getServerName())
                    .isEquals();
        }

    }

    /**
     * Creates or updates the record for an archive server. If the record for
     * the specified <code>serverId</code> does not exist yet, it is created.
     * Otherwise, it is updated. The operation is performed in an asynchronous
     * way so that it will not block for network communication. The result of
     * the operation can be checked through the returned future.
     * 
     * @param serverId
     *            unique ID identifying the server within the cluster. This ID
     *            must be different for each server.
     * @param serverName
     *            name (typically the hostname) of the server. The name is used
     *            to help the user in identifying a server. It should be unique
     *            for each server, but this condition is not enforced.
     * @param interNodeCommunicationUrl
     *            base URL for the web-service interface of the server that can
     *            be used by other servers within the cluster to communicate
     *            with the server.
     * @param lastOnlineTime
     *            time the server last updated its status. This time is used by
     *            other servers to determine if the server is online. Typically,
     *            this parameter should be the current time when calling this
     *            method. The last-online time needs to be updated periodically,
     *            but the {@link #updateServerLastOnlineTime(UUID, Date)} should
     *            be preferred for this purpose.
     * @return a future that can be used to check whether this operation
     *         finished and whether it was successful. In case of failure, the
     *         future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> createOrUpdateServer(UUID serverId,
            String serverName, String interNodeCommunicationUrl,
            Date lastOnlineTime);

    /**
     * Removes the server with the specified ID from the database. If the
     * database does not store information for the specified ID, the operation
     * will still succeed. The operation is performed in an asynchronous way so
     * that it will not block for network communication. The result of the
     * operation can be checked through the returned future.
     * 
     * @param serverId
     *            unique identifier of the server that shall be removed from the
     *            database.
     * @return future that can be used to check whether this operation finished
     *         and whether it was successful. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> deleteServer(UUID serverId);

    /**
     * Reads the server object for the specified ID from the database. The
     * object is not returned directly but through a listenable future, so that
     * the calling code can wait for the database operation to finish
     * asynchronously.
     * 
     * @param serverId
     *            unique identifier identifying the server.
     * @return a future providing access to the server object with the specified
     *         ID. If the database does not contain information for the
     *         specified ID, the future's <code>get</code> method returns
     *         <code>null</code>. In case of failure, the future's
     *         <code>get()</code> method will throw an exception.
     */
    ListenableFuture<? extends ClusterServer> getServer(UUID serverId);

    /**
     * <p>
     * Reads the server objects for all servers from the database. The server
     * objects are not returned directly but through a listenable future, so
     * that the calling code can wait for the database operation to finish
     * asynchronously.
     * </p>
     * 
     * <p>
     * The iterable returned by the future is only safe for iterating once.
     * Subsequent requests to create an iterator might result in the new
     * iterator only returning the elements that have not been returned by a
     * previously created iterator yet. Besides, the iterator might block while
     * iterating, waiting for additional elements to arrive from the network.
     * </p>
     * 
     * @return a future providing access to the server objects representing all
     *         servers within the archiving cluster. In case of failure, the
     *         future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<? extends Iterable<? extends ClusterServer>> getServers();

    /**
     * Tells whether this DAO has finished initialization and is ready to be
     * used. Even if this method returns <code>true</code>, operations might
     * still fail because of transient problems.
     * 
     * @return <code>true</code> if this DAO has been initialized and is ready
     *         to be used, <code>false</code> if initialization has not
     *         completed yet.
     */
    boolean isInitialized();

    /**
     * Updates the last-online time for an archive server. This method should
     * only be used if the record for the server has already been created using
     * the {@link #createOrUpdateServer(UUID, String, String, Date)} method.
     * However, implementations might not enforce this limitation, silently
     * creating a partially initialized record when this method is called first.
     * 
     * @param serverId
     *            unique ID identifying the server within the cluster. This ID
     *            must be different for each server.
     * @param lastOnlineTime
     *            time the server last updated its status. This time is used by
     *            other servers to determine if the server is online. Typically,
     *            this parameter should be the current time when calling this
     *            method. The last-online time needs to be updated periodically.
     * @return a future that can be used to check whether this operation
     *         finished and whether it was successful. In case of failure, the
     *         future's <code>get()</code> method will throw an exception.
     */
    ListenableFuture<Void> updateServerLastOnlineTime(UUID serverId,
            Date lastOnlineTime);

}
