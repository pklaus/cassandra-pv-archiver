/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.cluster;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.EventListener;

/**
 * Event sent by the {@link ClusterManagementService} when this server's online
 * status changes. Listeners can register for this event by implementing the
 * {@link ApplicationListener} interface or using the {@link EventListener}
 * annotation.
 * 
 * @author Sebastian Marsching
 */
public class ServerOnlineStatusEvent extends ApplicationEvent {

    private static final long serialVersionUID = -2285417044691498025L;

    private boolean online;

    /**
     * Creates an event sent by the specified cluster management service and
     * having the specified online status.
     * 
     * @param source
     *            cluster management service sending the event.
     * @param online
     *            <code>true</code> if this server is online now,
     *            <code>false</code> if it is offline now.
     */
    public ServerOnlineStatusEvent(ClusterManagementService source,
            boolean online) {
        super(source);
        this.online = online;
    }

    @Override
    public ClusterManagementService getSource() {
        return (ClusterManagementService) super.getSource();
    }

    /**
     * Tells the new online status of the server. <code>true</code> means that
     * the server changed from offline to online and <code>false</code> means
     * that it changed from online to offline.
     * 
     * @return this server's new online status.
     */
    public boolean isOnline() {
        return online;
    }

}
