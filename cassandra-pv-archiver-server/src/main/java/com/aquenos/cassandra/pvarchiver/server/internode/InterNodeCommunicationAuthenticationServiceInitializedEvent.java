/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.EventListener;

/**
 * Event signaling that the {@link InterNodeCommunicationAuthenticationService}
 * has been initialized completely and is ready for operation. Listeners can
 * register for this event by implementing the {@link ApplicationListener}
 * interface or using the {@link EventListener} annotation.
 * 
 * @author Sebastian Marsching
 */
public class InterNodeCommunicationAuthenticationServiceInitializedEvent extends
        ApplicationEvent {

    private static final long serialVersionUID = -2942612775039452594L;

    /**
     * Creates an event emitted by the specified source. Typically, this
     * constructor is only used by an instance of
     * {@link InterNodeCommunicationAuthenticationService}.
     * 
     * @param source
     *            the inter-node-communication authentication service that is
     *            sending the event.
     */
    public InterNodeCommunicationAuthenticationServiceInitializedEvent(
            InterNodeCommunicationAuthenticationService source) {
        super(source);
    }

    @Override
    public InterNodeCommunicationAuthenticationService getSource() {
        return (InterNodeCommunicationAuthenticationService) super.getSource();
    }

}
