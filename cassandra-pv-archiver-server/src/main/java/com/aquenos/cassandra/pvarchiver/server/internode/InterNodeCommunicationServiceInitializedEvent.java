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
 * Event signaling that the {@link InterNodeCommunicationService} has been
 * initialized completely and is ready for operation. Listeners can register for
 * this event by implementing the {@link ApplicationListener} interface or using
 * the {@link EventListener} annotation.
 * 
 * @author Sebastian Marsching
 */
public class InterNodeCommunicationServiceInitializedEvent extends
        ApplicationEvent {

    private static final long serialVersionUID = -1820540462280105010L;

    /**
     * Creates an event emitted by the specified source. Typically, this
     * constructor is only used by an instance of
     * {@link InterNodeCommunicationService}.
     * 
     * @param source
     *            the inter-node communication service that is sending the
     *            event.
     */
    public InterNodeCommunicationServiceInitializedEvent(
            InterNodeCommunicationService source) {
        super(source);
    }

    @Override
    public InterNodeCommunicationService getSource() {
        return (InterNodeCommunicationService) super.getSource();
    }

}
