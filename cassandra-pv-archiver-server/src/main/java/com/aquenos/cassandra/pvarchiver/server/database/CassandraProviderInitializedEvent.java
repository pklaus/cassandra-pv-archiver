/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import org.springframework.context.ApplicationEvent;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Application event triggered by a {@link CassandraProvider} when it has been
 * fully initialized. When this event is received, it means that the
 * {@link CassandraProvider} is now able to provide {@link Cluster} and
 * {@link Session} instances. However, operations on these instances might still
 * fail if the connection to the whole cluster is lost in the meantime.
 * 
 * @author Sebastian Marsching
 */
public class CassandraProviderInitializedEvent extends ApplicationEvent {

    private static final long serialVersionUID = -7339402376926454394L;

    /**
     * Creates an event originating from the specified Cassandra provider.
     * 
     * @param source
     *            Cassandra provider that is now ready to be used.
     */
    public CassandraProviderInitializedEvent(CassandraProvider source) {
        super(source);
    }

    @Override
    public CassandraProvider getSource() {
        return (CassandraProvider) super.getSource();
    }

}
