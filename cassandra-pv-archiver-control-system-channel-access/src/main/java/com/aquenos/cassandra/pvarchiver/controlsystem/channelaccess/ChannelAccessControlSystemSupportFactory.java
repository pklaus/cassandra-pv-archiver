/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess;

import java.util.Map;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupportFactory;
import com.datastax.driver.core.Session;

/**
 * Factory for the {@link ChannelAccessControlSystemSupport}. This factory uses
 * the configuration prefix "channelAccess" for configuration options passed to
 * {@link #createControlSystemSupport(Map, Session)}.
 * 
 * @author Sebastian Marsching
 */
public class ChannelAccessControlSystemSupportFactory implements
        ControlSystemSupportFactory {

    @Override
    public ChannelAccessControlSystemSupport createControlSystemSupport(
            Map<String, String> configuration, Session session) {
        return new ChannelAccessControlSystemSupport(configuration, session);
    }

    @Override
    public String getConfigurationPrefix() {
        return ChannelAccessControlSystemSupport.CONFIGURATION_OPTION_PREFIX;
    }

}
