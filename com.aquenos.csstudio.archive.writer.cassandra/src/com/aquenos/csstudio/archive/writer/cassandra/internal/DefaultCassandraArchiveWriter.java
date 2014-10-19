/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import com.aquenos.csstudio.archive.cassandra.CassandraArchivePreferences;
import com.aquenos.csstudio.archive.writer.cassandra.CassandraArchiveWriter;
import com.aquenos.csstudio.archive.writer.cassandra.CassandraArchiveWriterPreferences;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Sub-class of {@link CassandraArchiveWriter} that provides a default
 * constructor, reading the settings from {@link CassandraArchivePreferences}.
 * The main purpose of this class is to provide an extension for the archive
 * writer extension point. This class is only intended for internal use by the
 * classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class DefaultCassandraArchiveWriter extends CassandraArchiveWriter {
    public DefaultCassandraArchiveWriter() throws ConnectionException {
        super(CassandraArchivePreferences.getHosts(),
                CassandraArchivePreferences.getPort(),
                CassandraArchivePreferences.getKeyspace(),
                CassandraArchivePreferences.getReadDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteDataConsistencyLevel(),
                CassandraArchivePreferences.getReadMetaDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteMetaDataConsistencyLevel(),
                CassandraArchivePreferences.getRetryPolicy(),
                CassandraArchivePreferences.getUsername(),
                CassandraArchivePreferences.getPassword(),
                CassandraArchiveWriterPreferences
                        .getNumberOfCompressorWorkers());
    }

}
