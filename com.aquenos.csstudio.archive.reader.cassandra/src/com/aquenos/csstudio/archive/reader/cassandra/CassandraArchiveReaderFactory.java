/*
 * Copyright 2012-2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.reader.cassandra;

import org.csstudio.archive.reader.ArchiveReader;
import org.csstudio.archive.reader.ArchiveReaderFactory;

import com.aquenos.csstudio.archive.cassandra.CassandraArchivePreferences;

/**
 * Creates instances of {@link CassandraArchiveReader}. Mainly used for an
 * extension for the the archive reader extension point.
 * 
 * @author Sebastian Marsching
 */
public class CassandraArchiveReaderFactory implements ArchiveReaderFactory {

    @Override
    public ArchiveReader getArchiveReader(String url) throws Exception {
        return new CassandraArchiveReader(url,
                CassandraArchivePreferences.getReadDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteDataConsistencyLevel(),
                CassandraArchivePreferences.getReadMetaDataConsistencyLevel(),
                CassandraArchivePreferences.getWriteMetaDataConsistencyLevel(),
                CassandraArchivePreferences.getRetryPolicy());
    }

}
