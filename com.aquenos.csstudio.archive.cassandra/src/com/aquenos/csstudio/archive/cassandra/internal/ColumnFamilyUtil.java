/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;

/**
 * Utility for maintenance operation on column families. This class is only
 * intended for interal use by other classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class ColumnFamilyUtil {

    public static void createOrVerifyColumnFamily(Cluster cluster,
            Keyspace keyspace, String cfName, String keyValidationClass,
            String comparatorType, String defaultValidationClass,
            List<ColumnDefinition> columnDefinitions) {
        if (keyValidationClass == null) {
            keyValidationClass = "BytesType";
        }
        if (comparatorType == null) {
            comparatorType = "BytesType";
        }
        if (defaultValidationClass == null) {
            defaultValidationClass = "BytesType";
        }
        try {
            ColumnFamilyDefinition cfDef = keyspace.describeKeyspace()
                    .getColumnFamily(cfName);
            if (cfDef != null) {
                // Astyanax does not provide a nice way to verify, that we
                // have a standard column-family, thus we skip this test.
                // We also do not check any advanced options (compression,
                // etc.), because they might differ for good reasons.
                // We only check the validators and comparators, as well as
                // the validators defined for the (fixed) columns.
                if (!simplifyComparatorType(cfDef.getKeyValidationClass())
                        .equals(simplifyComparatorType(keyValidationClass))) {
                    throw new RuntimeException("Column family \"" + cfName
                            + "\" has key validation class \""
                            + cfDef.getKeyValidationClass()
                            + "\" but key validation class \""
                            + keyValidationClass + "\" was expected.");
                }
                if (!simplifyComparatorType(cfDef.getComparatorType()).equals(
                        simplifyComparatorType(comparatorType))) {
                    throw new RuntimeException("Column family \"" + cfName
                            + "\" has comparator type \""
                            + cfDef.getComparatorType()
                            + "\" but comparator type \"" + comparatorType
                            + "\" was expected.");
                }
                if (!simplifyComparatorType(cfDef.getDefaultValidationClass())
                        .equals(simplifyComparatorType(defaultValidationClass))) {
                    throw new RuntimeException("Column family \"" + cfName
                            + "\" has default validation class \""
                            + cfDef.getDefaultValidationClass()
                            + "\" but default validation class \""
                            + defaultValidationClass + "\" was expected.");
                }
                Map<String, ColumnDefinition> columnNamesToDefinitions = new HashMap<String, ColumnDefinition>();
                for (ColumnDefinition cDef : cfDef.getColumnDefinitionList()) {
                    columnNamesToDefinitions.put(cDef.getName(), cDef);
                }
                for (ColumnDefinition cDef : columnDefinitions) {
                    ColumnDefinition cDef2 = columnNamesToDefinitions.get(cDef
                            .getName());
                    if (cDef2 == null) {
                        throw new RuntimeException("Column family \"" + cfName
                                + "\" is missing expected column \""
                                + cDef.getName() + "\".");
                    }
                    verifyEquals(cDef, cDef2, cfName);
                }
            } else {
                cfDef = cluster.makeColumnFamilyDefinition();
                cfDef.setName(cfName);
                cfDef.setKeyspace(keyspace.getKeyspaceName());
                cfDef.setKeyValidationClass(keyValidationClass);
                cfDef.setComparatorType(comparatorType);
                cfDef.setDefaultValidationClass(defaultValidationClass);
                if (columnDefinitions != null) {
                    for (ColumnDefinition cDef : columnDefinitions) {
                        cfDef.addColumnDefinition(cDef);
                    }
                }
                cluster.addColumnFamily(cfDef);
                try {
                    waitForSchemaAgreement(cluster);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    private static void verifyEquals(ColumnDefinition cDef,
            ColumnDefinition cDef2, String cfName) {
        // We do not use indices, thus we only check the validation class.
        if (!simplifyComparatorType(cDef.getValidationClass()).equals(
                simplifyComparatorType(cDef2.getValidationClass()))) {
            throw new RuntimeException("Column \"" + cDef.getName()
                    + "\" in column family \"" + cfName
                    + "\" has validation class \"" + cDef2.getValidationClass()
                    + "\" but validation class \"" + cDef.getValidationClass()
                    + "\" was expected.");
        }
    }

    private static String simplifyComparatorType(String type) {
        if (type == null) {
            return "BytesType";
        }
        int openingParenthesisIndex = type.indexOf('(');
        StringBuilder result = new StringBuilder();
        if (type.startsWith("org.apache.cassandra.db.marshal.")) {
            result.append(type.substring(
                    "org.apache.cassandra.db.marshal.".length(),
                    (openingParenthesisIndex > 0) ? openingParenthesisIndex
                            : type.length()).trim());
        } else {
            result.append(type.substring(
                    0,
                    (openingParenthesisIndex > 0) ? openingParenthesisIndex
                            : type.length()).trim());
        }
        if (openingParenthesisIndex == -1) {
            return result.toString();
        }
        // If we are here, there is an opening parenthesis in the string,
        // thus we have to process the "parameters" (for composite types).
        int closingParenthesisIndex = type.lastIndexOf(')');
        if (closingParenthesisIndex == -1) {
            throw new IllegalArgumentException(
                    "Unbalanced parantheses in type \"" + type + "\".");
        }
        String parametersList = type.substring(openingParenthesisIndex + 1,
                closingParenthesisIndex);
        result.append('(');
        boolean firstParameter = true;
        for (String parameter : parametersList.split(",")) {
            if (!firstParameter) {
                result.append(',');
            }
            firstParameter = false;
            result.append(simplifyComparatorType(parameter));
        }
        result.append(')');
        return result.toString();
    }

    private static void waitForSchemaAgreement(Cluster cluster)
            throws InterruptedException, ConnectionException {
        int waited = 0;
        int versions = 0;
        while (versions != 1) {
            versions = 0;
            Map<String, List<String>> schema = cluster.describeSchemaVersions();
            for (Map.Entry<String, List<String>> entry : schema.entrySet()) {
                if (!entry.getKey().equals("UNREACHABLE"))
                    versions++;
            }

            if (versions != 1) {
                Thread.sleep(1000L);
                waited += 1000L;
                if (waited > 30000L)
                    throw new RuntimeException(
                            "Could not reach schema agreement in " + 30000L
                                    + "ms");
            }
        }
    }

}
