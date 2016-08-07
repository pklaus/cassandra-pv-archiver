/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.aquenos.cassandra.pvarchiver.server.archiving.internal.ArchiveConfigurationUtils;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;

/**
 * Import of the channel configurations for an archive server from XML. This
 * class can be used to read a file that has been generated with
 * {@link ArchiveServerConfigurationXmlExport}.
 * 
 * @author Sebastian Marsching
 */
public class ArchiveServerConfigurationXmlImport {

    private static final DocumentBuilderFactory DOCUMENT_BUILDER_FACTORY;
    private static final UUID INTERNAL_UUID = UUID
            .fromString("d91f4ceb-d0b7-4968-a83e-bea862bc9a81");
    private static final String XML_NS_URI = "http://www.aquenos.com/2016/xmlns/cassandra-pv-archiver-configuration";

    static {
        DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();
        DOCUMENT_BUILDER_FACTORY.setNamespaceAware(true);
        SchemaFactory schemaFactory = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema;
        try {
            schema = schemaFactory
                    .newSchema(ArchiveServerConfigurationXmlImport.class
                            .getClassLoader()
                            .getResource(
                                    "META-INF/xsd/cassandra-pv-archiver-configuration.xsd"));
        } catch (SAXException e) {
            throw new RuntimeException(
                    "Could not load XML schema for configuration files.");
        }
        DOCUMENT_BUILDER_FACTORY.setSchema(schema);
    }

    /**
     * Logger for this class.
     */
    protected Log log = LogFactory.getLog(this.getClass());

    private List<ChannelConfiguration> channels;

    /**
     * Creates an imported archive-server configuration from an XML file. This
     * constructor is suitable to read a file that has been generated with
     * {@link ArchiveServerConfigurationXmlExport#serialize(java.io.OutputStream, String)}
     * .
     * 
     * @param inputStream
     *            input stream from which the configuration file is read. The
     *            encoding is automatically discovered from the XML declaration.
     * @throws RuntimeException
     *             if there is a problem with the configuration file (e.g. an
     *             I/O error or an invalid format).
     */
    public ArchiveServerConfigurationXmlImport(InputStream inputStream) {
        DocumentBuilder documentBuilder;
        try {
            documentBuilder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(
                    "Unexpected ParserConfigurationException: "
                            + e.getMessage(), e);
        }
        documentBuilder.setErrorHandler(new ErrorHandler() {

            @Override
            public void warning(SAXParseException exception)
                    throws SAXException {
                if (exception.getLineNumber() != -1
                        && exception.getColumnNumber() != -1) {
                    log.warn("Warning in line " + exception.getLineNumber()
                            + ", column " + exception.getColumnNumber()
                            + " while parsing configuration file: "
                            + exception.getMessage());
                } else {
                    log.warn("Warning while parsing configuration file: "
                            + exception.getMessage());
                }
            }

            @Override
            public void fatalError(SAXParseException exception)
                    throws SAXException {
                if (exception.getLineNumber() != -1
                        && exception.getColumnNumber() != -1) {
                    throw new RuntimeException("Fatal error in line "
                            + exception.getLineNumber() + ", column "
                            + exception.getColumnNumber() + ": "
                            + exception.getMessage(), exception);
                } else {
                    throw new RuntimeException("Fatal error: "
                            + exception.getMessage(), exception);
                }
            }

            @Override
            public void error(SAXParseException exception) throws SAXException {
                if (exception.getLineNumber() != -1
                        && exception.getColumnNumber() != -1) {
                    throw new RuntimeException("Error in line "
                            + exception.getLineNumber() + ", column "
                            + exception.getColumnNumber() + ": "
                            + exception.getMessage(), exception);
                } else {
                    throw new RuntimeException("Error: "
                            + exception.getMessage(), exception);
                }
            }
        });
        Document document;
        try {
            document = documentBuilder.parse(inputStream);
        } catch (IOException | SAXException e) {
            String exceptionMessage = e.getMessage();
            if (exceptionMessage == null) {
                exceptionMessage = e.getClass().getName();
            }
            throw new RuntimeException("Parsing the XML document failed: "
                    + exceptionMessage, e);
        }
        Element serverConfigurationElement = document.getDocumentElement();
        // The validation should ensure that we only process well-formed
        // documents. However, if we decide to extend the schema in the future,
        // there might be more than one valid root element, so we check that we
        // have the correct root element just to be sure.
        if (!XML_NS_URI.equals(serverConfigurationElement.getNamespaceURI())
                || !"server-configuration".equals(serverConfigurationElement
                        .getLocalName())) {
            throw new RuntimeException("Invalid root-element: "
                    + serverConfigurationElement.getLocalName()
                    + " (namespace "
                    + serverConfigurationElement.getNamespaceURI() + ")");
        }
        // The XML schema ensures that the document is well-formed, so from here
        // we can parse it without any checks. The XML schema also takes care of
        // adding default values for attributes that are not required, so we do
        // not have to deal with missing attributes, either.
        List<Element> channelElements = DomUtils.getChildElementsByTagName(
                serverConfigurationElement, "channel");
        List<ChannelConfiguration> channels = new ArrayList<ChannelConfiguration>(
                channelElements.size());
        for (Element channelElement : channelElements) {
            String name = channelElement.getAttribute("name");
            String controlSystemType = channelElement
                    .getAttribute("control-system-type");
            boolean enabled = Boolean.parseBoolean(channelElement
                    .getAttribute("enabled"));
            Element rawSamplesElement = DomUtils.getChildElementByTagName(
                    channelElement, "raw-samples");
            TreeMap<Integer, Integer> decimationLevelToRetentionPeriod = new TreeMap<Integer, Integer>();
            if (rawSamplesElement == null) {
                decimationLevelToRetentionPeriod.put(0, 0);
            } else {
                int retentionPeriod = Integer.parseInt(
                        rawSamplesElement.getAttribute("retention-period"), 10);
                decimationLevelToRetentionPeriod.put(0, retentionPeriod);
            }
            for (Element decimationLevelElement : DomUtils
                    .getChildElementsByTagName(channelElement,
                            "decimation-level")) {
                int decimationPeriod = Integer.parseInt(decimationLevelElement
                        .getAttribute("decimation-period"), 10);
                int retentionPeriod = Integer
                        .parseInt(decimationLevelElement
                                .getAttribute("retention-period"), 10);
                decimationLevelToRetentionPeriod.put(decimationPeriod,
                        retentionPeriod);
            }
            TreeMap<String, String> options = new TreeMap<String, String>();
            for (Element controlSystemOptionElement : DomUtils
                    .getChildElementsByTagName(channelElement,
                            "control-system-option")) {
                String optionName = controlSystemOptionElement
                        .getAttribute("name");
                String optionValue = controlSystemOptionElement
                        .getAttribute("value");
                options.put(optionName, optionValue);
            }
            // The XML Schema cannot verify that retention periods are
            // well-ordered. Therefore, we check this manually here. The XML
            // Schema already verified that the retention periods do not contain
            // negative or null elements, so we can take a shortcut.
            try {
                ArchiveConfigurationUtils
                        .verifyRetentionPeriodsAscending(decimationLevelToRetentionPeriod);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Invalid definition for channel \""
                        + StringEscapeUtils.escapeJava(name) + "\": "
                        + e.getMessage());
            }
            ChannelConfiguration channel = new ChannelConfiguration(
                    INTERNAL_UUID, name, controlSystemType,
                    Maps.transformValues(decimationLevelToRetentionPeriod,
                            Functions.constant(-1L)),
                    decimationLevelToRetentionPeriod, enabled, options,
                    INTERNAL_UUID);
            channels.add(channel);
        }
        this.channels = Collections.unmodifiableList(channels);
    }

    /**
     * Returns the channels that have been imported from the configuration file.
     * The channels' server ID is not part of the configuration file and is thus
     * initialized with an arbitrary value.
     * 
     * @return list of channel configurations that were imported from the
     *         configuration file.
     */
    public List<ChannelConfiguration> getChannels() {
        return channels;
    }

}
