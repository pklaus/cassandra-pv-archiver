/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.lang3.StringEscapeUtils;

import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * XML export of the channel configurations for an archive server.
 * 
 * @author Sebastian Marsching
 */
public class ArchiveServerConfigurationXmlExport {

    private static final String NEWLINE = "\r\n";
    private static final String XML_NS_URI = "http://www.aquenos.com/2016/xmlns/cassandra-pv-archiver-configuration";
    private static final String XML_SCHEMA_LOCATION = "http://www.aquenos.com/2016/xmlns/cassandra-pv-archiver-configuration-3.0.0.xsd";

    private static void writeChannelEmpty(XMLStreamWriter xmlStreamWriter,
            ChannelConfiguration channel) throws XMLStreamException {
        xmlStreamWriter.writeCharacters("  ");
        xmlStreamWriter.writeEmptyElement(XML_NS_URI, "channel");
        xmlStreamWriter.writeAttribute("name", channel.getChannelName());
        xmlStreamWriter.writeAttribute("control-system-type",
                channel.getControlSystemType());
        if (!channel.isEnabled()) {
            xmlStreamWriter.writeAttribute("enabled", "false");
        }
        xmlStreamWriter.writeCharacters(NEWLINE);
    }

    private static void writeChannelEnd(XMLStreamWriter xmlStreamWriter)
            throws XMLStreamException {
        xmlStreamWriter.writeCharacters("  ");
        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeCharacters(NEWLINE);
    }

    private static void writeChannelStart(XMLStreamWriter xmlStreamWriter,
            ChannelConfiguration channel) throws XMLStreamException {
        xmlStreamWriter.writeCharacters("  ");
        xmlStreamWriter.writeStartElement(XML_NS_URI, "channel");
        xmlStreamWriter.writeAttribute("name", channel.getChannelName());
        xmlStreamWriter.writeAttribute("control-system-type",
                channel.getControlSystemType());
        if (!channel.isEnabled()) {
            xmlStreamWriter.writeAttribute("enabled", "false");
        }
        xmlStreamWriter.writeCharacters(NEWLINE);
    }

    private List<ChannelConfiguration> channels;

    /**
     * Creates an XML export of the specified channels. The constructor actually
     * only validates the specified list of channels. The serialization to XML
     * has to be started by calling the {@link #serialize(OutputStream, String)}
     * method.
     * 
     * @param channels
     *            list of channel configurations.
     * @throws IllegalArgumentException
     *             if <code>channels</code> contains more than one configuration
     *             object for the same channel name.
     * @throws NullPointerException
     *             if <code>channels</code> is <code>null</code> or contains
     *             <code>null</code> elements.
     */
    public ArchiveServerConfigurationXmlExport(
            List<ChannelConfiguration> channels) {
        this.channels = ImmutableList.copyOf(channels);
        HashSet<String> seenChannelNames = new HashSet<String>();
        for (ChannelConfiguration channel : this.channels) {
            if (!seenChannelNames.add(channel.getChannelName())) {
                throw new IllegalArgumentException(
                        "Channel \""
                                + StringEscapeUtils.escapeJava(channel
                                        .getChannelName())
                                + "\" is present more than once.");
            }
        }
    }

    /**
     * Serializes the channel configurations represented by this object to XML.
     * 
     * @param outputStream
     *            output stream to which the XML data is supposed to be written.
     * @param encoding
     *            character encoding to use for the XML data. If
     *            <code>null</code>, "UTF-8" is used.
     * @throws NullPointerException
     *             if <code>outputStream</code> is <code>null</code>.
     * @throws RuntimeException
     *             if the serialization fails (most likely because of an I/O
     *             error).
     */
    public void serialize(OutputStream outputStream, String encoding) {
        Preconditions.checkNotNull(outputStream);
        if (encoding == null) {
            encoding = "UTF-8";
        }
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
        try {
            XMLStreamWriter xmlStreamWriter;
            xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(
                    outputStream, encoding);
            xmlStreamWriter.writeStartDocument(encoding, "1.0");
            xmlStreamWriter.writeCharacters(NEWLINE);
            xmlStreamWriter.writeStartElement("server-configuration");
            xmlStreamWriter.writeDefaultNamespace(XML_NS_URI);
            xmlStreamWriter.writeNamespace("xsi",
                    XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
            xmlStreamWriter.writeAttribute(
                    XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI,
                    "schemaLocation", XML_NS_URI + " " + XML_SCHEMA_LOCATION);
            xmlStreamWriter.writeCharacters(NEWLINE);
            for (ChannelConfiguration channel : channels) {
                boolean channelStartWritten = false;
                // We can simply iterate over the decimation levels because they
                // are always ordered by their decimation period and thus the
                // raw samples always come first.
                for (Map.Entry<Integer, Integer> decimationLevel : channel
                        .getDecimationLevelToRetentionPeriod().entrySet()) {
                    int decimationPeriod = decimationLevel.getKey();
                    int retentionPeriod = decimationLevel.getValue();
                    if (decimationPeriod == 0) {
                        if (retentionPeriod > 0) {
                            if (!channelStartWritten) {
                                writeChannelStart(xmlStreamWriter, channel);
                                channelStartWritten = true;
                            }
                            xmlStreamWriter.writeCharacters("    ");
                            xmlStreamWriter.writeEmptyElement(XML_NS_URI,
                                    "raw-samples");
                            xmlStreamWriter.writeAttribute("retention-period",
                                    String.valueOf(retentionPeriod));
                            xmlStreamWriter.writeCharacters(NEWLINE);
                        }
                    } else {
                        if (!channelStartWritten) {
                            writeChannelStart(xmlStreamWriter, channel);
                            channelStartWritten = true;
                        }
                        xmlStreamWriter.writeCharacters("    ");
                        xmlStreamWriter.writeEmptyElement(XML_NS_URI,
                                "decimation-level");
                        xmlStreamWriter.writeAttribute("decimation-period",
                                String.valueOf(decimationPeriod));
                        if (retentionPeriod > 0) {
                            xmlStreamWriter.writeAttribute("retention-period",
                                    String.valueOf(retentionPeriod));
                        }
                        xmlStreamWriter.writeCharacters(NEWLINE);
                    }
                }
                for (Map.Entry<String, String> option : channel.getOptions()
                        .entrySet()) {
                    if (!channelStartWritten) {
                        writeChannelStart(xmlStreamWriter, channel);
                        channelStartWritten = true;
                    }
                    String optionName = option.getKey();
                    String optionValue = option.getValue();
                    xmlStreamWriter.writeCharacters("    ");
                    xmlStreamWriter.writeEmptyElement(XML_NS_URI,
                            "control-system-option");
                    xmlStreamWriter.writeAttribute("name", optionName);
                    if (!optionValue.isEmpty()) {
                        xmlStreamWriter.writeAttribute("value", optionValue);
                    }
                    xmlStreamWriter.writeCharacters(NEWLINE);
                }
                if (channelStartWritten) {
                    writeChannelEnd(xmlStreamWriter);
                } else {
                    writeChannelEmpty(xmlStreamWriter, channel);
                }
            }
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeCharacters(NEWLINE);
            xmlStreamWriter.writeEndDocument();
            return;
        } catch (XMLStreamException e) {
            throw new RuntimeException("Serialization to XML failed: "
                    + e.getMessage(), e);
        }
    }

}
