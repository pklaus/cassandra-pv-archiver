/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.archiveaccess.controller;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveAccessService;
import com.aquenos.cassandra.pvarchiver.server.archiving.ChannelInformationCache;
import com.aquenos.cassandra.pvarchiver.server.archiving.TimeStampLimitMode;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;
import com.aquenos.cassandra.pvarchiver.server.database.ChannelMetaDataDAO.ChannelInformation;
import com.aquenos.cassandra.pvarchiver.server.util.FutureUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * Controller handling requests to the legacy archive-access API. This is the
 * API that is implemented by the JSON Archive Proxy. The API is made available
 * under the URI path <code>/archive-access/api/1.0/archive</code>.
 * 
 * @author Sebastian Marsching
 */
@Controller
@RequestMapping(value = "/archive-access/api/1.0/archive")
public class Api10Controller {

    private ArchiveAccessService archiveAccessService;
    private ChannelInformationCache channelInformationCache;
    private ControlSystemSupportRegistry controlSystemSupportRegistry;
    private JsonFactory jsonFactory = new JsonFactory();

    /**
     * Sets the archive access service. The archive access service is used for
     * retrieving samples from the archive. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param archiveAccessService
     *            archive access service for retrieving samples.
     */
    @Autowired
    public void setArchiveAccessService(
            ArchiveAccessService archiveAccessService) {
        this.archiveAccessService = archiveAccessService;
    }

    /**
     * Sets the channel information cache. The cache is used for reading
     * {@link ChannelInformation} objects from the database without having to
     * query the database every time. Typically, this method is called
     * automatically by the Spring container.
     * 
     * @param channelInformationCache
     *            channel information cache for getting
     *            {@link ChannelInformation} objects.
     */
    @Autowired
    public void setChannelInformationCache(
            ChannelInformationCache channelInformationCache) {
        this.channelInformationCache = channelInformationCache;
    }

    /**
     * Sets the control-system support registry. The registry is used to gain
     * access to the control-system supports available on this archiving server.
     * Typically, this method is called automatically by the Spring container.
     * 
     * @param controlSystemSupportRegistry
     *            control-system support registry for this archiving server.
     */
    @Autowired
    public void setControlSystemSupportRegistry(
            ControlSystemSupportRegistry controlSystemSupportRegistry) {
        this.controlSystemSupportRegistry = controlSystemSupportRegistry;
    }

    /**
     * Generates a list of archives served by this server. For this
     * implementation, this is a fixed list that always contains exactly one
     * entry.
     * 
     * @param request
     *            HTTP servlet request. The request is used to check whether the
     *            "prettyPrint" option has been specified by the client.
     * @param response
     *            HTTP servlet response. The HTTP servlet response is used for
     *            writing the response. directly
     * @throws IOException
     *             if there is an I/O error while writing the response.
     */
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    public void archiveInfo(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        JsonGenerator generator = createJsonGenerator(request, response);
        try {
            generator.writeStartArray();
            generator.writeStartObject();
            generator.writeNumberField("key", 1);
            generator.writeStringField("name", "Cassandra");
            generator.writeStringField("description", "Cassandra PV Archive");
            generator.writeEndObject();
            generator.writeEndArray();
        } finally {
            generator.close();
        }
    }

    /**
     * Generates a list of channel names that match the specified pattern. The
     * pattern is interpreted as a glob pattern (a pattern that may contain
     * question marks (?) for matching exactly one character and asterisks (*)
     * for matching an arbitrary number of characters. The matching is performed
     * in a case-insensitive way.
     * 
     * @param request
     *            HTTP servlet request. The request is used to extract the
     *            pattern from the request URI and to check whether the
     *            "prettyPrint" option has been specified by the client.
     * @param response
     *            HTTP servlet response. The HTTP servlet response is used for
     *            writing the response. directly
     * @throws IOException
     *             if there is an I/O error while writing the response.
     */
    @RequestMapping(value = "/1/channels-by-pattern/**", method = RequestMethod.GET)
    @ResponseBody
    public void channelsByPattern(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        // Due to the way how the client generates the request URI (in
        // particular how escaping is performed), we cannot rely on Spring's
        // path-variable support. Instead, we have to process the request URI
        // directly.
        String pattern = extractExtraPathInfo(request,
                "/1/channels-by-pattern/");
        if (pattern == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        channelsByPatternInternal(request, response,
                compileGlobPattern(pattern));
    }

    /**
     * Generates a list of channel names that match the specified regular
     * expression. The regular expression is compiled using
     * {@link Pattern#compile(String)}.
     * 
     * @param request
     *            HTTP servlet request. The request is used to extract the
     *            regular expression from the request URI and to check whether
     *            the "prettyPrint" option has been specified by the client.
     * @param response
     *            HTTP servlet response. The HTTP servlet response is used for
     *            writing the response. directly
     * @throws IOException
     *             if there is an I/O error while writing the response.
     */
    @RequestMapping(value = "/1/channels-by-regexp/**", method = RequestMethod.GET)
    @ResponseBody
    public void channelsByRegExp(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        // Due to the way how the client generates the request URI (in
        // particular how escaping is performed), we cannot rely on Spring's
        // path-variable support. Instead, we have to process the request URI
        // directly.
        String regExp = extractExtraPathInfo(request, "/1/channels-by-regexp/");
        if (regExp == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        Pattern pattern;
        try {
            pattern = Pattern.compile(regExp);
        } catch (PatternSyntaxException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid regular expression: " + e.getMessage());
            return;
        }
        channelsByPatternInternal(request, response, pattern);
    }

    /**
     * Retrieves and returns samples for a specific channel from the archive.
     * 
     * @param request
     *            HTTP servlet request. The request is used to extract the
     *            channel name from the request URI and to check whether the
     *            "prettyPrint" option has been specified by the client.
     * @param response
     *            HTTP servlet response. The HTTP servlet response is used for
     *            writing the response. directly
     * @param start
     *            start of the interval for which samples shall be retrieved.
     * @param end
     *            end of the interval for which samples shall be retrieved.
     * @param count
     *            desired number of samples. A decimation level providing a
     *            number of samples that is as close to the specified number of
     *            samples as possible is chosen. If <code>null</code>, only raw
     *            samples are considered.
     * @throws IOException
     *             if there is an I/O error while writing the response.
     */
    @RequestMapping(value = "/1/samples/**", method = RequestMethod.GET)
    @ResponseBody
    public void samples(HttpServletRequest request,
            HttpServletResponse response, @RequestParam long start,
            @RequestParam long end,
            @RequestParam(required = false) Integer count) throws IOException {
        // Due to the way how the client generates the request URI (in
        // particular how escaping is performed), we cannot rely on Spring's
        // path-variable support. Instead, we have to process the request URI
        // directly.
        String channelName = extractExtraPathInfo(request, "/1/samples/");
        if (channelName == null || channelName.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        ChannelInformation channelInformation;
        try {
            channelInformation = channelInformationCache
                    .getChannel(channelName);
        } catch (IllegalStateException e) {
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            return;
        }
        if (channelInformation == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        ControlSystemSupport<?> controlSystemSupport = controlSystemSupportRegistry
                .getControlSystemSupport(channelInformation
                        .getControlSystemType());
        if (controlSystemSupport == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }
        if (start < 0L || end < start) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        if (count != null && count <= 0) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        sendSamples(request, response, channelInformation,
                controlSystemSupport, start, end, count);
    }

    private void channelsByPatternInternal(HttpServletRequest request,
            HttpServletResponse response, Pattern pattern) throws IOException {
        SortedMap<String, ChannelInformation> channels;
        try {
            channels = channelInformationCache.getChannels();
        } catch (IllegalStateException e) {
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "Channel information is currently not available.");
            return;
        }
        Matcher matcher = pattern.matcher("");
        JsonGenerator generator = createJsonGenerator(request, response);
        try {
            generator.writeStartArray();
            for (String channelName : channels.keySet()) {
                matcher.reset(channelName);
                if (matcher.matches()) {
                    generator.writeString(channelName);
                }
            }
            generator.writeEndArray();
        } finally {
            generator.close();
        }
    }

    private Pattern compileGlobPattern(String pattern) {
        // We only support a reduced subset of glob patterns. In particular, we
        // support the question mark (?) matching exactly one character and the
        // asterisk (*) matching zero or more characters.
        StringBuilder regExpBuilder = new StringBuilder();
        while (!pattern.isEmpty()) {
            int firstAsterisk = pattern.indexOf('*');
            int firstQuestionMark = pattern.indexOf('?');
            int firstWildcard;
            if (firstAsterisk == -1) {
                firstWildcard = firstQuestionMark;
            } else if (firstQuestionMark == -1) {
                firstWildcard = firstAsterisk;
            } else {
                firstWildcard = Math.min(firstAsterisk, firstQuestionMark);
            }
            if (firstWildcard == -1) {
                // The pattern does not have any wildcards left, so we can add
                // the rest of the pattern to the regular expression.
                regExpBuilder.append(Pattern.quote(pattern));
                pattern = "";
            } else if (firstAsterisk == 0) {
                regExpBuilder.append(".*");
                pattern = pattern.substring(1);
            } else if (firstQuestionMark == 0) {
                regExpBuilder.append(".");
                pattern = pattern.substring(1);
            } else {
                // We can add the part of the pattern preceding the next
                // wildcard to the regular expression.
                regExpBuilder.append(Pattern.quote(pattern.substring(0,
                        firstWildcard)));
                pattern = pattern.substring(firstWildcard);
            }
        }
        return Pattern.compile(regExpBuilder.toString(),
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
                        | Pattern.UNICODE_CASE);
    }

    private JsonGenerator createJsonGenerator(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        prepareJsonResponse(response);
        JsonGenerator jsonGenerator = jsonFactory.createGenerator(response
                .getOutputStream());
        if (request.getParameter("prettyPrint") != null) {
            jsonGenerator.useDefaultPrettyPrinter();
        }
        return jsonGenerator;
    }

    private String extractExtraPathInfo(HttpServletRequest request,
            String expectedPrefix) {
        expectedPrefix = request.getContextPath()
                + "/archive-access/api/1.0/archive" + expectedPrefix;
        String requestUri = request.getRequestURI();
        if (!requestUri.startsWith(expectedPrefix)) {
            return null;
        } else {
            try {
                return URLDecoder.decode(
                        requestUri.substring(expectedPrefix.length()), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(
                        "Unexpected unsupported encoding exception: The platform does not support the UTF-8 encoding.",
                        e);
            }
        }
    }

    private void prepareJsonResponse(HttpServletResponse response) {
        response.setCharacterEncoding("UTF-8");
        response.setContentType("application/json");
    }

    private <SampleType extends Sample> void sendSamples(
            HttpServletRequest request, HttpServletResponse response,
            ChannelInformation channelInformation,
            ControlSystemSupport<SampleType> controlSystemSupport, long start,
            long end, Integer count) throws IOException {
        Iterator<SampleType> samples;
        if (count == null) {
            // Return raw samples
            samples = FutureUtils.getUnchecked(
                    archiveAccessService.getSamples(channelInformation, 0,
                            start, TimeStampLimitMode.AT_OR_BEFORE, end,
                            TimeStampLimitMode.AT_OR_AFTER,
                            controlSystemSupport)).iterator();
        } else {
            // Return "optimized" samples.
            double perfectDecimationPeriodSecondsAsDouble = (end - start)
                    / ((double) count) / 1000000000.0;
            int perfectDecimationPeriodSeconds = (perfectDecimationPeriodSecondsAsDouble > Integer.MAX_VALUE) ? Integer.MAX_VALUE
                    : ((int) Math.floor(perfectDecimationPeriodSecondsAsDouble));
            // Sorting the available decimation levels is the easiest way of
            // finding the best ones. We need them sorted anyway in case we want
            // to try decimation levels with a greater decimation period later.
            TreeSet<Integer> availableDecimationLevels = new TreeSet<Integer>(
                    channelInformation.getDecimationLevels());
            Integer nextLongerDecimationPeriodSeconds = availableDecimationLevels
                    .ceiling(perfectDecimationPeriodSeconds);
            Integer nextShorterDecimationPeriodSeconds = availableDecimationLevels
                    .floor(perfectDecimationPeriodSeconds);
            // There must always be a shorter (or equal) decimation period
            // because there always is decimation level zero that stores raw
            // samples.
            Integer bestDecimationPeriodSeconds;
            if (nextLongerDecimationPeriodSeconds == null) {
                bestDecimationPeriodSeconds = nextShorterDecimationPeriodSeconds;
            } else if (nextLongerDecimationPeriodSeconds.intValue() == nextShorterDecimationPeriodSeconds
                    .intValue()) {
                // If the decimation period is an exact match, we do not have to
                // choose.
                bestDecimationPeriodSeconds = nextShorterDecimationPeriodSeconds;
            } else {
                // If there is a longer and a shorter one, we have to choose
                // which one matches better. We favor a shorter one because in
                // this case the user gets more samples than requested while
                // choosing the longer one will lead to less samples than
                // expected, which could be a problem. However, if the longer
                // one is only five percent longer and the deviation of the
                // shorter one is greater, we favor the longer one. In this
                // case, the number of samples returned should still be close
                // enough to what the user expects.
                double longerMatch = nextLongerDecimationPeriodSeconds
                        / perfectDecimationPeriodSecondsAsDouble - 1.0;
                double shorterMatch = (nextShorterDecimationPeriodSeconds
                        / perfectDecimationPeriodSecondsAsDouble - 1.0)
                        * -1.0;
                if (longerMatch < 0.05 && longerMatch < shorterMatch) {
                    bestDecimationPeriodSeconds = nextLongerDecimationPeriodSeconds;
                } else {
                    bestDecimationPeriodSeconds = nextShorterDecimationPeriodSeconds;
                }

            }
            // When the best matching decimation level does not cover the
            // requested period, we can try one with a longer decimation period
            // because it might also have a longer retention period.
            Set<Integer> usableDecimationLevels = availableDecimationLevels
                    .tailSet(bestDecimationPeriodSeconds);
            LinkedList<PeekingIterator<SampleType>> usableIterators = new LinkedList<PeekingIterator<SampleType>>();
            // The initial value is not used anyway because we overwrite it when
            // we add the first iterator to usableIterators. However, we get a
            // compiler error if we do not initialize it and Long.MAX_VALUE
            // looks like the most sensible initial value.
            long earliestSampleTimeStamp = Long.MAX_VALUE;
            for (int decimationLevel : usableDecimationLevels) {
                PeekingIterator<SampleType> peekingIterator;
                // If we have not found a decimation level with samples yet, we
                // request samples for the whole range. Otherwise, we only
                // request samples that are before the range that is already
                // covered.
                if (usableIterators.isEmpty()) {
                    peekingIterator = Iterators.peekingIterator(FutureUtils
                            .getUnchecked(
                                    archiveAccessService.getSamples(
                                            channelInformation,
                                            decimationLevel, start,
                                            TimeStampLimitMode.AT_OR_BEFORE,
                                            end,
                                            TimeStampLimitMode.AT_OR_AFTER,
                                            controlSystemSupport)).iterator());
                } else {
                    peekingIterator = Iterators.peekingIterator(FutureUtils
                            .getUnchecked(
                                    archiveAccessService.getSamples(
                                            channelInformation,
                                            decimationLevel, start,
                                            TimeStampLimitMode.AT_OR_BEFORE,
                                            earliestSampleTimeStamp - 1L,
                                            TimeStampLimitMode.AT_OR_BEFORE,
                                            controlSystemSupport)).iterator());
                }
                // If there are no samples available from that decimation level,
                // we cannot use it and try the next one.
                if (!peekingIterator.hasNext()) {
                    continue;
                }
                long firstSampleTimeStamp = peekingIterator.peek()
                        .getTimeStamp();
                // If this is the first usable decimation level that actually
                // has a sample, we always want to add it to the list of usable
                // iterators. We also want to add it to the list if its first
                // sample is before the earliest sample we found so far.
                if (usableIterators.isEmpty()
                        || firstSampleTimeStamp < earliestSampleTimeStamp) {
                    // We add the iterator to the beginning of the list because
                    // it contains samples that are before the samples from the
                    // iterators that already are in the list.
                    usableIterators.addFirst(peekingIterator);
                    earliestSampleTimeStamp = firstSampleTimeStamp;
                }
                // If this decimation level covers the whole requested range, we
                // do not have to look for a decimation level with a longer
                // retention period.
                if (firstSampleTimeStamp <= start) {
                    break;
                }
            }
            // If there is just one iterator (the most common case), we can
            // simply use it. If there is no iterator, we use an empty iterator.
            // If there is more than one iterator, we concatenate them in
            // reverse order.
            if (usableIterators.size() == 1) {
                samples = usableIterators.element();
            } else if (usableIterators.size() == 0) {
                samples = Iterators.emptyIterator();
            } else {
                samples = Iterators
                        .concat(usableIterators.descendingIterator());
            }
        }
        JsonGenerator generator = createJsonGenerator(request, response);
        try {
            generator.writeStartArray();
            while (samples.hasNext()) {
                SampleType sample = samples.next();
                generator.writeStartObject();
                controlSystemSupport.serializeSampleToJsonV1(sample, generator);
                generator.writeEndObject();
            }
            generator.writeEndArray();
        } finally {
            generator.close();
        }
    }
}
