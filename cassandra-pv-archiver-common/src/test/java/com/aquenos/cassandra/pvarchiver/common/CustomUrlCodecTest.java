/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.common;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import org.junit.Test;

import com.aquenos.cassandra.pvarchiver.common.CustomUrlCodec;

/**
 * Tests for the {@link CustomUrlCodec}.
 * 
 * @author Sebastian Marsching
 */
public class CustomUrlCodecTest {

    /**
     * Tests the {@link CustomUrlCodec#decode(String)} and
     * {@link CustomUrlCodec#encode(String)} methods.
     */
    @Test
    public void testDefaultDecodeEncode() {
        String unencodedString = "äöü_-abc./~&=;!$()*,-:_";
        String expectedEncodedString = "~C3~A4~C3~B6~C3~BC_-abc~2E~2F~7E~26~3D~3B~21~24~28~29~2A~2C-~3A_";
        String encodedString = CustomUrlCodec.encode(unencodedString);
        assertEquals(expectedEncodedString, encodedString);
        assertEquals(unencodedString, CustomUrlCodec.decode(encodedString));
        // As we have no native upper characters and it should not matter for
        // the code sequences, converting to a lower-case string should still
        // yield the same result after decoding.
        assertEquals(unencodedString,
                CustomUrlCodec.decode(encodedString.toLowerCase()));
        // An encoded string that contains a invalid code sequence should simply
        // have this code sequence not touched when being decoded.
        String illegalEncodedString = "~._~1g~a";
        assertEquals(illegalEncodedString,
                CustomUrlCodec.decode(illegalEncodedString));
    }

    /**
     * Tests the {@link CustomUrlCodec#decode(String, String)} and
     * {@link CustomUrlCodec#encode(String, String)} methods.
     */
    @Test
    public void testEncodingDecodeEncode() {
        String encoding = "UTF-8";
        String unencodedString = "äöü_-abc./~&=;!$()*,-:_";
        String expectedEncodedString = "~C3~A4~C3~B6~C3~BC_-abc~2E~2F~7E~26~3D~3B~21~24~28~29~2A~2C-~3A_";
        String encodedString = CustomUrlCodec.encode(unencodedString, encoding);
        assertEquals(expectedEncodedString, encodedString);
        assertEquals(unencodedString,
                CustomUrlCodec.decode(encodedString, encoding));
        // As we have no native upper characters and it should not matter for
        // the code sequences, converting to a lower-case string should still
        // yield the same result after decoding.
        assertEquals(unencodedString,
                CustomUrlCodec.decode(encodedString.toLowerCase(), encoding));
        // An encoded string that contains a invalid code sequence should simply
        // have this code sequence not touched when being decoded.
        String illegalEncodedString = "~._~1g~a";
        assertEquals(illegalEncodedString,
                CustomUrlCodec.decode(illegalEncodedString, encoding));
    }

    /**
     * Tests the {@link CustomUrlCodec#decode(String, Charset)} and
     * {@link CustomUrlCodec#encode(String, Charset)} methods.
     */
    @Test
    public void testCharsetDecodeEncode() {
        Charset charset = Charset.forName("UTF-8");
        String unencodedString = "äöü_-abc./~&=;!$()*,-:_";
        String expectedEncodedString = "~C3~A4~C3~B6~C3~BC_-abc~2E~2F~7E~26~3D~3B~21~24~28~29~2A~2C-~3A_";
        String encodedString = CustomUrlCodec.encode(unencodedString, charset);
        assertEquals(expectedEncodedString, encodedString);
        assertEquals(unencodedString,
                CustomUrlCodec.decode(encodedString, charset));
        // As we have no native upper characters and it should not matter for
        // the code sequences, converting to a lower-case string should still
        // yield the same result after decoding.
        assertEquals(unencodedString,
                CustomUrlCodec.decode(encodedString.toLowerCase(), charset));
        // An encoded string that contains a invalid code sequence should simply
        // have this code sequence not touched when being decoded.
        String illegalEncodedString = "~._~1g~a";
        assertEquals(illegalEncodedString,
                CustomUrlCodec.decode(illegalEncodedString, charset));
    }

}
