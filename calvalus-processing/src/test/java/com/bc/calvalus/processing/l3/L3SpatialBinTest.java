/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.l3;

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static org.junit.Assert.assertEquals;

public class L3SpatialBinTest {

    @Test
    public void testMetadataExchange() throws IOException {
        StringBuffer accu = new StringBuffer(65535);
        for (int i=0; i<65535; ++i) {
            accu.append('m');
        }
        String metadata = accu.toString();
        L3SpatialBin l3SpatialBin = new L3SpatialBin(metadata);
        PipedInputStream input = new PipedInputStream(65536*3);
        PipedOutputStream output = new PipedOutputStream(input);
        l3SpatialBin.write(new DataOutputStream(output));
        L3SpatialBin l3SpatialBin1 = (L3SpatialBin) L3SpatialBin.read(new DataInputStream(input));

        assertEquals(metadata.length(), l3SpatialBin1.getMetadata().length());
        assertEquals(metadata, l3SpatialBin1.getMetadata());
    }
}
