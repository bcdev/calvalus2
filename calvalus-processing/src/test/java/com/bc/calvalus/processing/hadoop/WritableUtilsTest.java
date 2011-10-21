/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.hadoop;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.Assert.*;

public class WritableUtilsTest {

    @Test(expected = IllegalArgumentException.class)
    public void testConvertByteToFloat_wrongSize() throws Exception {
        byte[] byteArray = new byte[7];
        float[] floatArray = new float[2];
        WritableUtils.convertByteToFloat(byteArray, floatArray);
    }

    @Test
    public void testConvertByteToFloat() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        dataOutputStream.writeFloat(0.12f);
        dataOutputStream.writeFloat(0.42f);
        dataOutputStream.writeFloat(5.67f);
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        float[] floatArray = new float[3];
        WritableUtils.convertByteToFloat(byteArray, floatArray);
        assertEquals(0.12f, floatArray[0], 0.000001f);
        assertEquals(0.42f, floatArray[1], 0.000001f);
        assertEquals(5.67f, floatArray[2], 0.000001f);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertFloatToByte_wrongSize() throws Exception {
        float[] floatArray = new float[]{0.12f, 0.42f, 5.67f};
        byte[] byteArray = new byte[3];
        WritableUtils.convertFloatToByte(floatArray, byteArray);
    }

    @Test
    public void testConvertFloatToByte() throws Exception {
        float[] floatArray = new float[]{0.12f, 0.42f, 5.67f};
        byte[] byteArray = new byte[3 * 4];
        WritableUtils.convertFloatToByte(floatArray, byteArray);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(byteArray));
        assertEquals(0.12f, in.readFloat(), 0.000001f);
        assertEquals(0.42f, in.readFloat(), 0.000001f);
        assertEquals(5.67f, in.readFloat(), 0.000001f);
    }

}
