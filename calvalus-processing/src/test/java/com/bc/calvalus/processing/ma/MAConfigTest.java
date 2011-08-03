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

package com.bc.calvalus.processing.ma;


import org.esa.beam.framework.datamodel.GeoPos;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class MAConfigTest {

    @Test
    public void testGetRecordSource() {
        MAConfig maConfig = new MAConfig(TestRecordSourceSpi.class.getName());
        RecordSource recordSource = maConfig.createRecordSource();
        assertNotNull(recordSource);
        assertNotNull(recordSource.getRecords());
        Iterator<Record> recordIterator = recordSource.getRecords().iterator();
        assertTrue(recordIterator.hasNext());
        Record record = recordIterator.next();
        assertNotNull(record);
    }

    public static class TestRecordSourceSpi implements RecordSourceSpi {
        @Override
        public RecordSource createRecordSource(MAConfig maConfig) {
            return new TestRecordSource();
        }
    }

    public static class TestRecordSource implements RecordSource {

        @Override
        public Iterable<Record> getRecords() {
            Record record = new Record() {
                @Override
                public Object getId() {
                    return "A";
                }

                @Override
                public GeoPos getGeoPos() {
                    return new GeoPos();
                }
            };
            return Arrays.asList(record);
        }
    }
}
