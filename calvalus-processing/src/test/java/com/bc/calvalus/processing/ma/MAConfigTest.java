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


import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The configuration for the match-up processing.
 *
 * @author MarcoZ
 * @author Norman
 */
public class MAConfigTest {

    @Test
    public void testGetRecordSource() throws Exception {
        MAConfig maConfig = new MAConfig(TestRecordSourceSpi.class.getName(), "");
        RecordSource recordSource = maConfig.createRecordSource();
        assertNotNull(recordSource);
        assertNotNull(recordSource.getRecords());
        Iterator<Record> recordIterator = recordSource.getRecords().iterator();
        assertTrue(recordIterator.hasNext());
        Record record = recordIterator.next();
        assertNotNull(record);
    }

    public static class TestRecordSourceSpi extends RecordSourceSpi {
        @Override
        public RecordSource createRecordSource(String url) {
            DefaultHeader header = new DefaultHeader("lat", "lon");
            DefaultRecordSource recordSource = new DefaultRecordSource(header);
            ExtractorTest.addPointRecord(recordSource, 0F, 0F);
            return recordSource;
        }
    }

}
