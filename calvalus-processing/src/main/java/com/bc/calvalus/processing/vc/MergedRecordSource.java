/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.vc;

import com.bc.calvalus.processing.ma.DefaultHeader;
import com.bc.calvalus.processing.ma.DefaultRecord;
import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.ceres.core.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bc.calvalus.processing.ma.PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX;

/**
 * A record source
 */
public class MergedRecordSource implements RecordSource {

    private final Header header;
    private final List<NamedRecordSource> namedRecords;
    private final NamedRecordSource referenceRecords;
    private final NamedRecordSource baseL2Records;

    public MergedRecordSource(NamedRecordSource referenceRecords, NamedRecordSource baseL2Records, List<NamedRecordSource> namedRecords) {
        Assert.notNull(referenceRecords, "referenceRecords != null");
        Assert.notNull(baseL2Records, "baseL2Records != null");
        Assert.notNull(namedRecords, "namedRecords != null");
        Assert.argument(namedRecords.size() > 0, "namedRecords.length > 0");

        this.referenceRecords = referenceRecords;
        this.baseL2Records = baseL2Records;
        this.namedRecords = namedRecords;

        List<NamedRecordSource> headerSources = new ArrayList<NamedRecordSource>(namedRecords.size() + 1);
        headerSources.addAll(namedRecords);
        headerSources.add(baseL2Records);
        this.header = new DefaultHeader(false, false, getAttributeNames(headerSources), baseL2Records.getHeader().getAnnotationNames());
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public Iterable<Record> getRecords() {
        Map<Integer, List<Record>> recordsMap = new HashMap<Integer, List<Record>>(referenceRecords.getNumRecords());
        for (Record referenceRecordsRecord : referenceRecords.getRecords()) {
            recordsMap.put(referenceRecordsRecord.getId(), new ArrayList<Record>(namedRecords.size() + 1));
        }
        for (NamedRecordSource namedRecordSource : namedRecords) {
            for (Record record : namedRecordSource.getRecords()) {
                recordsMap.get(record.getId()).add(record);
            }
        }

        Map<Integer, Record> baseL2Map = new HashMap<Integer, Record>(referenceRecords.getNumRecords());
        for (Record record : baseL2Records.getRecords()) {
            baseL2Map.put(record.getId(), record);
        }
        int numAttributes = header.getAttributeNames().length;
        List<Record> mergedRecords = new ArrayList<Record>(referenceRecords.getNumRecords());

        for (Record referenceRecordsRecord : referenceRecords.getRecords()) {
            int id = referenceRecordsRecord.getId();

            Object[] attributeValues = new Object[numAttributes];
            int attributeDestPos = 0;
            List<Record> recordList = recordsMap.get(id);
            for (Record record : recordList) {
                Object[] srcAttributes = record.getAttributeValues();
                System.arraycopy(srcAttributes, 0, attributeValues, attributeDestPos, srcAttributes.length);
                attributeDestPos += srcAttributes.length;
            }
            Record l2Record = baseL2Map.get(id);
            Object[] srcAttributes = l2Record.getAttributeValues();
            System.arraycopy(srcAttributes, 0, attributeValues, attributeDestPos, srcAttributes.length);

            mergedRecords.add(new DefaultRecord(referenceRecordsRecord.getId(),
                                                referenceRecordsRecord.getLocation(),
                                                referenceRecordsRecord.getTime(),
                                                attributeValues,
                                                l2Record.getAnnotationValues()));
        }
        return mergedRecords;
    }

    @Override
    public String getTimeAndLocationColumnDescription() {
        return null;
    }

    private static String[] getAttributeNames(List<NamedRecordSource> namedRecordSources) {
        List<String> attributeNames = new ArrayList<String>();
        for (NamedRecordSource namedRecordSource : namedRecordSources) {
            String name = namedRecordSource.getName();
            for (String attributeName : namedRecordSource.getHeader().getAttributeNames()) {
                String newAttributeName;
                if (attributeName.startsWith(ATTRIB_NAME_AGGREG_PREFIX)) {
                    newAttributeName = ATTRIB_NAME_AGGREG_PREFIX + name + attributeName.substring(ATTRIB_NAME_AGGREG_PREFIX.length());
                } else {
                    newAttributeName = name + attributeName;
                }
                attributeNames.add(newAttributeName);
            }
        }
        return attributeNames.toArray(new String[attributeNames.size()]);
    }
}
