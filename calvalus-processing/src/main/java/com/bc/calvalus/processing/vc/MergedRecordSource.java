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
import java.util.Iterator;
import java.util.List;

import static com.bc.calvalus.processing.ma.PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX;

/**
 * A record source
 */
public class MergedRecordSource implements RecordSource {

    private final Header header;
    private final List<NamedRecordSource> namedRecordSources;
    private final NamedRecordSource baseL2RecordSource;

    public MergedRecordSource(NamedRecordSource baseL2RecordSource, List<NamedRecordSource> namedRecordSources) {
        this.baseL2RecordSource = baseL2RecordSource;
        Assert.notNull(namedRecordSources, "namedRecordSources != null");
        Assert.argument(namedRecordSources.size() > 0, "namedRecordSources.length > 0");
        this.namedRecordSources = namedRecordSources;
        String[] attributeNames = getAttributeNames(namedRecordSources);
        header = new DefaultHeader(false, false, attributeNames, baseL2RecordSource.getHeader().getAnnotationNames());
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public Iterable<Record> getRecords() {
        return new MergedIterable();
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

    private class MergedIterable implements Iterable<Record> {
        @Override
        public Iterator<Record> iterator() {
            return new MergedIterator(baseL2RecordSource,
                                      namedRecordSources,
                                      header.getAttributeNames().length
            );
        }
    }

    private static class MergedIterator implements Iterator<Record> {

        private final Iterator<Record> baseL2Source;
        private final List<Iterator<Record>> sources;
        private final int numAttributes;

        private MergedIterator(NamedRecordSource baseL2Source, List<NamedRecordSource> namedRecordSources, int numAttributes) {
            this.baseL2Source = baseL2Source.getRecords().iterator();
            this.numAttributes = numAttributes;
            sources = new ArrayList<Iterator<Record>>(namedRecordSources.size());
            for (NamedRecordSource namedRecordSource : namedRecordSources) {
                sources.add(namedRecordSource.getRecords().iterator());
            }
        }

        @Override
        public boolean hasNext() {
            return sources.get(0).hasNext();
        }

        @Override
        public Record next() {
            Object[] attributeValues = new Object[numAttributes];
            int attributeDestPos = 0;
            for (Iterator<Record> source : sources) {
                Record record = source.next();

                Object[] srcAttributes = record.getAttributeValues();
                System.arraycopy(srcAttributes, 0, attributeValues, attributeDestPos, srcAttributes.length);
                attributeDestPos += srcAttributes.length;
            }
            Record record = baseL2Source.next();
            return new DefaultRecord(null, null, attributeValues, record.getAnnotationValues());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
