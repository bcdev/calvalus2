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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.bc.calvalus.processing.ma.PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX;

/**
 * A record source
 */
public class MergedRecordSource implements RecordSource {

    private final Header header;
    private final List<NamedRecordSource> namedRecordSources;

    public MergedRecordSource(NamedRecordSource... namedRecordSources) {
        this(Arrays.asList(namedRecordSources));
    }

    public MergedRecordSource(List<NamedRecordSource> namedRecordSources) {
        Assert.notNull(namedRecordSources, "namedRecordSources != null");
        Assert.argument(namedRecordSources.size() > 0, "namedRecordSources.length > 0");
        this.namedRecordSources = namedRecordSources;
        String[] attributeNames = getAttributeNames(namedRecordSources);
        String[] annotationNames = getAnnotationNames(namedRecordSources);
        header = new DefaultHeader(false, false, attributeNames, annotationNames);

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

    private static String[] getAnnotationNames(List<NamedRecordSource> namedRecordSources) {
        List<String> annotationNames = new ArrayList<String>();
        for (NamedRecordSource namedRecordSource : namedRecordSources) {
            String name = namedRecordSource.getName();
            for (String aName : namedRecordSource.getHeader().getAnnotationNames()) {
                annotationNames.add(name + aName);
            }
        }
        return annotationNames.toArray(new String[annotationNames.size()]);
    }


    private class MergedIterable implements Iterable<Record> {
        @Override
        public Iterator<Record> iterator() {
            return new MergedIterator(namedRecordSources,
                                      header.getAttributeNames().length,
                                      header.getAnnotationNames().length);
        }
    }

    private static class MergedIterator implements Iterator<Record> {

        private final List<Iterator<Record>> sources;
        private final int numAttributes;
        private final int numAnnotations;

        private MergedIterator(List<NamedRecordSource> namedRecordSources, int numAttributes, int numAnnotations) {
            this.numAttributes = numAttributes;
            this.numAnnotations = numAnnotations;
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
            Object[] annotationValues = new Object[numAnnotations];
            int attributeDestPos = 0;
            int annotationDestPos = 0;
            for (Iterator<Record> source : sources) {
                Record record = source.next();

                Object[] src = record.getAttributeValues();
                System.arraycopy(src, 0, attributeValues, attributeDestPos, src.length);
                attributeDestPos += src.length;

                src = record.getAnnotationValues();
                System.arraycopy(src, 0, annotationValues, annotationDestPos, src.length);
                annotationDestPos += src.length;
            }
            return new DefaultRecord(null, null, attributeValues, annotationValues);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
