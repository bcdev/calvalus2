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

package com.bc.calvalus.processing.ma.compare;

import com.bc.calvalus.processing.ma.MAMapper;
import com.bc.calvalus.processing.ma.RecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.bc.calvalus.processing.ma.PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX;

/**
 * @author MarcoZ
 */
public class MACompareMapper extends Mapper<Text, RecordWritable, MAKey, IndexedRecordWritable> {

    private String identifier;
    private MAKey maKey;
    private int identifierIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path path = fileSplit.getPath();
        identifier = path.getParent().getName();
        Configuration conf = context.getConfiguration();
        List<String> identifiers = Arrays.asList(conf.getStrings("calvalus.ma.identifiers"));
        identifierIndex = identifiers.indexOf(identifier);
        maKey = new MAKey();
        maKey.setIdentifierOrder(identifierIndex);
    }

    @Override
    protected void map(Text key, RecordWritable record, Context context) throws IOException, InterruptedException {
        if (key.equals(MAMapper.HEADER_KEY)) {
            Object[] attributeValues = record.getAttributeValues();
            Object[] newAttributeNames = new Object[attributeValues.length];
            for (int i = 0; i < attributeValues.length; i++) {
                String attributeName = (String) attributeValues[i];
                if (attributeName.startsWith(ATTRIB_NAME_AGGREG_PREFIX)) {
                    newAttributeNames[i] = ATTRIB_NAME_AGGREG_PREFIX + identifier + "_" + attributeName.substring(ATTRIB_NAME_AGGREG_PREFIX.length());
                } else {
                    newAttributeNames[i] = identifier + "_" + attributeName;
                }
            }
            IndexedRecordWritable indexedRecord = new IndexedRecordWritable(identifierIndex,
                                                                            newAttributeNames,
                                                                            record.getAnnotationValues());

            maKey.setReferenceId(MAKey.HEADER_KEY);
            maKey.setProductName("");

            context.write(maKey, indexedRecord);
        } else {
            String keyAsString = key.toString();
            String[] split = keyAsString.split("_", 2);
            maKey.setReferenceId(Integer.parseInt(split[0]));
            maKey.setProductName(split[1]);

            IndexedRecordWritable indexedRecord = new IndexedRecordWritable(identifierIndex,
                                                                            record.getAttributeValues(),
                                                                            record.getAnnotationValues());
            context.write(maKey, indexedRecord);
        }
    }
}
