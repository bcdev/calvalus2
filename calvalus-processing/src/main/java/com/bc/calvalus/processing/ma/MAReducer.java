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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Reads the records emitted by the MAMapper.
 * It is expected that each true 'record' key will only have one unique value.
 * Only 'header' keys ("#") will have multiple values containing all the same the attribute names.
 * This is why the reducer only writes the first value.
 *
 * @author Norman Fomferra
 */
public class MAReducer extends Reducer<Text, RecordWritable, Text, RecordWritable> {

    @Override
    protected void reduce(Text key, Iterable<RecordWritable> values, Context context) throws IOException, InterruptedException {

        HashMap<String, X> map = new HashMap<String, X>();

        Iterator<RecordWritable> iterator = values.iterator();
        if (iterator.hasNext()) {
            RecordWritable record = iterator.next();
            context.write(key, record);
        }
    }

    public static class X {
        private final String siteName;
        private final String variableName;

        private final List<Number> referenceValues;
        private final List<Number> meanValues;
        private final List<Number> stdDevValues;
        private final List<Number> nValues;

        public X(String siteName, String variableName) {
            this.siteName = siteName;
            this.variableName = variableName;
            referenceValues = new ArrayList<Number>(32);
            meanValues = new ArrayList<Number>(32);
            stdDevValues = new ArrayList<Number>(32);
            nValues = new ArrayList<Number>(32);
        }


    }
}
