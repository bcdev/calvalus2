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

package com.bc.calvalus.processing.beam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The index for the key/value base streaming product format.
 *
 * @author MarcoZ
 */
public class StreamingProductIndex {

    private static final Progressable NULL_PROGRESSABLE = new Progressable() {
        @Override
        public void progress() {
            //do nothing
        }
    };
    private static final String NUM_ENTRIES = "numEntries";

    private Path indexPath;
    private Configuration configuration;

    public StreamingProductIndex(Path indexPath, Configuration configuration) throws IOException {
        this.indexPath = indexPath;
        this.configuration = configuration;
    }

    public static Path getIndexPath(Path productPath) {
        return new Path(productPath.getParent(), productPath.getName() + ".index");
    }

    public void writeIndex(Map<String, Long> indexMap) throws IOException {
        SequenceFile.Writer indexWriter = createWriter(indexMap.size());
        try {
            Set<Map.Entry<String, Long>> entries = indexMap.entrySet();
            Text key = new Text();
            LongWritable value = new LongWritable();
            for (Map.Entry<String, Long> entry : entries) {
                key.set(entry.getKey());
                value.set(entry.getValue());
                indexWriter.append(key, value);
            }
        } finally {
            indexWriter.close();
        }
    }

    private SequenceFile.Writer createWriter(int numEntries) throws IOException {
        SequenceFile.Metadata metadata = new SequenceFile.Metadata();
        metadata.set(new Text(NUM_ENTRIES), new Text(Integer.toString(numEntries)));
        return SequenceFile.createWriter(FileSystem.get(configuration),
                                         configuration,
                                         indexPath,
                                         Text.class,
                                         LongWritable.class,
                                         SequenceFile.CompressionType.RECORD,
                                         new DefaultCodec(),
                                         NULL_PROGRESSABLE,
                                         metadata);
    }

    public Map<String, Long> getIndex(SequenceFile.Reader reader) throws IOException {
        Map<String, Long> indexMap = readIndex();
        if (indexMap.isEmpty()) {
            indexMap = buildIndex(reader);
//            for now, don't write index while reading
//            writeIndex(indexMap);
        }
        return indexMap;
    }

    public static Map<String, Long> buildIndex(SequenceFile.Reader reader) throws IOException {
        Map<String, Long> indexMap = new HashMap<String, Long>();
        Text key = new Text();
        long currentPos = reader.getPosition();
        while (reader.next(key)) {
            indexMap.put(key.toString(), currentPos);
            currentPos = reader.getPosition();
        }
        return indexMap;
    }

    private Map<String, Long> readIndex() throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(indexPath)) {
            SequenceFile.Reader indexReader = new SequenceFile.Reader(fs, indexPath, configuration);
            try {
                Text numEntriesText = indexReader.getMetadata().get(new Text(NUM_ENTRIES));
                if (numEntriesText != null) {
                    int numEntries = Integer.parseInt(numEntriesText.toString());

                    Map<String, Long> indexMap = new HashMap<String, Long>(numEntries);
                    Text key = new Text();
                    LongWritable value = new LongWritable();
                    while (indexReader.next(key, value)) {
                        indexMap.put(key.toString(), value.get());
                    }
                    if (numEntries == indexMap.size()) {
                        return indexMap;
                    }
                }
            } finally {
                indexReader.close();
            }
        }
        return Collections.emptyMap();
    }
}
