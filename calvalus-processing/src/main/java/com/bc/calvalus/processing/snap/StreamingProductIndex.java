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

package com.bc.calvalus.processing.snap;

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
        FileSystem fs = indexPath.getFileSystem(configuration);
        return SequenceFile.createWriter(fs,
                                         configuration,
                                         indexPath,
                                         Text.class,
                                         LongWritable.class,
                                         SequenceFile.CompressionType.RECORD,
                                         new DefaultCodec(),
                                         NULL_PROGRESSABLE,
                                         metadata);
    }

    public static Map<String, Long> buildIndex(SequenceFile.Reader productStreamReader) throws IOException {
        Map<String, Long> indexMap = new HashMap<String, Long>();
        Text key = new Text();
        long currentPos = productStreamReader.getPosition();
        while (productStreamReader.next(key)) {
            indexMap.put(key.toString(), currentPos);
            currentPos = productStreamReader.getPosition();
        }
        return indexMap;
    }

    public Map<String, Long> readIndex() throws IOException {
        FileSystem fs = indexPath.getFileSystem(configuration);
        if (fs.exists(indexPath)) {
            SequenceFile.Reader indexReader = null;
            try {
                indexReader = new SequenceFile.Reader(fs, indexPath, configuration);
                SequenceFile.Metadata metadata = indexReader.getMetadata();
                Text numEntriesText = metadata.get(new Text(NUM_ENTRIES));
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
            } catch (IOException ioe) {
                // in case of an IO error,
                // ignore the read index and return an empty map
            } finally {
                if (indexReader != null) {
                    indexReader.close();
                }
            }
        }
        return Collections.emptyMap();
    }
}
