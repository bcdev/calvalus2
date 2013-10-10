/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible for reading and writing the processing metadata.
 */
public class ProcessingMetadata {

    private static final String FILE_NAME = "_processing_metadata";

    public static void write(Path outputDirectory, Configuration conf, Map<String, String> metadata) throws IOException {
        Path metadataPath = new Path(outputDirectory, FILE_NAME);
        FileSystem fs = metadataPath.getFileSystem(conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, metadataPath, Text.class, Text.class);
        try {
            Text key = new Text();
            Text value = new Text();
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                key.set(entry.getKey());
                value.set(entry.getValue());
                writer.append(key, value);
            }
        } finally {
            writer.close();
        }
    }

    public static Map<String, String> read(Path inputDirectory, Configuration conf) throws IOException {
        Path metadataPath = new Path(inputDirectory, FILE_NAME);
        FileSystem fs = metadataPath.getFileSystem(conf);
        if (fs.exists(metadataPath)) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, metadataPath, conf);
            Map<String, String> metadata = new HashMap<String, String>();
            Text key = new Text();
            Text value = new Text();
            try {
                while (reader.next(key, value)) {
                    metadata.put(key.toString(), value.toString());
                }
            } finally {
                reader.close();
            }
            return metadata;
        } else {
            return Collections.emptyMap();
        }
    }

    public static void metadata2Config(Map<String, String> metadata, Configuration conf, String[] keys) throws IOException {
        for (String key : keys) {
            String value = metadata.get(key);
            if (value != null) {
                conf.set(key, value);
            }
        }
    }

    public static Map<String, String> config2metadata(Configuration conf, String[] keys) throws IOException {
        Map<String, String> metadata = new HashMap<String, String>();
        for (String key : keys) {
            String value = conf.get(key);
            if (value != null) {
                metadata.put(key, value);
            }
        }
        return metadata;
    }
}
