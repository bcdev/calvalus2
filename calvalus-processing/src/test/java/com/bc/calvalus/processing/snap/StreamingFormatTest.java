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


import com.bc.calvalus.processing.UnixTestRunner;
import com.bc.calvalus.processing.hadoop.ByteArrayWritable;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.math.MathUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author MarcoZ
 */
@RunWith(UnixTestRunner.class)
public class StreamingFormatTest {

    private static final int TILE_HEIGHT = 64;
    private Configuration configuration;
    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.getLocal(configuration);
    }

    @Test
    public void testWriteReadCycle() throws Exception {

        File testProductFile = MerisProductTestRunner.getTestProductFile();

        System.setProperty("snap.dataio.reader.tileHeight", "64");
        System.setProperty("snap.dataio.reader.tileWidth", "*");
        ProductReader productReader = ProductIO.getProductReader("ENVISAT");
        Product sourceProduct = productReader.readProductNodes(testProductFile, null);

        Path outputDir = new Path("target/testdata/StreamingProductWriterTest");
        Path productPath = new Path(outputDir, "testWrite.seq");
        try {
            assertFalse(fileSystem.exists(productPath));
            ProgressMonitor pm = ProgressMonitor.NULL;
            StreamingProductWriter.writeProductInSlices(configuration, pm, sourceProduct, productPath);
            assertTrue(fileSystem.exists(productPath));

            testThatProductSequenceFileIsCorrect(productPath, getNumKeys(sourceProduct));

            testThatProductIsCorrect(sourceProduct, productPath);

            testThatIndicesAreTheSame(productPath);

            // check that this all works, even when the index is removed
            // (it will be rebuild)
            Path indexPath = StreamingProductIndex.getIndexPath(productPath);
            fileSystem.delete(indexPath, false);
            assertFalse(fileSystem.exists(indexPath));

            testThatProductIsCorrect(sourceProduct, productPath);
            assertTrue(fileSystem.exists(indexPath));

            testThatIndicesAreTheSame(productPath);
        } finally {
            fileSystem.delete(outputDir, true);
        }
    }

    private void testThatIndicesAreTheSame(Path productPath) throws IOException {
        Path indexPath = StreamingProductIndex.getIndexPath(productPath);
        StreamingProductIndex streamingProductIndex = new StreamingProductIndex(indexPath, configuration);
        Map<String, Long> writtenIndex = streamingProductIndex.readIndex();

        Map<String, Long> buildIndex;
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, productPath, configuration)) {
            buildIndex = StreamingProductIndex.buildIndex(reader);
        }
        assertNotNull(buildIndex);
        assertEquals(buildIndex.size(), writtenIndex.size());

        Set<String> buildKeys = buildIndex.keySet();
        for (String buildKey : buildKeys) {
            assertTrue(writtenIndex.containsKey(buildKey));
            assertEquals(buildIndex.get(buildKey), writtenIndex.get(buildKey));
        }
    }

    private void testThatProductIsCorrect(Product sourceProduct, Path productPath) throws IOException {
        Product targetProduct = CalvalusProductIO.readProduct(productPath, configuration, StreamingProductPlugin.FORMAT_NAME);
        try {
            assertEquals(sourceProduct.getName(), targetProduct.getName());
            assertEquals(sourceProduct.getNumBands(), targetProduct.getNumBands());

            // check that tie-point data is equal
            ProductData.Float srcTP0 = (ProductData.Float) sourceProduct.getTiePointGridAt(0).getData();
            ProductData.Float targetTP0 = (ProductData.Float) targetProduct.getTiePointGridAt(0).getData();
            assertArrayEquals(srcTP0.getArray(), targetTP0.getArray(), 1e-5f);

            // check that band data is equal
            Raster srcData = sourceProduct.getBandAt(0).getSourceImage().getData();
            Raster targetData = targetProduct.getBandAt(0).getSourceImage().getData();
            for (int y = 0; y < sourceProduct.getSceneRasterHeight(); y++) {
                for (int x = 0; x < sourceProduct.getSceneRasterWidth(); x++) {
                    float srcSample = srcData.getSampleFloat(x, y, 0);
                    float targetSample = targetData.getSampleFloat(x, y, 0);
                    assertEquals("[x" + "," + y + "]", srcSample, targetSample, 1e-5f);
                }
            }
        } finally {
            if (targetProduct != null) {
                targetProduct.dispose();
            }
        }
    }

    private void testThatProductSequenceFileIsCorrect(Path productPath, int numKeys) throws IOException {
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, productPath, configuration)) {
            assertSame(Text.class, reader.getKeyClass());
            assertSame(ByteArrayWritable.class, reader.getValueClass());

            SequenceFile.Metadata metadata = reader.getMetadata();
            TreeMap<Text, Text> metadatMap = metadata.getMetadata();
            assertEquals(2, metadatMap.size());

            assertTrue(metadatMap.containsKey(new Text("dim")));
            assertTrue(metadatMap.containsKey(new Text("slice.height")));

            Text sliceHeigthText = metadatMap.get(new Text("slice.height"));
            assertEquals(Integer.toString(TILE_HEIGHT), sliceHeigthText.toString());

            List<String> keyList = new ArrayList<String>();
            Text key = new Text();
            while (reader.next(key)) {
                keyList.add(key.toString());
            }

            assertEquals(numKeys, keyList.size());
        }
    }

    private int getNumKeys(Product sourceProduct) {
        int numSlices = MathUtils.ceilInt(sourceProduct.getSceneRasterHeight() / 64f);
        int numBandKeys = sourceProduct.getNumBands() * numSlices;
        int numTiepointKeys = sourceProduct.getNumTiePointGrids();
        return numBandKeys + numTiepointKeys;
    }

}
