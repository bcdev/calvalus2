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

package com.bc.calvalus.processing.snap;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.ByteArrayWritable;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.esa.snap.core.dataio.*;
import org.esa.snap.core.dataio.dimap.DimapHeaderWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.util.ImageUtils;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.*;
import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class StreamingProductWriter extends AbstractProductWriter {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private final Map<String, Long> indexMap;
    private Path path;
    private Configuration configuration;
    private final Progressable progressable = null; // TODO no longer needed ??, progress through pm
    private SequenceFile.Writer sequenceFileWriter;
    private int tileHeight;


    public StreamingProductWriter(ProductWriterPlugIn productWriterPlugIn) {
        super(productWriterPlugIn);
        indexMap = new HashMap<>();
    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        final Product product = getSourceProduct();
        Object output = getOutput();
        if (output instanceof PathConfiguration) {
            PathConfiguration pathConfiguration = (PathConfiguration) output;
            path = pathConfiguration.getPath();
            configuration = pathConfiguration.getConfiguration();
        } else {
            throw new IllegalFileFormatException("input is not of the correct type.");
        }
        tileHeight = product.getPreferredTileSize().height;
        sequenceFileWriter = writeHeader(product, path);
        writeTiePointData(product, sequenceFileWriter, indexMap);
        LOG.info(" written header");
    }

    @Override
    public void writeBandRasterData(Band band, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData productData, ProgressMonitor pm) throws IOException {
        int sliceIndex = sourceOffsetY / tileHeight;
        String key = band.getName() + ":" + sliceIndex;
        updateIndex(indexMap, key, sequenceFileWriter.getLength());
        writeProductData(sequenceFileWriter, key, productData);
    }

    @Override
    public void flush() throws IOException {
        //noop
    }

    @Override
    public void close() throws IOException {
        sequenceFileWriter.close();

        Path indexPath = StreamingProductIndex.getIndexPath(path);
        StreamingProductIndex streamingProductIndex = new StreamingProductIndex(indexPath, configuration);
        streamingProductIndex.writeIndex(indexMap);
    }

    @Override
    public void deleteOutput() throws IOException {

    }

    public static void writeProductInSlices(Configuration configuration,
                                            ProgressMonitor pm,
                                            Product product,
                                            Path path) throws IOException {
        PathConfiguration output = new PathConfiguration(path, configuration);
        writeProductInSlices(product, output, StreamingProductPlugin.FORMAT_NAME, pm);
    }

    public static void writeProductInSlices(Product product, Object output, String format, ProgressMonitor pm) throws IOException {
        ProductWriter productWriter = ProductIO.getProductWriter(format);
        if (productWriter == null) {
            throw new IllegalArgumentException(String.format("No product writer found for format %s.", format));
        }
        product.setProductWriter(productWriter);
        productWriter.writeProductNodes(product, output);
        writeAllBandsInSlices(product, pm);
        product.closeProductWriter();
    }

    // TODO move to calvalusProductIO
    private static void writeAllBandsInSlices(Product product, ProgressMonitor pm) throws IOException {
        ProductWriter productWriter = product.getProductWriter();

        // for correct progress indication we need to collect
        // all bands which shall be written to the output
        List<Band> bandsToWrite = new ArrayList<>();
        for (int i = 0; i < product.getNumBands(); i++) {
            Band band = product.getBandAt(i);
            if (productWriter.shouldWrite(band)) {
                bandsToWrite.add(band);
            }
        }

        if (!bandsToWrite.isEmpty()) {
            int sceneHeight = product.getSceneRasterHeight();
            pm.beginTask("Writing bands of product '" + product.getName() + "'...", bandsToWrite.size() * sceneHeight);
            try {

                int x = 0;
                int sliceIndex = 0;
                int[] bandTileHeights = new int[bandsToWrite.size()];
                int[] bandTileWidths = new int[bandsToWrite.size()];
                for (int i = 0; i < bandsToWrite.size(); i++) {
                    bandTileHeights[i] = Math.min((int)product.getPreferredTileSize().getHeight(), bandsToWrite.get(i).getRasterHeight());
                    bandTileWidths[i] = Math.min(product.getSceneRasterWidth(), bandsToWrite.get(i).getRasterWidth());
                }

                for (int i = 0; i < bandsToWrite.size(); i++) {
                    final Band band = bandsToWrite.get(i);
                    int h = bandTileHeights[i];
                    int w = bandTileWidths[i];

                    for (int y = 0; y < bandsToWrite.get(i).getRasterHeight(); y += bandTileHeights[i], sliceIndex++) {
                        if (y + h > bandsToWrite.get(i).getRasterHeight()) {
                            h = bandsToWrite.get(i).getRasterHeight() - y;
                        }
                        Rectangle rectangle = new Rectangle(x, y, w, h);
                        Raster tile = band.getSourceImage().getData(rectangle);
                        boolean directMode = tile.getDataBuffer().getSize() == w * h;
                        ProductData productData;
                        if (directMode) {
                            Object primitiveArray = ImageUtils.getPrimitiveArray(tile.getDataBuffer());
                            productData = ProductData.createInstance(band.getDataType(), primitiveArray);
                        } else {
                            productData = ProductData.createInstance(band.getDataType(), w * h);
                            tile.getDataElements(x, y, w, h, productData.getElems());
                        }
                        productWriter.writeBandRasterData(band, x, y, w, h, productData, ProgressMonitor.NULL);
                        pm.worked(h);
                    }
                }
            } finally {
                pm.done();
            }
        }
    }

    private SequenceFile.Writer writeHeader(Product product, Path outputPath) throws IOException {
        SequenceFile.Metadata metadata = createMetadata(product, tileHeight);
        FileSystem fileSystem = outputPath.getFileSystem(configuration);
        return SequenceFile.createWriter(fileSystem,
                                         configuration,
                                         outputPath,
                                         Text.class,
                                         ByteArrayWritable.class,
                                         1024 * 1024, //buffersize,
                                         fileSystem.getDefaultReplication(),
                                         fileSystem.getDefaultBlockSize(),
                                         SequenceFile.CompressionType.NONE,
                                         null, // new DefaultCodec(),
                                         progressable,
                                         metadata);
    }

    private static void writeTiePointData(Product product, SequenceFile.Writer writer, Map<String, Long> indexMap) throws IOException {
        TiePointGrid[] tiePointGrids = product.getTiePointGrids();
        for (TiePointGrid tiePointGrid : tiePointGrids) {
            String key = "tiepoint:" + tiePointGrid.getName();
            ProductData productData = tiePointGrid.getData();
            updateIndex(indexMap, key, writer.getLength());
            writeProductData(writer, key, productData);
        }
    }

    private static void writeProductData(SequenceFile.Writer writer, String key, ProductData productData) throws IOException {
        int capacity = productData.getNumElems() * productData.getElemSize();
        InternalByteArrayOutputStream byteArrayOutputStream = new InternalByteArrayOutputStream(capacity);
        ImageOutputStream cacheImageOutputStream = new MemoryCacheImageOutputStream(byteArrayOutputStream);
        productData.writeTo(cacheImageOutputStream);
        cacheImageOutputStream.flush();
        byte[] buf = byteArrayOutputStream.getInternalBuffer();
        writer.append(new Text(key), new ByteArrayWritable(buf));
    }

    private static void updateIndex(Map<String, Long> indexMap, String key, long position) {
        indexMap.put(key, position);
    }

    private static SequenceFile.Metadata createMetadata(Product product, int tile_height) {

        final StringWriter stringWriter = new StringWriter();
        final DimapHeaderWriter writer = new DimapHeaderWriter(product, stringWriter, "");
        writer.writeHeader();
        writer.close();

        String[][] metadataKeyValues = new String[][]{
                {"dim", stringWriter.getBuffer().toString()},
                {"slice.height", Integer.toString(tile_height)},
        };

        SequenceFile.Metadata metadata = new SequenceFile.Metadata();
        for (String[] metadataKeyValue : metadataKeyValues) {
            metadata.set(new Text(metadataKeyValue[0]), new Text(metadataKeyValue[1]));
        }
        return metadata;

    }

    private static class InternalByteArrayOutputStream extends ByteArrayOutputStream {
        private InternalByteArrayOutputStream(int capacity) {
            super(capacity);
        }

        private byte[] getInternalBuffer() {
            return buf;
        }
    }

}
