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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.ByteArrayWritable;
import com.bc.ceres.core.ProgressMonitor;
import com.sun.media.jai.util.SunTileCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.dataio.dimap.DimapHeaderWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.gpf.internal.OperatorImage;
import org.esa.snap.core.image.RasterDataNodeOpImage;
import org.esa.snap.core.image.VirtualBandOpImage;
import org.esa.snap.core.util.DateTimeUtils;
import org.esa.snap.core.util.ImageUtils;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import javax.media.jai.CachedTile;
import javax.media.jai.JAI;
import javax.media.jai.TileCache;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
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
    private static boolean tileCacheDebugging;


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
            tileCacheDebugging = configuration.getBoolean(JobConfigNames.CALVALUS_DEBUG_TILECACHE, false);
        } else {
            throw new IllegalFileFormatException("input is not of the correct type.");
        }
        sequenceFileWriter = writeHeader(product, path);
        writeTiePointData(product, sequenceFileWriter, indexMap);
        LOG.info(" written header");
    }

    @Override
    public void writeBandRasterData(Band band, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData productData, ProgressMonitor pm) throws IOException {
        String key = band.getName() + ":" + sourceOffsetX + ":" + sourceOffsetY;
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

    public static void writeProductInTiles(Configuration configuration,
                                            ProgressMonitor pm,
                                            Product product,
                                            Path path) throws IOException {
        PathConfiguration output = new PathConfiguration(path, configuration);
        writeProductInTiles(product, output, StreamingProductPlugin.FORMAT_NAME, pm);
    }

    public static void writeProductInTiles(Product product, Object output, String format, ProgressMonitor pm) throws IOException {
        ProductWriter productWriter = ProductIO.getProductWriter(format);
        if (productWriter == null) {
            throw new IllegalArgumentException(String.format("No product writer found for format %s.", format));
        }
        product.setProductWriter(productWriter);
        productWriter.writeProductNodes(product, output);
        writeAllBandsInTiles(product, pm);
        product.closeProductWriter();
    }

    // TODO move to calvalusProductIO
    private static void writeAllBandsInTiles(Product product, ProgressMonitor pm) throws IOException {
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

            if (allBandsSameSize(bandsToWrite)) {
                CalvalusLogger.getLogger().info("Writing bands of the same size");
                writeSameSizedBands(product, bandsToWrite, productWriter, pm);
            } else {
                CalvalusLogger.getLogger().info("Writing bands of different sizes");
                writeDifferentSizedBands(product, pm, productWriter, bandsToWrite);
            }
        }
    }

    private static boolean allBandsSameSize(List<Band> bandsToWrite) {
        if (bandsToWrite.size() == 1) {
            return true;
        }
        int firstRasterHeight = bandsToWrite.get(0).getRasterHeight();
        int firstRasterWidth = bandsToWrite.get(0).getRasterWidth();
        for (int i = 1; i < bandsToWrite.size(); i++) {
            Band band = bandsToWrite.get(i);
            boolean isSameSize = band.getRasterHeight() == firstRasterHeight && band.getRasterWidth() == firstRasterWidth;
            if (!isSameSize) {
                return false;
            }
        }
        return true;
    }

    private static void writeDifferentSizedBands(Product product, ProgressMonitor pm, ProductWriter productWriter, List<Band> bandsToWrite) throws IOException {
        try {

            int x = 0;
            int sliceIndex = 0;
            int[] bandTileHeights = new int[bandsToWrite.size()];
            int[] bandTileWidths = new int[bandsToWrite.size()];
            for (int i = 0; i < bandsToWrite.size(); i++) {
                bandTileHeights[i] = Math.min((int) product.getPreferredTileSize().getHeight(), bandsToWrite.get(i).getRasterHeight());
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
                    tileCacheDebugging(band, rectangle);
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

    private static void writeSameSizedBands(Product product, List<Band> bandsToWrite, ProductWriter productWriter, ProgressMonitor pm) throws IOException {
        final int sceneHeight = product.getSceneRasterHeight();
        final int sceneWidth = product.getSceneRasterWidth();
        Dimension tileSize = product.getPreferredTileSize();
        final int tileHeight = tileSize.height;
        final int tileWidth = tileSize.width;
        try {
            int h = tileHeight;
            for (int y = 0; y < sceneHeight; y += tileHeight) {
                if (y + h > sceneHeight) {
                    h = sceneHeight - y;
                }
                int w = tileWidth;
                for (int x = 0; x < sceneWidth; x += tileWidth) {
                    if (x + w > sceneWidth) {
                        w = sceneWidth - x;
                    }

                    for (Band band : bandsToWrite) {
                        Rectangle rectangle = new Rectangle(x, y, w, h);
                        Raster tile = band.getSourceImage().getData(rectangle);
                        tileCacheDebugging(band, rectangle);
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
            }
        } finally {
            pm.done();
        }
    }

    private static void tileCacheDebugging(Band band, Rectangle rect) {
        if (!tileCacheDebugging) {
            return;
        }
        System.out.println("==============================================================");
        System.out.println(DateTimeUtils.ISO_8601_FORMAT.format(new Date()));
        System.out.println("writing Band = " + band.getName() + "; Rectangle = " + rect);
        TileCache tileCache = JAI.getDefaultInstance().getTileCache();
        if (tileCache instanceof SunTileCache) {
            SunTileCache sunTileCache = (SunTileCache) tileCache;
            long cacheTileCount = sunTileCache.getCacheTileCount();
            System.out.println("Tiles in Cache = " + cacheTileCount);
            long memUsed = sunTileCache.getCacheMemoryUsed() / (1024 * 1024);
            long memCapacity = sunTileCache.getMemoryCapacity() / (1024 * 1024);
            System.out.printf("Memory used %d MB (capacity %d MB)%n", memUsed, memCapacity);
            printCachedTiles(((Hashtable) sunTileCache.getCachedObject()).values());
        }
    }


    private SequenceFile.Writer writeHeader(Product product, Path outputPath) throws IOException {
        SequenceFile.Metadata metadata = createMetadata(product);
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

    private static SequenceFile.Metadata createMetadata(Product product) {

        final StringWriter stringWriter = new StringWriter();
        final DimapHeaderWriter writer = new DimapHeaderWriter(product, stringWriter, "");
        writer.writeHeader();
        writer.close();

        Dimension preferredTileSize = product.getPreferredTileSize();

        String[][] metadataKeyValues = new String[][]{
                {"dim", stringWriter.getBuffer().toString()},
                {"tile.height", Integer.toString(preferredTileSize.height)},
                {"tile.width", Integer.toString(preferredTileSize.width)},
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

    private static void printCachedTiles(Collection<CachedTile> tiles) {
        final Map<String, Long> numTiles = new HashMap<>(100);
        final Map<String, Long> sizeTiles = new HashMap<>(100);
        for (CachedTile sct : tiles) {
            RenderedImage owner = sct.getOwner();
            if (owner == null) {
                continue;
            }
            String name = owner.getClass().getSimpleName() + " " + getImageComment(owner);
            increment(numTiles, name, 1);
            increment(sizeTiles, name, sct.getTileSize());
        }
        List<Map.Entry<String, Long>> sortedBySize = new ArrayList<>(sizeTiles.entrySet());
        Collections.sort(sortedBySize, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));
        for (Map.Entry<String, Long> entry : sortedBySize) {
            String name = entry.getKey();
            Long sizeBytes = entry.getValue();
            Long tileCount = numTiles.get(name);

            System.out.printf("size=%8.2fMB  ", (sizeBytes / (1024.0 * 1024.0)));
            System.out.printf("#tiles=%5d   ", tileCount);
            System.out.print("(" + name + ")  ");
            System.out.println();
        }
    }

    private static String getImageComment(RenderedImage image) {
        if (image instanceof RasterDataNodeOpImage) {
            RasterDataNodeOpImage rdnoi = (RasterDataNodeOpImage) image;
            return rdnoi.getRasterDataNode().getName();
        } else if (image instanceof VirtualBandOpImage) {
            VirtualBandOpImage vboi = (VirtualBandOpImage) image;
            return vboi.getExpression();
        } else if (image instanceof OperatorImage) {
            final String s = image.toString();
            final int p1 = s.indexOf('[');
            final int p2 = s.indexOf(']', p1 + 1);
            if (p1 > 0 && p2 > p1) {
                return s.substring(p1 + 1, p2 - 1);
            }
            return s;
        } else {
            return "";
        }
    }

    private static void increment(Map<String, Long> numImages, String name, long amount) {
        Long count = numImages.get(name);
        if (count == null) {
            numImages.put(name, amount);
        } else {
            numImages.put(name, count.intValue() + amount);
        }
    }
}
