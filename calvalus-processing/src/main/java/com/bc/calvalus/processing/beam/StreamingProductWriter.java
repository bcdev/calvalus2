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
import com.bc.calvalus.processing.hadoop.ByteArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import org.esa.beam.dataio.dimap.DimapHeaderWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.esa.beam.util.ImageUtils;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.logging.Logger;


public class StreamingProductWriter {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public static void writeProduct(Product product, Path outputPath, MapContext context, int tileHeight) throws IOException {
        SequenceFile.Writer writer = writeHeader(product, outputPath, context, tileHeight);
        LOG.info(" written header");
        Band[] bands = product.getBands();
        int x = 0;
        int w = product.getSceneRasterWidth();
        int h = tileHeight;
        int sliceIndex = 0;
        for (int y = 0; y < product.getSceneRasterHeight(); y += tileHeight, sliceIndex++) {
            if (y + h > product.getSceneRasterHeight()) {
                h = product.getSceneRasterHeight() - y;
            }
            for (Band band : bands) {
                Raster data = band.getSourceImage().getData(new Rectangle(x, y, w, h));
                ProductData productData = ProductData.createInstance(band.getDataType(),
                                                                     ImageUtils.getPrimitiveArray(data.getDataBuffer()));
                String key = band.getName() + ":" + sliceIndex;
                writeProductData(writer, key, productData);
            }
            LOG.info(" written y=" + y + " h=" + h);
        }
        writer.close();
    }

    public static SequenceFile.Writer writeHeader(Product product, Path outputPath, MapContext context, int tile_height) throws IOException {
        SequenceFile.Metadata metadata = createMetadata(product, tile_height);
        Configuration configuration = context.getConfiguration();
        FileSystem fileSystem = outputPath.getFileSystem(configuration);
        SequenceFile.Writer sequenceFileWriter = SequenceFile.createWriter(fileSystem,
                                                                           configuration,
                                                                           outputPath,
                                                                           Text.class,
                                                                           ByteArrayWritable.class,
                                                                           1024 * 1024, //buffersize,
                                                                           (short) 1, //replication,
                                                                           fileSystem.getDefaultBlockSize(),
                                                                           SequenceFile.CompressionType.NONE,
                                                                           null, // new DefaultCodec(),
                                                                           context,
                                                                           metadata);

        writeTiePointData(product, sequenceFileWriter);
        return sequenceFileWriter;
    }

    private static void writeTiePointData(Product product, SequenceFile.Writer sequenceFileWriter) throws IOException {
        TiePointGrid[] tiePointGrids = product.getTiePointGrids();
        for (TiePointGrid tiePointGrid : tiePointGrids) {
            String key = "tiepoint:" + tiePointGrid.getName();
            ProductData productData = tiePointGrid.getData();
            writeProductData(sequenceFileWriter, key, productData);
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
