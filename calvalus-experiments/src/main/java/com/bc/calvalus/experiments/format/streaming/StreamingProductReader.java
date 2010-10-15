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

package com.bc.calvalus.experiments.format.streaming;

import com.bc.calvalus.hadoop.io.ByteArrayWritable;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.esa.beam.dataio.dimap.DimapProductHelpers;
import org.esa.beam.framework.dataio.AbstractProductReader;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.esa.beam.util.math.MathUtils;
import org.jdom.Document;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;


public class StreamingProductReader extends AbstractProductReader {

    private final Path path;
    private final Configuration configuration;

    private SequenceFile.Reader reader;
    private int sliceHeight;
    private long startPos;
    Map<Band, ByteArrayWritable> cache = new HashMap<Band, ByteArrayWritable>();

    public StreamingProductReader(Path path, Configuration configuration) {
        super(null);    // TODO
        this.path = path;
        this.configuration = configuration;
    }

    @Override
    protected Product readProductNodesImpl() throws IOException {
        FileSystem fileSystem = path.getFileSystem(configuration);
        reader = new SequenceFile.Reader(fileSystem, path, configuration);
        Product product = readHeader();
        product.setPreferredTileSize(product.getSceneRasterWidth(), sliceHeight);
        return product;
    }

    @Override
    protected synchronized void readBandRasterDataImpl(int sourceOffsetX, int sourceOffsetY,
                                          int sourceWidth, int sourceHeight,
                                          int sourceStepX, int sourceStepY,
                                          Band destBand,
                                          int destOffsetX, int destOffsetY,
                                          int destWidth, int destHeight,
                                          ProductData destBuffer, ProgressMonitor pm) throws IOException {
        System.out.println("destBand = " + destBand.getName());
        System.out.println("sourceOffsetX = " + sourceOffsetX);
        System.out.println("sourceOffsetY = " + sourceOffsetY);
        System.out.println("sourceWidth = " + sourceWidth);
        System.out.println("sourceHeight = " + sourceHeight);
        int sliceIndex = MathUtils.floorInt(sourceOffsetY / sliceHeight);
        System.out.println("sliceIndex = " + sliceIndex);
        System.out.println("====================================");
        String expectedKey = destBand.getName() + ":" + sliceIndex;
        System.out.println("expectedKey = " + expectedKey);

        Text key = new Text();
        ByteArrayWritable value = new ByteArrayWritable();
        try {
            reader.next(key, value);
            System.out.println("key = " + key);
            if (!key.toString().equals(expectedKey)) {
                reader.seek(startPos);
                boolean hasMore = true;
                while (hasMore) {
                    hasMore = reader.next(key, value);
                    System.out.println("key = " + key);
                    if (key.toString().equals(expectedKey)) {
                        break;
                    }
                }
                if (!key.toString().equals(expectedKey)) {
                    throw new IllegalStateException(String.format("key '%s' expected but got '%s'", expectedKey, key));
                }
            }
            byte[] byteArray = value.getArray();

            InputStream inputStream = new ByteArrayInputStream(byteArray);
            ImageInputStream iis = new MemoryCacheImageInputStream(inputStream);

            destBuffer.readFrom(iis);

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
        }
        //TODO    use cache
    }

    public void close() throws IOException {
        reader.close();
        cache.clear();
    }

    private Product readHeader() throws IOException {
        SequenceFile.Metadata metadata = reader.getMetadata();
        Text dimText = metadata.get(new Text("dim"));
        Text sliceHeightText = metadata.get(new Text("slice.height"));
        sliceHeight = Integer.parseInt(sliceHeightText.toString());

        final InputStream inputStream = new ByteArrayInputStream(dimText.getBytes());
        Document dom = DimapProductHelpers.createDom(inputStream);
        Product product = DimapProductHelpers.createProduct(dom);
        readTiepoints(product);
        startPos = reader.getPosition();
        return product;
    }

    private void readTiepoints(Product product) throws IOException {
        Text key = new Text();
        ByteArrayWritable value = new ByteArrayWritable();
        TiePointGrid[] tiePointGrids = product.getTiePointGrids();
        for (TiePointGrid tpg : tiePointGrids) {
            String expectedKey = "tiepoint:" + tpg.getName();
            reader.next(key, value);
            if (!key.toString().equals(expectedKey)) {
                throw new IllegalStateException(String.format("key '%s' expected but got '%s'", expectedKey, key));
            }
            byte[] byteArray = value.getArray();
            ProductData productData = ProductData.createInstance(tpg.getDataType(), (int) tpg.getNumDataElems());

            InputStream inputStream = new ByteArrayInputStream(byteArray);
            ImageInputStream iis = new MemoryCacheImageInputStream(inputStream);

            productData.readFrom(iis);
            tpg.setData(productData);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("/home/marcoz/tmp/inermediate_product.seq");
        StreamingProductReader reader = new StreamingProductReader(path, configuration);
        Product product = reader.readProductNodes(null, null);
        ProductWriter writer = ProductIO.getProductWriter(ProductIO.DEFAULT_FORMAT_NAME);
        writer.writeProductNodes(product, "/home/marcoz/tmp/inermediate_product.dim");
        
        ProductIO.writeProduct(product, "/home/marcoz/tmp/inermediate_product.dim", ProductIO.DEFAULT_FORMAT_NAME);
    }

}
