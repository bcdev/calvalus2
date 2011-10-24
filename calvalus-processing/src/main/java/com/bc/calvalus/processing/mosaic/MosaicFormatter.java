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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.beam.ProductFactory;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Progressable;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Point;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;


public class MosaicFormatter extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final NumberFormat PART_FILE_NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        PART_FILE_NUMBER_FORMAT.setMinimumIntegerDigits(5);
        PART_FILE_NUMBER_FORMAT.setGroupingUsed(false);
    }

    private static final String PART_FILE_PREFIX = "part-r-";
    Configuration jobConfig;
    private boolean create5by5DegreeProducts = true; //TODO configure
    List<Point> tileIndexPoints = null;
//    List<Long> tileIndexOffsets = null;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        jobConfig = context.getConfiguration();
        new ProductFactory(jobConfig);
        final FileSplit split = (FileSplit) context.getInputSplit();
        Path path = split.getPath();
        if (create5by5DegreeProducts) {
            process5by5degreeProducts(context, path);
        } else {
            processAllPartsToOneProduct(context, path);
        }
    }

    public void processAllPartsToOneProduct(Context context, Path partsDir) throws IOException {
        MosaicAlgorithm algorithm = MosaicUtils.createAlgorithm(jobConfig);
        MosaicGrid mosaicGrid = new MosaicGrid();

        Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        Rectangle geometryRegion = mosaicGrid.computeRegion(regionGeometry);
        Rectangle productRegion = mosaicGrid.alignToTileGrid(geometryRegion);

        Product product = createProduct("mosaic-result", productRegion, algorithm.getOutputFeatures());
        CrsGeoCoding geoCoding = createCRS(productRegion, mosaicGrid.getPixelSize());
        product.setGeoCoding(geoCoding);

        // TODO
        ProductWriter productWriter = createProductWriter(product, new File("/tmp/mosaic_we_sr_4.nc"), "NetCDF-BEAM");

        try {
//            Path partsDir = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-production/abc_we-lc-sr/");

            final FileStatus[] parts = getPartFiles(partsDir, FileSystem.get(jobConfig));
            for (FileStatus part : parts) {
                Path partFile = part.getPath();
                System.out.println(MessageFormat.format("reading and handling part {0}", partFile));
                handlePart(context, product, productWriter, partFile, mosaicGrid, productRegion);
            }
        } finally {
            productWriter.close();
        }
    }

    public void process5by5degreeProducts(Context context, Path partFile) throws IOException, InterruptedException {
        MosaicAlgorithm algorithm = MosaicUtils.createAlgorithm(jobConfig);
        MosaicGrid productGrid = new MosaicGrid(180 / 5, 370 * 5);
        MosaicGrid mosaicGrid = new MosaicGrid();

//        Geometry regionGeometry = null;//JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
//        Rectangle geometryRegion = mosaicGrid.computeRegion(regionGeometry);
//        Rectangle globalRegion = mosaicGrid.alignToTileGrid(geometryRegion);

        String format = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
        String compression = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, null);
        String outputPrefix = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, null);

        final int numPartitions = 18; // TODO config
        int partitionNumber = getPartitionNumber(partFile.getName());
        Geometry partGeometry = getPartGeometry(partitionNumber, numPartitions);
        LOG.info("partGeometry = " + partGeometry);
        Point[] tileIndices = productGrid.getTileIndices(partGeometry);

        for (Point tile : tileIndices) {
            context.progress();
            Rectangle productRegion = productGrid.getTileRectangle(tile.x, tile.y);
            if (tileIndexPoints != null) {
               boolean containsData = checkCacheForTiles(productRegion, mosaicGrid);
               if (!containsData) {
                   LOG.info("Cache-has not data");
                   continue;
               }
            }
            String productName = getTileProductName(outputPrefix, tile.x, tile.y);
            LOG.info("productName = " + productName);
            Product product = createProduct(productName, productRegion, algorithm.getOutputFeatures());
            CrsGeoCoding geoCoding = createCRS(productRegion, productGrid.getPixelSize());
            product.setGeoCoding(geoCoding);

            ProductFormatter productFormatter = new ProductFormatter(productName, format, compression);
            File productFile = productFormatter.createTemporaryProductFile();

            try {
                ProductWriter productWriter = createProductWriter(product, productFile, productFormatter.getOutputFormat());
                boolean containsData = false;
                try {
                    LOG.info("Writing product");
                    containsData = handlePart(context, product, productWriter, partFile, mosaicGrid, productRegion);
                } finally {
                    productWriter.close();
                }
                if (containsData) {
                    context.getCounter("Tile-Lines", Integer.toString(tile.y)).increment(1);
                    LOG.info("Copying to HDFS");
                    productFormatter.compressToHDFS(context, productFile);
                } else {
                    LOG.info("product is empty");
                }
            } finally {
                productFormatter.cleanupTempDir();
                product.dispose();
            }
        }
    }

    static Geometry getPartGeometry(int partitionNumber, int numPartitions) {
        double x1 = -180.0;
        double x2 = 180.0;
        double degreePerPartition = 180.0 / numPartitions;
        double y1 = 90.0 - partitionNumber * degreePerPartition;
        double y2 = 90.0 - (partitionNumber + 1) * degreePerPartition;
        return new GeometryFactory().toGeometry(new Envelope(x1, x2, y1, y2));
    }

    static int getPartitionNumber(String partFilename) {
        if (!partFilename.startsWith(PART_FILE_PREFIX)) {
            throw new IllegalArgumentException("part file name is not-conforming.");
        }
        String numberPart = partFilename.substring(PART_FILE_PREFIX.length());
        try {
            Number partNumber = PART_FILE_NUMBER_FORMAT.parse(numberPart);
            return partNumber.intValue();
        } catch (ParseException e) {
            throw new IllegalArgumentException("part file number can not be parsed.");
        }
    }

    static String getTileProductName(String prefix, int tileX, int tileY) {
        return String.format("%s-v%02dh%02d", prefix, tileX, tileY);
    }

    private ProductWriter createProductWriter(Product product, File outputFile, String outputFormat) throws IOException {

        ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }
        productWriter = new BufferedProductWriter(productWriter);
        productWriter.writeProductNodes(product, outputFile);
        return productWriter;
    }

    private Product createProduct(String productName, Rectangle outputRegion, String[] outputFeatures) {
        final Product product = new Product(productName, "CALVALUS-Mosaic", outputRegion.width, outputRegion.height);
        for (String outputFeature : outputFeatures) {
            Band band = product.addBand(outputFeature, ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
        }
        //TODO
        //product.setStartTime(formatterConfig.getStartTime());
        //product.setEndTime(formatterConfig.getEndTime());
        return product;
    }

    private CrsGeoCoding createCRS(Rectangle outputRegion, double pixelSize) {
        CrsGeoCoding geoCoding;
        try {
            geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                                         outputRegion.width,
                                         outputRegion.height,
                                         -180.0 + pixelSize * outputRegion.x,
                                         90.0 - pixelSize * outputRegion.y,
                                         pixelSize,
                                         pixelSize,
                                         0.0, 0.0);
        } catch (FactoryException e) {
            throw new IllegalStateException(e);
        } catch (TransformException e) {
            throw new IllegalStateException(e);
        }
        return geoCoding;
    }

    private static FileStatus[] getPartFiles(Path partsDir, FileSystem hdfs) throws IOException {
        final FileStatus[] parts = hdfs.listStatus(partsDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(PART_FILE_PREFIX);
            }
        });
        System.out.println(MessageFormat.format("collecting {0} parts", parts.length));
        Arrays.sort(parts);
        return parts;
    }

    private boolean handlePart(Progressable progressable, Product product, ProductWriter productWriter, Path partFile, MosaicGrid mosaicGrid, Rectangle productRegion) throws IOException {
        LOG.info("productRegion " + productRegion);
        boolean containsData = false;
        FileSystem hdfs = FileSystem.get(jobConfig);
        SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, jobConfig);
        try {
            boolean buildCache = false;
            if (tileIndexPoints == null) {
                tileIndexPoints = new ArrayList<Point>(360 * 10);
//                tileIndexOffsets = new ArrayList<Long>(360 * 10);
                buildCache = true;
            }

            Band[] bands = product.getBands();
            TileIndexWritable key = new TileIndexWritable();
            TileDataWritable data = new TileDataWritable();
//            long currentPos = reader.getPosition();
            while (reader.next(key)) {
                progressable.progress();
                if (buildCache) {
                    tileIndexPoints.add(new Point(key.getTileX(), key.getTileY()));
//                    tileIndexOffsets.add(currentPos);
                }
                Rectangle tileRect = mosaicGrid.getTileRectangle(key.getTileX(), key.getTileY());
                if (productRegion.contains(tileRect)) {
                    tileRect = makeRelativeTo(tileRect, productRegion);
                    containsData = true;
                    reader.getCurrentValue(data);
                    float[][] samples = data.getSamples();
                    for (int i = 0; i < bands.length; i++) {
                        progressable.progress();
                        ProductData productData = ProductData.createInstance(samples[i]);
                        productWriter.writeBandRasterData(bands[i], tileRect.x, tileRect.y, tileRect.width, tileRect.height, productData, ProgressMonitor.NULL);
                    }
                }
//                currentPos = reader.getPosition();
            }
        } finally {
            reader.close();
        }
        return containsData;
    }

    private Rectangle makeRelativeTo(Rectangle tileRect, Rectangle productRegion) {
        return new Rectangle(tileRect.x - productRegion.x, tileRect.y - productRegion.y, tileRect.width, tileRect.height);
    }

    private boolean checkCacheForTiles(Rectangle productRegion, MosaicGrid mosaicGrid) {
        LOG.info("cache has " + tileIndexPoints.size() + " elems");
        for (Point point : tileIndexPoints) {
            Rectangle tileRect = mosaicGrid.getTileRectangle(point.x, point.y);
            if (productRegion.contains(tileRect)) {
                return true;
            }
        }
        return false;
    }

}
