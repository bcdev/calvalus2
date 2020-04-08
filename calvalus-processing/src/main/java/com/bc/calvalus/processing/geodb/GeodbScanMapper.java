/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.geodb;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.Mask;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.VectorDataNode;
import org.esa.snap.core.util.ProductUtils;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.GeometryCoordinateSequenceTransformer;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.logging.Logger;

/**
 * A mapper for generating entries for the product-DB
 */
public class GeodbScanMapper extends Mapper<NullWritable, NullWritable, Text, Text> {

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Geometry", 100);
        try {
            Product product = processorAdapter.getInputProduct();
            if (product != null) {
                Polygon polygon = computeProductGeometry(product);
                if (polygon != null) {
                    // hack to remove (empty) inner rings of Sentinel 2 detector footprints
                    if (polygon.getNumInteriorRing() > 0) {
                        polygon = new Polygon((LinearRing) polygon.getExteriorRing(), null, polygon.getFactory());
                    }
                    String wkt = polygon.toString();
                    pm.worked(50);

                    DateFormat dateFormat = DateUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    ProductData.UTC startUTC = product.getStartTime();
                    String startTime = "null";
                    if (startUTC != null) {
                        startTime = dateFormat.format(startUTC.getAsDate());
                    }
                    ProductData.UTC endUTC = product.getEndTime();
                    String endTime = "null";
                    if (endUTC != null) {
                        endTime = dateFormat.format(endUTC.getAsDate());
                    }
                    String dbPath = getDBPath(processorAdapter.getInputPath(), context.getConfiguration());

                    String result = startTime + "\t" + endTime + "\t" + wkt;
                    context.write(new Text(dbPath), new Text(result));
                }
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    private Polygon computeProductGeometry(Product product) {
//        LOGGER.info("compute product geometry using reader " + product.getProductReader().getClass().getSimpleName());
//        if (product.getProductReader().getClass().getSimpleName().startsWith("Sentinel2OrthoProductReader")) {
//            LOGGER.info("determine detector footprint of S2 product");
            ProductNodeGroup<Mask> maskGroup = product.getMaskGroup();
            int nodeCount = maskGroup.getNodeCount();
            Geometry union = new GeometryFactory().createPolygon((Coordinate[]) null);
            for (int i = 0; i < nodeCount; i++) {
                Mask mask = maskGroup.get(i);
                Mask.ImageType imageType = mask.getImageType();
                if (imageType == Mask.VectorDataType.INSTANCE) {
                    CoordinateReferenceSystem mapCRS = mask.getGeoCoding().getMapCRS();
                    VectorDataNode vectorData = Mask.VectorDataType.getVectorData(mask);
                    String vectorDataName = vectorData.getName();
                    if (vectorDataName.startsWith("detector_footprint")) {
                        FeatureIterator<SimpleFeature> features = vectorData.getFeatureCollection().features();
                        while (features.hasNext()) {
                            Geometry sourceGeom = (Geometry) features.next().getDefaultGeometry();
                            try {
                                Geometry targetGeom = transformGeometry(sourceGeom, mapCRS, DefaultGeographicCRS.WGS84);
                                union = union.union(targetGeom);
                            } catch (Exception ignore) {
                                ignore.printStackTrace();
                            }
                        }
                    }
                }
            }
            Geometry footprint = TopologyPreservingSimplifier.simplify(union, 0.001);
            if (!footprint.isEmpty()) {
                if (footprint instanceof Polygon) {
                    LOGGER.info("S2 detector footprint determined: " + footprint);
                    return (Polygon) footprint;
                } else {
                    LOGGER.warning("S2 detector footprint is not a polygon: " + footprint);
                }
//            } else {
//                LOGGER.warning("footprint is empty.");
            }
//        }
        final Polygon productOutline = computeProductGeometryDefault(product);
        LOGGER.info("product outline: " + productOutline);
        return productOutline;
    }

    private Geometry transformGeometry(Geometry sourceGeom,
                                       CoordinateReferenceSystem sourceCrs,
                                       CoordinateReferenceSystem targetCrs) throws FactoryException, TransformException {
        MathTransform mt = CRS.findMathTransform(sourceCrs, targetCrs, true);
        GeometryCoordinateSequenceTransformer gcst = new GeometryCoordinateSequenceTransformer();
        gcst.setMathTransform(mt);
        return gcst.transform(sourceGeom);
    }

    /**
     * Removes the schema and the autority, if they are from the default filesystem
     */
    public static String getDBPath(Path productPath, Configuration configuration) {
        URI productURI = productPath.toUri();
        URI defaultURI = FileSystem.getDefaultUri(configuration);

        boolean sameSchema = false;
        if (defaultURI.getScheme() != null && defaultURI.getScheme().equals(productURI.getScheme())) {
            sameSchema = true;
        }
        boolean sameAuthority = false;
        if (defaultURI.getAuthority() != null && defaultURI.getAuthority().equals(productURI.getAuthority())) {
            sameAuthority = true;
        }

        String dbPath = productPath.toString();
        if (sameSchema && sameAuthority) {
            dbPath = new Path(null, null, productURI.getPath()).toString();
        }
        return dbPath;
    }

    public static Polygon computeProductGeometryDefault(Product product) {
        try {
            final boolean usePixelCenter = true;
            final Rectangle region = new Rectangle(0, 0, product.getSceneRasterWidth(), product.getSceneRasterHeight());
            final int step = Math.min(region.width, region.height) / 8;
            final GeoPos[] geoPoints = ProductUtils.createGeoBoundary(product, region, step, usePixelCenter);

            final Coordinate[] coordinates = new Coordinate[geoPoints.length + 1];
            for (int i = 0; i < geoPoints.length; i++) {
                coordinates[i] = new Coordinate(geoPoints[i].lon, geoPoints[i].lat);
            }
            coordinates[coordinates.length - 1] = new Coordinate(geoPoints[0].lon, geoPoints[0].lat);

            GeometryFactory factory = new GeometryFactory();
            LinearRing linearRing = factory.createLinearRing(coordinates);
            return factory.createPolygon(linearRing, null);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
