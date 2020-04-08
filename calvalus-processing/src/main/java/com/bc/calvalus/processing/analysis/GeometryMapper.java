/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;

import java.awt.geom.GeneralPath;
import java.awt.geom.PathIterator;
import java.io.IOException;
import java.util.ArrayList;

/**
 * A mapper for generating geometries as WKT of products.
 */
public class GeometryMapper extends Mapper<NullWritable, NullWritable, Text, Text> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Geometry", 100);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 50));
            if (product != null) {
                Geometry geometry = computeProductGeometry(product);
                pm.worked(50);
                context.write(new Text("1"), new Text(geometry.toString()));
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    public Geometry computeProductGeometry(Product product) {
        try {
            final GeneralPath[] paths = ProductUtils.createGeoBoundaryPaths(product);
            final Polygon[] polygons = new Polygon[paths.length];

            GeometryFactory geometryFactory = new GeometryFactory();
            for (int i = 0; i < paths.length; i++) {
                polygons[i] = convertToJtsPolygon(paths[i].getPathIterator(null), geometryFactory);
            }
            final DouglasPeuckerSimplifier peuckerSimplifier = new DouglasPeuckerSimplifier(
                    polygons.length == 1 ? polygons[0] : geometryFactory.createMultiPolygon(polygons));
            return peuckerSimplifier.getResultGeometry();
        } catch (Exception e) {
            return null;
        }
    }

    private Polygon convertToJtsPolygon(PathIterator pathIterator, GeometryFactory factory) {
        ArrayList<double[]> coordList = new ArrayList<double[]>();
        int lastOpenIndex = 0;
        while (!pathIterator.isDone()) {
            final double[] coords = new double[6];
            final int segType = pathIterator.currentSegment(coords);
            if (segType == PathIterator.SEG_CLOSE) {
                // we should only detect a single SEG_CLOSE
                coordList.add(coordList.get(lastOpenIndex));
                lastOpenIndex = coordList.size();
            } else {
                coordList.add(coords);
            }
            pathIterator.next();
        }
        final Coordinate[] coordinates = new Coordinate[coordList.size()];
        for (int i1 = 0; i1 < coordinates.length; i1++) {
            final double[] coord = coordList.get(i1);
            coordinates[i1] = new Coordinate(coord[0], coord[1]);
        }

        return factory.createPolygon(factory.createLinearRing(coordinates), null);
    }
}
