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

package com.bc.calvalus.processing.prevue;

import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;
import org.esa.snap.core.util.math.MathUtils;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.cs.DefaultCartesianCS;
import org.geotools.referencing.cs.DefaultEllipsoidalCS;
import org.geotools.referencing.datum.DefaultGeodeticDatum;
import org.geotools.referencing.operation.projection.MapProjection;
import org.geotools.referencing.operation.projection.TransverseMercator;
import org.opengis.geometry.DirectPosition;
import org.opengis.parameter.ParameterDescriptor;
import org.opengis.parameter.ParameterDescriptorGroup;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CRSFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.GeographicCRS;
import org.opengis.referencing.datum.GeodeticDatum;
import org.opengis.referencing.operation.Conversion;
import org.opengis.referencing.operation.CoordinateOperationFactory;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.OperationMethod;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.util.HashMap;

/**
 * For "Prevue", does data extraction in a special way...
 */
public class PrevueMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration jobConfig = context.getConfiguration();
        final MAConfig maConfig = MAConfig.get(jobConfig);

        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        processorAdapter.prepareProcessing();
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Geometry", 100);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 50));
            if (product == null) {
                processorAdapter.dispose();
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Empty products").increment(1);
                return;
            }
            RecordSource recordSource = getReferenceRecordSource(maConfig);
            handleProduct(product, recordSource, context);
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Used products").increment(1);
        } catch (Exception e) {
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Failed products").increment(1);
            throw new IOException(e);
        } finally {
            pm.done();
            context.setStatus("");
            processorAdapter.dispose();
        }
    }

    private void handleProduct(Product product, RecordSource recordSource, Context context) throws Exception {
        DecimalFormat decimalFormat = new DecimalFormat("000");
        for (Record record : recordSource.getRecords()) {
            context.progress();
            GeoPos location = record.getLocation();
            PixelPos pixelPos = product.getSceneGeoCoding().getPixelPos(location, null);
            if (product.containsPixel(pixelPos)) {
                Double id = (Double) record.getAttributeValues()[0];
                String idAsString = decimalFormat.format(id);
                context.setStatus("ID " + idAsString);

                ReprojectionOp reprojectionOp;
                if (product.getProductType().contains("_FSG_")) {
                    reprojectionOp = getUtmReprojectionOp(product, location);
                } else {
                    reprojectionOp = getLatLonReprojectionOp(product, location);
                }
                Product targetProduct = reprojectionOp.getTargetProduct();

                // NO metadata
                targetProduct.getMetadataRoot().getElementGroup().removeAll();

                Path outputDir = new Path(FileOutputFormat.getWorkOutputPath(context), idAsString);
                Path outputPath = new Path(outputDir, product.getName() + ".txt");

                FileSystem fileSystem = outputPath.getFileSystem(context.getConfiguration());
                FSDataOutputStream outputStream = fileSystem.create(outputPath);
                Writer writer = new OutputStreamWriter(outputStream);

                ProductWriter ascii = ProductIO.getProductWriter("BEAM-ASCII");
                ascii.writeProductNodes(targetProduct, writer);
                ascii.close();

                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Written ASCII products").increment(1);

                reprojectionOp.dispose();
            }
        }
    }

    private ReprojectionOp getLatLonReprojectionOp(Product product, GeoPos location) throws Exception {
        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setSourceProduct(product);
        reprojectionOp.setParameter("crs", DefaultGeographicCRS.WGS84.toWKT());
        reprojectionOp.setParameter("referencePixelX", 25.5);
        reprojectionOp.setParameter("referencePixelY", 25.5);
        reprojectionOp.setParameter("pixelSizeX", 1.0 / 112.0);
        reprojectionOp.setParameter("pixelSizeY", 1.0 / 112.0);
        reprojectionOp.setParameter("width", 49);
        reprojectionOp.setParameter("height", 49);
        reprojectionOp.setParameter("easting", (double) location.getLon());
        reprojectionOp.setParameter("northing", (double) location.getLat());
        return reprojectionOp;
    }

    private ReprojectionOp getUtmReprojectionOp(Product product, GeoPos location) throws Exception {
        CoordinateReferenceSystem crsUtmAutomatic = getCRSUtmAutomatic(location);

        DefaultGeographicCRS wgs84 = DefaultGeographicCRS.WGS84;
        MathTransform transform = CRS.findMathTransform(wgs84, crsUtmAutomatic);
        DirectPosition centerWgs84 = new DirectPosition2D(wgs84, location.getLon(), location.getLat());
        DirectPosition centerUTM = transform.transform(centerWgs84, null);

        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setSourceProduct(product);
        reprojectionOp.setParameter("crs", crsUtmAutomatic.toWKT());
        reprojectionOp.setParameter("referencePixelX", 25.5);
        reprojectionOp.setParameter("referencePixelY", 25.5);
        reprojectionOp.setParameter("pixelSizeX", 260.0);
        reprojectionOp.setParameter("pixelSizeY", 260.0);
        reprojectionOp.setParameter("width", 49);
        reprojectionOp.setParameter("height", 49);
        reprojectionOp.setParameter("easting", centerUTM.getOrdinate(0));
        reprojectionOp.setParameter("northing", centerUTM.getOrdinate(1));
        return reprojectionOp;
    }

    static CoordinateReferenceSystem getCRSUtmAutomatic(final GeoPos referencePos) throws FactoryException {
        GeodeticDatum datum = DefaultGeodeticDatum.WGS84;
        int zoneIndex = getZoneIndex((float) referencePos.getLon());
        final boolean south = referencePos.getLat() < 0.0;
        ParameterValueGroup tmParameters = createTransverseMercatorParameters(zoneIndex, south, datum);
        final String projName = getProjectionName(zoneIndex, south);

        return createCrs(projName, new TransverseMercator.Provider(), tmParameters, datum);
    }

    static int getZoneIndex(double longitude) {
        final float zoneIndex = (((float)longitude + 180.0f) / 6.0f - 0.5f) + 1;
        return MathUtils.roundAndCrop(zoneIndex, 1, 60);
    }

    static ParameterValueGroup createTransverseMercatorParameters(int zoneIndex, boolean south, GeodeticDatum datum) {
        ParameterDescriptorGroup tmParameters = new TransverseMercator.Provider().getParameters();
        ParameterValueGroup tmValues = tmParameters.createValue();

        setValue(tmValues, MapProjection.AbstractProvider.SEMI_MAJOR, datum.getEllipsoid().getSemiMajorAxis());
        setValue(tmValues, MapProjection.AbstractProvider.SEMI_MINOR, datum.getEllipsoid().getSemiMinorAxis());
        setValue(tmValues, MapProjection.AbstractProvider.LATITUDE_OF_ORIGIN, 0.0);
        setValue(tmValues, MapProjection.AbstractProvider.CENTRAL_MERIDIAN, getCentralMeridian(zoneIndex));
        setValue(tmValues, MapProjection.AbstractProvider.SCALE_FACTOR, 0.9996);
        setValue(tmValues, MapProjection.AbstractProvider.FALSE_EASTING, 500000.0);
        setValue(tmValues, MapProjection.AbstractProvider.FALSE_NORTHING, south ? 10000000.0 : 0.0);
        return tmValues;
    }

    static double getCentralMeridian(int zoneIndex) {
        return (zoneIndex - 0.5) * 6.0 - 180.0;
    }

    static CoordinateReferenceSystem createCrs(String crsName,
                                               OperationMethod method,
                                               ParameterValueGroup parameters,
                                               GeodeticDatum datum) throws FactoryException {
        final CRSFactory crsFactory = ReferencingFactoryFinder.getCRSFactory(null);
        final CoordinateOperationFactory coFactory = ReferencingFactoryFinder.getCoordinateOperationFactory(null);
        final HashMap<String, Object> projProperties = new HashMap<String, Object>();
        projProperties.put("name", crsName + " / " + datum.getName().getCode());
        final Conversion conversion = coFactory.createDefiningConversion(projProperties,
                                                                         method,
                                                                         parameters);
        final HashMap<String, Object> baseCrsProperties = new HashMap<String, Object>();
        baseCrsProperties.put("name", datum.getName().getCode());
        final GeographicCRS baseCrs = crsFactory.createGeographicCRS(baseCrsProperties, datum,
                                                                     DefaultEllipsoidalCS.GEODETIC_2D);
        return crsFactory.createProjectedCRS(projProperties, baseCrs, conversion, DefaultCartesianCS.PROJECTED);
    }

    private static void setValue(ParameterValueGroup values, ParameterDescriptor descriptor, double value) {
        values.parameter(descriptor.getName().getCode()).setValue(value);
    }

    static String getProjectionName(int zoneIndex, boolean south) {
        return "UTM Zone " + zoneIndex + (south ? ", South" : "");
    }

    private RecordSource getReferenceRecordSource(MAConfig maConfig) {
        final RecordSource referenceRecordSource;
        try {
            referenceRecordSource = maConfig.createRecordSource();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return referenceRecordSource;
    }


}
