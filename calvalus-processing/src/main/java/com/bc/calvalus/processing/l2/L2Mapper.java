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

package com.bc.calvalus.processing.l2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.beam.SnapGraphAdapter;
import com.bc.calvalus.processing.hadoop.HDFSSimpleFileSystem;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.bc.ceres.metadata.MetadataResourceEngine;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.io.gml2.GMLWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.velocity.VelocityContext;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.FeatureUtils;
import org.esa.snap.core.util.io.FileUtils;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.InternationalString;

import javax.measure.unit.Unit;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Processor adapter for L2 operators.
 * <ul>
 * <li>transforms request to parameter objects</li>
 * <li>instantiates and calls operator</li>
 * <li>handles results</li>
 * </ul>
 *
 * @author Boe
 */
public class L2Mapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Mapper implementation method. See class comment.
     *
     * @param context task "configuration"
     *
     * @throws java.io.IOException  if installation or process initiation raises it
     * @throws InterruptedException if processing is interrupted externally
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration jobConfig = context.getConfiguration();
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        LOG.info("processing input " + processorAdapter.getInputPath() + " ...");
        final int progressForProcessing = processorAdapter.supportsPullProcessing() ? 5 : 95;
        final int progressForSaving = processorAdapter.supportsPullProcessing() ? 95 : 5;
        pm.beginTask("Level 2 processing", progressForProcessing + progressForSaving);
        try {
            long t0 = System.currentTimeMillis();
            processorAdapter.prepareProcessing();
            if (!jobConfig.getBoolean(JobConfigNames.CALVALUS_PROCESS_ALL, false)) {
                LOG.info("testing whether target product exists ...");
                if (processorAdapter.canSkipInputProduct()) {
                    LOG.info("target product exists, nothing to compute.");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product exist").increment(1);
                    return;
                }
                LOG.info("target product does not exist");
            }
            Rectangle sourceRectangle = processorAdapter.getInputRectangle();
            if (sourceRectangle != null && sourceRectangle.isEmpty()) {
                LOG.warning("product does not cover region, skipping processing.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            } else {
                LOG.info("preparing done in [ms]: " + (System.currentTimeMillis() - t0));
                t0 = System.currentTimeMillis();
                // process input and write target product
                if (processorAdapter.processSourceProduct(ProcessorAdapter.MODE.EXECUTE, SubProgressMonitor.create(pm, progressForProcessing))) {
                    LOG.info(context.getTaskAttemptID() + " target product created");
                    processorAdapter.saveProcessedProducts(SubProgressMonitor.create(pm, progressForSaving));
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product processed").increment(1);

                    if (jobConfig.get(JobConfigNames.CALVALUS_METADATA_TEMPLATE) != null) {
                        processMetadata(context,
                                        processorAdapter.getInputPath().toString(),
                                        processorAdapter.getInputProduct(),
                                        processorAdapter.getOutputProductPath().toString(),
                                        processorAdapter.openProcessedProduct());
                    }
                    LOG.info("processing done in [ms]: " + (System.currentTimeMillis() - t0));
                } else {
                    LOG.warning("product has not been processed.");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not processed").increment(1);
                }
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Processing exception: " + e.toString(), e);
            throw new IOException("Processing exception: " + e.toString(), e);
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    static void processMetadata(Mapper.Context context,
                                String sourcePath, Product sourceProduct,
                                String targetPath, Product targetProduct) throws IOException {
        Configuration jobConfig = context.getConfiguration();
        String templatePath = jobConfig.get(JobConfigNames.CALVALUS_METADATA_TEMPLATE);
        if (templatePath != null) {
            Path path = new Path(templatePath);
            if (path.getFileSystem(jobConfig).exists(path)) {
                HDFSSimpleFileSystem hdfsSimpleFileSystem = new HDFSSimpleFileSystem(context);
                MetadataResourceEngine metadataResourceEngine = new MetadataResourceEngine(hdfsSimpleFileSystem);

                VelocityContext vcx = metadataResourceEngine.getVelocityContext();
                vcx.put("system", System.getProperties());
                vcx.put("softwareName", "Calvalus");
                vcx.put("softwareVersion", "2.7-SNAPSHOT");
                vcx.put("processingTime", ProductData.UTC.create(new Date(), 0));

                File targetFile = new File(targetPath);
                vcx.put("targetFile", targetFile);
                String targetBaseName = FileUtils.getFilenameWithoutExtension(targetFile);
                vcx.put("targetBaseName", targetBaseName);
                vcx.put("targetName", targetFile.getName());
                vcx.put("targetSize", String.format("%.1f", targetProduct != null ? targetProduct.getRawStorageSize() / (1024.0f * 1024.0f) : 0.0));

                vcx.put("configuration", jobConfig);
                vcx.put("sourceProduct", sourceProduct);
                vcx.put("targetProduct", targetProduct);
                GeoCoding geoCoding = targetProduct != null ? targetProduct.getSceneGeoCoding() : null;
                if (geoCoding != null) {
                    CoordinateReferenceSystem mapCRS = geoCoding.getMapCRS();
                    try {
                        Integer epsgCode = CRS.lookupEpsgCode(mapCRS, false);
                        if (epsgCode != null) {
                            vcx.put("epsgCode", epsgCode);
                            CRSAuthorityFactory authorityFactory = CRS.getAuthorityFactory(true);
                            InternationalString descriptionText = authorityFactory.getDescriptionText(
                                    "EPSG:" + epsgCode.toString());
                            if (descriptionText != null) {
                                String epsgDescription = descriptionText.toString();
                                vcx.put("epsgDescription", epsgDescription);
                            }
                        }
                    } catch (FactoryException ignore) {
                    }

                    if (geoCoding.getImageToMapTransform() instanceof AffineTransform2D) {
                        AffineTransform2D affineTransform2D = (AffineTransform2D) geoCoding.getImageToMapTransform();
                        double resolution = affineTransform2D.getScaleX();

                        Unit<?> unit = mapCRS.getCoordinateSystem().getAxis(0).getUnit();
                        String unitSymbol = unit.toString();
                        if ("°".equals(unitSymbol)) {
                            unitSymbol = "degree";
                        }
                        vcx.put("resolutionUnit", unitSymbol);
                        vcx.put("resolution", String.format("%.4f", resolution));
                    }
                }

                if (jobConfig.getBoolean(JobConfigNames.CALVALUS_OUTPUT_QUICKLOOKS, false)) {
                    if (jobConfig.get(JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS) != null) {
                        String qlName = targetBaseName + ".png";
                        vcx.put("quicklookName", qlName);
                    }
                }


                Geometry geometry = FeatureUtils.createGeoBoundaryPolygon(targetProduct != null ? targetProduct : sourceProduct);
                String wkt = new WKTWriter().write(geometry);
                String gml = getGML(geometry);
                Envelope envelope = geometry.getEnvelopeInternal();

                vcx.put("targetProductWKT", wkt);
                vcx.put("targetProductGML", gml);
                vcx.put("targetProductEnvelope", envelope);
                vcx.put("GlobalFunctions", new SnapGraphAdapter.GlobalFunctions(LOG));

                metadataResourceEngine.readRelatedResource("source", sourcePath);

                metadataResourceEngine.writeRelatedResource(templatePath, targetPath);
            } else {
                LOG.severe("Template does not exists: " + templatePath);
            }
        }
    }

    static String getGML(Geometry geometry) {
        // to many white-spaces break the display of the geometry in a GeoServer
        String gmlString = new GMLWriter().write(geometry);
        Pattern pattern = Pattern.compile("\\s{2,}"); // 2 or more succeeding white-spaces
        Matcher matcher = pattern.matcher(gmlString);

        return matcher.replaceAll(" ");
    }
}
