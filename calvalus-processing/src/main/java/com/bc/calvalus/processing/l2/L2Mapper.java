package com.bc.calvalus.processing.l2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HDFSSimpleFileSystem;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.bc.ceres.metadata.MetadataResourceEngine;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.io.gml2.GMLWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.datamodel.GeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.FeatureUtils;
import org.esa.beam.util.io.FileUtils;
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

/**
 * Processor adapter for L2 operators.
 * <ul>
 * <li>transforms request to parameter objects</li>
 * <li>instantiates and calls operator</li>
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
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Level 2 processing", 100);
        try {
            if (jobConfig.getBoolean(JobConfigNames.CALVALUS_RESUME_PROCESSING, false)) {
                LOG.info("resume: testing for target products");
                if (!processorAdapter.shouldProcessInputProduct()) {
                    // nothing to compute, result exists
                    LOG.info("resume: target product exist.");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product exist").increment(1);
                    return;
                }
            }
            Rectangle sourceRectangle = processorAdapter.getInputRectangle();
            if (sourceRectangle != null && sourceRectangle.isEmpty()) {
                LOG.warning("product does not cover region, skip processing.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            } else {
                // process input and write target product
                int numProducts = processorAdapter.processSourceProduct(SubProgressMonitor.create(pm, 50));
                if (numProducts > 0) {
                    LOG.info(context.getTaskAttemptID() + " target product created");
                    processorAdapter.saveProcessedProducts(SubProgressMonitor.create(pm, 50));
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product processed").increment(1);

                    processMetadata(context,
                                    processorAdapter.getInputPath().toString(),
                                    processorAdapter.getInputProduct(),
                                    processorAdapter.getOutputPath().toString(),
                                    processorAdapter.openProcessedProduct()
                    );
                } else {
                    LOG.warning("product has not been processed.");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not processed").increment(1);
                }
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "BEAM exception: " + e.toString(), e);
            throw new IOException("BEAM exception: " + e.toString(), e);
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
            if (FileSystem.get(jobConfig).exists(path)) {
                HDFSSimpleFileSystem hdfsSimpleFileSystem = new HDFSSimpleFileSystem(context);
                MetadataResourceEngine metadataResourceEngine = new MetadataResourceEngine(hdfsSimpleFileSystem);

                VelocityContext vcx = metadataResourceEngine.getVelocityContext();
                vcx.put("system", System.getProperties());
                vcx.put("softwareName", "Calvalus");
                vcx.put("softwareVersion", "1.5");
                vcx.put("processingTime", ProductData.UTC.create(new Date(), 0));

                File targetFile = new File(targetPath);
                vcx.put("targetFile", targetFile);
                String targetBaseName = FileUtils.getFilenameWithoutExtension(targetFile);
                vcx.put("targetBaseName", targetBaseName);
                vcx.put("targetName", targetFile.getName());
                vcx.put("targetSize", String.format("%.1f", targetProduct.getRawStorageSize() / (1024.0f * 1024.0f)));

                vcx.put("configuration", jobConfig);
                vcx.put("sourceProduct", sourceProduct);
                vcx.put("targetProduct", targetProduct);
                GeoCoding geoCoding = targetProduct.getGeoCoding();
                if (geoCoding != null) {
                    CoordinateReferenceSystem mapCRS = geoCoding.getMapCRS();
                    try {
                        Integer epsgCode = CRS.lookupEpsgCode(mapCRS, false);
                        vcx.put("epsgCode", epsgCode);
                        CRSAuthorityFactory authorityFactory = CRS.getAuthorityFactory(true);
                        InternationalString epsgDescription = authorityFactory.getDescriptionText(epsgCode.toString());
                        vcx.put("epsgDescription", epsgDescription);
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


                Geometry geometry = FeatureUtils.createGeoBoundaryPolygon(targetProduct);
                String wkt = new WKTWriter().write(geometry);
                String gml = new GMLWriter().write(geometry);
                Envelope envelope = geometry.getEnvelopeInternal();

                vcx.put("targetProductWKT", wkt);
                vcx.put("targetProductGML", gml);
                vcx.put("targetProductEnvelope", envelope);

                metadataResourceEngine.readRelatedResource("source", sourcePath);

                metadataResourceEngine.writeRelatedResource(templatePath, targetPath);
            } else {
                LOG.severe("Template does not exists: " + templatePath);
            }
        }
    }
}
