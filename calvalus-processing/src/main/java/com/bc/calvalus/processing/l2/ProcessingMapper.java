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
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.analysis.QuicklookGenerator;
import com.bc.calvalus.processing.analysis.Quicklooks;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.beam.SnapGraphAdapter;
import com.bc.calvalus.processing.hadoop.HDFSSimpleFileSystem;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.bc.ceres.metadata.MetadataResourceEngine;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.io.gml2.GMLWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.velocity.VelocityContext;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.FeatureUtils;
import org.esa.snap.core.util.StringUtils;
import org.esa.snap.core.util.io.FileUtils;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.InternationalString;

import javax.imageio.ImageIO;
import javax.measure.unit.Unit;
import java.awt.Rectangle;
import java.awt.image.RenderedImage;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class ProcessingMapper extends Mapper<NullWritable, NullWritable, Text /*N1 input name*/, Text /*split output name*/> {

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};

    /** For use with cdt, called with request and input and output dir as parameters */
    public static void main(String args[]) {
        try {
            final String path = args[0];
            final String input = args[1];
            final String output = args[2];

            final ObjectMapper jsonParser = new ObjectMapper().configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            final Map<String, Object> request = jsonParser.readValue(new File(path), VALUE_TYPE_REF);
            final Configuration conf = new Configuration();
            for (Map.Entry<String,Object> entry : request.entrySet()) {
                conf.set(entry.getKey(), String.valueOf(entry.getValue()));
            }
            // TODO translate all request parameters
            translateParameter(conf, "productionName", "mapreduce.job.name");
            translateParameter(conf, "processorAdapterType", "calvalus.l2.processorType");  // TODO
            translateParameter(conf, "processorName", "calvalus.l2.scriptFiles");  // TODO
            
            conf.set("mapreduce.output.fileoutputformat.outputdir", output);

            final FileSplit split = new ProductSplit(new Path(input), 1, new String[] { "localhost" });
            final NoRecordReader reader = new NoRecordReader();
            final StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();

            final String productionName = String.valueOf(request.get("productionName")).replaceAll(" ", "_");
            final TaskAttemptID task = new TaskAttemptID(productionName,0, TaskType.MAP, 0, 0);
            final TaskAttemptContextImpl taskContext = new TaskAttemptContextImpl(conf, task, reporter);
            final OutputCommitter committer = new SimpleOutputFormat().getOutputCommitter(taskContext);

            final MapContextImpl<NullWritable, NullWritable, Text, Text> mapContext =
                    new MapContextImpl<NullWritable, NullWritable, Text, Text>(conf, task, reader, null, committer, reporter, split);
            final Mapper<NullWritable, NullWritable, Text, Text>.Context context =
                    new WrappedMapper<NullWritable, NullWritable, Text, Text>().getMapContext(mapContext);

            final ProcessingMapper mapper = new ProcessingMapper();
            mapper.run(context);
            committer.commitTask(taskContext);
            committer.cleanupJob(taskContext);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Mapper implementation method. See class comment.
     *
     * Parameters handled in this mapper:
     *   outputFormat
     *   outputCompression
     *   metadataTemplate
     *   qlParameters
     *   outputRegex
     *   outputReplacement
     *   outputDateElement
     *   outputDateFormat
     *   forceReprocess
     *   outputCrs
     *   replaceNanValue
     *   regionGeometry
     *   outputBands
     * + the parameters listed in translateParameters
     *
     * @param context  the task, a Hadoop "configuration"
     * @throws IOException          if installation or process initiation raises it
     * @throws InterruptedException if processing is interrupted externally
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {

        translateParameters(context);
        final Configuration jobConfig = context.getConfiguration();
        final String outputFormat = jobConfig.get(JobConfigNames.OUTPUT_FORMAT);
        final String outputCompression = jobConfig.get(JobConfigNames.OUTPUT_COMPRESSION);

        final ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        String inputName = processorAdapter.getInputPath().getName();
        String productName = null;
        if (processorAdapter.getInputParameters() != null) {
            for (int i = 0; i < processorAdapter.getInputParameters().length; i += 2) {
                if ("output".equals(processorAdapter.getInputParameters()[i])) {
                    productName = getProductName(jobConfig, processorAdapter.getInputParameters()[i + 1]);
                }
            }
        }
        if (productName == null) {
            if (! "MTD_MSIL1C.xml".equals(inputName)) {  // TODO
                productName = getProductName(jobConfig, inputName);
            } else {
                productName = getProductName(jobConfig, processorAdapter.getInputPath().getParent().getName());
            }
        }
        final ProductFormatter productFormatter = outputFormat != null ? new ProductFormatter(productName, outputFormat, outputCompression) : null;

        final ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        final int progressForProcessing = processorAdapter.supportsPullProcessing() ? 5 : 95;
        final int progressForSaving = processorAdapter.supportsPullProcessing() ? 95 : 5;

        LOG.info("processing input " + processorAdapter.getInputPath() + " ...");
        pm.beginTask("Level 2 processing", progressForProcessing + progressForSaving);

        try {

            // check and prepare, localise

            long t0 = System.currentTimeMillis();
            if (productFormatter != null && checkFormattedOutputExists(jobConfig, context, productFormatter.getOutputFilename())) { return; }
            processorAdapter.prepareProcessing();
            if (checkNativeOutputExists(jobConfig, context, processorAdapter)) { return; }
            if (checkInputIntersectsRoi(jobConfig, context, processorAdapter)) { return; }
            LOG.info("preparing done in [ms]: " + (System.currentTimeMillis() - t0));

            // process and write native product

            t0 = System.currentTimeMillis();
            if (!processorAdapter.processSourceProduct(ProcessorAdapter.MODE.EXECUTE,
                                                       SubProgressMonitor.create(pm, progressForProcessing))) {
                LOG.warning("product has not been processed.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not processed").increment(1);
                return;
            }

            if (jobConfig.getBoolean("outputNative", false) || productFormatter == null) {
                LOG.info(context.getTaskAttemptID() + " target product created");
                processorAdapter.saveProcessedProducts(SubProgressMonitor.create(pm, progressForSaving));
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product processed").increment(1);
                LOG.info("product processed");
            }

            // re-read target product for customisation, metadata processing, quicklook generation, formatting

            if (checkNoFormattingRequired(jobConfig)) { return; }
            Product targetProduct;
            if (jobConfig.getBoolean("outputNative", false)) {
                targetProduct = ProductIO.readProduct(processorAdapter.getOutputProductPath().toString());
            } else {
                targetProduct = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, progressForProcessing));
            }
            LOG.info("target product determined");
            if (checkProductEmpty(context, targetProduct)) { return; }
            targetProduct = customiseTargetProduct(jobConfig, processorAdapter, targetProduct);

            if (jobConfig.get(JobConfigNames.METADATA_TEMPLATE) != null) {
                context.setStatus("Metadata");
                processMetadata(context,
                                processorAdapter.getInputPath().toString(),
                                processorAdapter.getInputProduct(),
                                processorAdapter.getOutputProductPath().toString(),
                                targetProduct);
                LOG.info("metadata template " + jobConfig.get(JobConfigNames.METADATA_TEMPLATE) + " applied");
            }
            pm.worked(5);

            if (jobConfig.get(JobConfigNames.QL_PARAMETERS) != null) {
                context.setStatus("Quicklooks");
                writeQuicklooks(context, jobConfig, productName, targetProduct);
            }
            pm.worked(5);
            LOG.info("processing done in [ms]: " + (System.currentTimeMillis() - t0));

            context.setStatus("Writing");
            if (productFormatter != null) {
                writeProductFile(targetProduct, productFormatter, context, jobConfig,
                                 productFormatter.getOutputFormat(), SubProgressMonitor.create(pm, progressForSaving));
            }
            pm.worked(10);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Processing exception: " + e.toString(), e);
            throw new IOException("Processing exception: " + e.toString(), e);
        } finally {
            context.setStatus("");
            if (productFormatter != null) {
                productFormatter.cleanupTempDir();
            }
            pm.done();
            processorAdapter.dispose();
        }
    }

    protected void translateParameters(Context context) {
        Configuration conf = context.getConfiguration();
        translateParameter(conf, JobConfigNames.PRESERVE_DATE_TREE, JobConfigNames.CALVALUS_OUTPUT_PRESERVE_DATE_TREE);
        translateParameter(conf, JobConfigNames.REGION_GEOMETRY, JobConfigNames.CALVALUS_REGION_GEOMETRY);
        translateParameter(conf, JobConfigNames.INPUT_FORMAT, JobConfigNames.CALVALUS_INPUT_FORMAT);
        translateParameter(conf, JobConfigNames.INPUT_SUBSETTING, JobConfigNames.CALVALUS_INPUT_SUBSETTING);
        translateParameter(conf, JobConfigNames.OUTPUT_SUBSETTING, JobConfigNames.CALVALUS_OUTPUT_SUBSETTING);
        translateParameter(conf, JobConfigNames.PROCESSOR_PARAMETERS, JobConfigNames.CALVALUS_L2_PARAMETERS);
        translateParameter(conf, JobConfigNames.QL_PARAMETERS, JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS);
        translateParameter(conf, JobConfigNames.PROCESSOR_NAME, JobConfigNames.CALVALUS_L2_OPERATOR);
        translateParameter(conf, JobConfigNames.FORCE_REPROCESS, JobConfigNames.CALVALUS_PROCESS_ALL);
        translateParameter(conf, JobConfigNames.SNAP_TILECACHE, "calvalus.system.snap.jai.tileCacheSize");
    }

    private static void translateParameter(Configuration conf, String in, String out) {
        if (conf.get(in) != null) {
            conf.set(out, conf.get(in));
        }
    }


    protected String getProductName(Configuration jobConfig, String fileName) {
        String regex = jobConfig.get(JobConfigNames.OUTPUT_REGEX, "^(.*)$");
        String replacement = jobConfig.get(JobConfigNames.OUTPUT_REPLACEMENT, "L2_of_$1");
        String dateElement = jobConfig.get(JobConfigNames.OUTPUT_DATE_ELEMENT, null);
        String dateFormat = jobConfig.get(JobConfigNames.OUTPUT_DATE_FORMAT, null);
        try {
            String newProductName = FileUtils.getFilenameWithoutExtension(fileName);
            if (regex != null && replacement != null) {
                Matcher m = Pattern.compile(regex).matcher(newProductName);
                newProductName = m.replaceAll(replacement);
                if (dateElement != null && dateFormat != null) {
                    final String dateString = m.replaceAll(dateElement);

                    final DateFormat df1 = DateUtils.createDateFormat(dateFormat);
                    final DateFormat df2 = DateUtils.createDateFormat(newProductName);
                    final Date date = df1.parse(dateString);
                    newProductName = df2.format(date);
                }
                LOG.info("output name for " + fileName + " set to " + newProductName);
            }
            return newProductName;
        } catch (Exception e) {
            if (dateElement == null || dateFormat == null) {
                throw new RuntimeException("failed to convert name " + fileName + " matching regex " + regex + " by " + replacement);
            } else {
                throw new RuntimeException("failed to convert name " + fileName + " matching regex " + regex +
                                                   " date ele " + dateElement + " date format " + dateFormat + " by " + replacement);
            }
        }
    }


    protected boolean checkFormattedOutputExists(Configuration jobConfig, Context context, String outputFilename) throws IOException {
        if (!jobConfig.getBoolean(JobConfigNames.FORCE_REPROCESS, false)) {
            Path outputProductPath = new Path(FileOutputFormat.getOutputPath(context), outputFilename);
            LOG.fine("check whether target product " + outputProductPath + " exists");
            if (outputProductPath.getFileSystem(jobConfig).exists(outputProductPath)) {
                LOG.info("formatted target product " + outputProductPath + " exists, nothing to compute");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product exist").increment(1);
                return true;
            }
        }
        return false;
    }

    protected boolean checkNativeOutputExists(Configuration jobConfig, Context context, ProcessorAdapter processorAdapter) throws IOException {
        if (!jobConfig.getBoolean(JobConfigNames.FORCE_REPROCESS, false) && jobConfig.get(JobConfigNames.OUTPUT_FORMAT) == null && jobConfig.get(JobConfigNames.QL_PARAMETERS) == null) {
            LOG.fine("checking whether native target product exists ...");
            if (processorAdapter.canSkipInputProduct()) {
                LOG.info("native target product " + processorAdapter.getOutputProductPath() + " exists, nothing to compute.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product exist").increment(1);
                return true;
            }
        }
        return false;
    }

    protected boolean checkInputIntersectsRoi(Configuration jobConfig, Context context, ProcessorAdapter processorAdapter) throws IOException {
        Rectangle sourceRectangle = processorAdapter.getInputRectangle();
        if (sourceRectangle != null && sourceRectangle.isEmpty()) {
            LOG.info("input product does not cover region, processing skipped");
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            return true;
        }
        return false;
    }

    protected boolean checkNoFormattingRequired(Configuration jobConfig) {
        return jobConfig.get("outputFormat") == null
                && jobConfig.get("qlParameters") == null
                && jobConfig.get(JobConfigNames.METADATA_TEMPLATE) == null;
    }

    protected boolean checkProductEmpty(Mapper.Context context, Product product) {
         if (product == null || product.getSceneRasterWidth() == 0 || product.getSceneRasterHeight() == 0) {
             LOG.info("target product is empty, writing skipped");
             context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
             return true;
         }
         return false;
     }


    protected Product customiseTargetProduct(Configuration jobConfig, ProcessorAdapter processorAdapter, Product targetProduct) throws IOException {

        String crsWkt = jobConfig.get(JobConfigNames.OUTPUT_CRS);
        if (StringUtils.isNotNullAndNotEmpty(crsWkt)) {
            targetProduct = doReprojection(crsWkt, jobConfig, targetProduct);
            LOG.info("target product reprojected to crs " + crsWkt);
        }
        Map<String, Object> spatialSubsetParameter = createSpatialSubsetParameter(jobConfig);
        if (!spatialSubsetParameter.isEmpty()) {
            targetProduct = GPF.createProduct("Subset", spatialSubsetParameter, targetProduct);
            LOG.info("reprojected target product spatially subsetted to " + jobConfig.get(JobConfigNames.REGION_GEOMETRY));
        }
        if (ProcessorAdapter.hasInvalidStartAndStopTime(targetProduct)) {
            ProcessorAdapter.copySceneRasterStartAndStopTime(processorAdapter.getInputProduct(), targetProduct, null);
            LOG.info("target product start and stop times set, copied from input");
        }
        return targetProduct;
    }

    protected Product doReprojection(String crsWkt, Configuration jobConfig, Product product) {
        Map<String, Object> reprojParams = new HashMap<String, Object>();
        reprojParams.put("crs", crsWkt);
        if (jobConfig.get(JobConfigNames.REPLACE_NAN_VALUE) != null) {
            double replacement = jobConfig.getFloat(JobConfigNames.REPLACE_NAN_VALUE, 0);
            reprojParams.put("noDataValue", replacement);
        }
        product = GPF.createProduct("Reproject", reprojParams, product);
        return product;
    }


    protected void processMetadata(Context context,
                               String sourcePath, Product sourceProduct,
                               String targetPath, Product targetProduct) throws IOException {
       Configuration jobConfig = context.getConfiguration();
       String templatePath = jobConfig.get(JobConfigNames.METADATA_TEMPLATE);
       if (templatePath != null) {
           Path path = new Path(templatePath);
           if (path.getFileSystem(jobConfig).exists(path)) {
               HDFSSimpleFileSystem hdfsSimpleFileSystem = new HDFSSimpleFileSystem(context);
               MetadataResourceEngine metadataResourceEngine = new MetadataResourceEngine(hdfsSimpleFileSystem);

               VelocityContext vcx = metadataResourceEngine.getVelocityContext();
               vcx.put("system", System.getProperties());
               vcx.put("softwareName", "Calvalus");
               vcx.put("softwareVersion", "2.15-SNAPSHOT");
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

               if (jobConfig.get(JobConfigNames.QL_PARAMETERS) != null) {
                   String qlName = targetBaseName + ".png";
                   vcx.put("quicklookName", qlName);
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

   private static String getGML(Geometry geometry) {
       // to many white-spaces break the display of the geometry in a GeoServer
       String gmlString = new GMLWriter().write(geometry);
       Pattern pattern = Pattern.compile("\\s{2,}"); // 2 or more succeeding white-spaces
       Matcher matcher = pattern.matcher(gmlString);
       return matcher.replaceAll(" ");
   }


    protected void writeQuicklooks(Mapper.Context context, Configuration jobConfig, String productName, Product targetProduct) {
        LOG.info("Creating quicklooks.");
        List<Quicklooks.QLConfig> qlConfigList = getValidQlConfigs(jobConfig);
        for (Quicklooks.QLConfig qlConfig : qlConfigList) {
            String imageFileName;
            imageFileName = productName;
            if (qlConfigList.size() > 1) {
                imageFileName = imageFileName + "_" + qlConfig.getBandName();
            }
            try {
                createQuicklook(targetProduct, imageFileName, context, qlConfig);
                LOG.info("quicklook for " + qlConfig.getBandName() + " created");
            } catch (Exception e) {
                String msg = String.format("Could not create quicklook image '%s'.", qlConfig.getBandName());
                LOG.log(Level.WARNING, msg, e);
            }
        }
        LOG.info("Finished creating quicklooks.");
    }

    protected void createQuicklook(Product product, String imageFileName, Mapper.Context context,
                                       Quicklooks.QLConfig config) throws IOException, InterruptedException {
        RenderedImage quicklookImage = new QuicklookGenerator(context, product, config).createImage();
        if (quicklookImage != null) {
            OutputStream outputStream = createOutputStream(context, imageFileName + "." + config.getImageType());
            OutputStream pmOutputStream = new BytesCountingOutputStream(outputStream, context);
            try {
                ImageIO.write(quicklookImage, config.getImageType(), pmOutputStream);
            } finally {
                outputStream.close();
            }
        }
    }

    private static OutputStream createOutputStream(Mapper.Context context, String fileName) throws IOException, InterruptedException {
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
        final FSDataOutputStream fsDataOutputStream = path.getFileSystem(context.getConfiguration()).create(path);
        return new BufferedOutputStream(fsDataOutputStream);
    }

    protected Map<String, Object> createSpatialSubsetParameter(Configuration jobConfig) {
        boolean hasCrsWkt = StringUtils.isNotNullAndNotEmpty(jobConfig.get(JobConfigNames.OUTPUT_CRS));

        String regionGeometry = jobConfig.get(JobConfigNames.REGION_GEOMETRY);
        boolean hasGeometry = StringUtils.isNotNullAndNotEmpty(regionGeometry);

        Map<String, Object> subsetParams = new HashMap<String, Object>();
        if (hasGeometry && hasCrsWkt) {
            // only subset a second time, if a reprojection has happened
            subsetParams.put("geoRegion", regionGeometry);
        }
        return subsetParams;
    }

    protected Map<String, Object> createBandSubsetParameter(Product targetProduct, Configuration jobConfig) {
        String outputBandList = jobConfig.get(JobConfigNames.OUTPUT_BANDS);
        boolean hasBandList = StringUtils.isNotNullAndNotEmpty(outputBandList);
        Map<String, Object> subsetParams = new HashMap<String, Object>();
        if (hasBandList) {
            String[] bandNames = outputBandList.split(",");
            StringBuilder sb = new StringBuilder();
            for (String bandName : bandNames) {
                if (targetProduct.containsBand(bandName)) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(bandName);
                } else {
                    String msgPattern = "Band '%s' shall be included in the output product but is not contained in " +
                                        "the processed product. Check if processing options are set appropriate?";
                    throw new IllegalStateException(String.format(msgPattern, bandName));
                }
            }
            subsetParams.put("bandNames", sb.toString());
        }
        return subsetParams;
    }

    protected List<Quicklooks.QLConfig> getValidQlConfigs(Configuration conf) {
        Quicklooks.QLConfig[] allQlConfigs = Quicklooks.get(conf);
        String[] bandNames = conf.getStrings(JobConfigNames.OUTPUT_BANDS);
        if (bandNames == null) {
            return Arrays.asList(allQlConfigs);
        }
        List<Quicklooks.QLConfig> qlConfigList = new ArrayList<Quicklooks.QLConfig>(bandNames.length);
        for (String bandName : bandNames) {
            Quicklooks.QLConfig qlConfig = getQuicklookConfig(allQlConfigs, bandName);
            if (qlConfig != null) {
                qlConfigList.add(qlConfig);
            }
        }
        return qlConfigList;
    }

    protected Quicklooks.QLConfig getQuicklookConfig(Quicklooks.QLConfig[] allQlConfigs, String bandName) {
        for (Quicklooks.QLConfig config : allQlConfigs) {
            if (bandName.equalsIgnoreCase(config.getBandName())) {
                return config;
            }
        }
        return null;
    }


    protected Product writeProductFile(Product targetProduct, ProductFormatter productFormatter, Mapper.Context context,
                                       Configuration jobConfig, String outputFormat, ProgressMonitor pm) throws
            IOException, InterruptedException {
        long t0 = System.currentTimeMillis();
        Map<String, Object> bandSubsetParameter = createBandSubsetParameter(targetProduct, jobConfig);
        if (!bandSubsetParameter.isEmpty()) {
            targetProduct = GPF.createProduct("Subset", bandSubsetParameter, targetProduct);
        }

        File productFile = productFormatter.createTemporaryProductFile();
        LOG.info("Start writing product to file: " + productFile.getName());

        //ProductIO.writeProduct(targetProduct, productFile, outputFormat, false, pm);
        GPF.writeProduct(targetProduct, productFile, outputFormat, false, pm);
        LOG.info("formatting done in [ms]: " + (System.currentTimeMillis() - t0));

        t0 = System.currentTimeMillis();
        context.setStatus("Copying");
        productFormatter.compressToHDFS(context, productFile);
        context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product formatted").increment(1);
        LOG.info("Formatted product " + productFile.getName() + " archived in " + FileOutputFormat.getWorkOutputPath(context));
        LOG.info("archiving done in [ms]: " + (System.currentTimeMillis() - t0));
        return targetProduct;
    }


    private static class BytesCountingOutputStream extends OutputStream {

        private static final String FILE_SYSTEM_COUNTERS = "FileSystemCounters";
        private static final String FILE_BYTES_WRITTEN = "FILE_BYTES_WRITTEN";

        private final OutputStream wrappedStream;
        private final Mapper.Context context;
        private int countedBytes;

        public BytesCountingOutputStream(OutputStream outputStream, Mapper.Context context) {
            wrappedStream = outputStream;
            this.context = context;
        }

        @Override
        public void write(int b) throws IOException {
            wrappedStream.write(b);
            maybeIncrementHadoopCounter(1);
        }

        @Override
        public void write(byte[] b) throws IOException {
            wrappedStream.write(b);
            maybeIncrementHadoopCounter(b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            wrappedStream.write(b, off, len);
            maybeIncrementHadoopCounter(len - off);
        }

        @Override
        public void close() throws IOException {
            wrappedStream.close();
            incrementHadoopCounter();
        }

        @Override
        public void flush() throws IOException {
            wrappedStream.flush();
            incrementHadoopCounter();
        }


        private void maybeIncrementHadoopCounter(int byteCount) {
            countedBytes += byteCount;
            if (countedBytes / (1024 * 10) >= 1) {
                incrementHadoopCounter();
            }
        }

        private void incrementHadoopCounter() {
            context.getCounter(FILE_SYSTEM_COUNTERS, FILE_BYTES_WRITTEN).increment(countedBytes);
            context.progress();
            countedBytes = 0;
        }
    }
}

