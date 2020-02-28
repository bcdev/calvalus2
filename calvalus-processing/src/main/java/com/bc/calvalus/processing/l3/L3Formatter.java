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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.beam.GpfUtils;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.binding.Converter;
import com.bc.ceres.binding.ConverterRegistry;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.TemporalBinSource;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.Formatter;
import org.esa.snap.binning.operator.FormatterConfig;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.runtime.Engine;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * For formatting the results of a SNAP Level 3 Hadoop Job.
 */
public class L3Formatter {

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();


    private final ProductData.UTC startTime;
    private final ProductData.UTC endTime;
    private final Configuration configuration;
    private final PlanetaryGrid planetaryGrid;
    private final String[] featureNames;
    private final MetadataSerializer metadataSerializer;
    private final BinningConfig binningConfig;
    private FormatterConfig formatterConfig;


    private L3Formatter(String dateStart, String dateStop, String outputFile, String outputFormat, Configuration conf) throws BindingException {
        binningConfig = HadoopBinManager.getBinningConfig(conf);
        planetaryGrid = binningConfig.createPlanetaryGrid();
        this.startTime = parseTime(dateStart);
        this.endTime = parseTime(dateStop);
        this.configuration = conf;

        featureNames = conf.getStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES);
        String formatterXML = conf.get(JobConfigNames.CALVALUS_L3_FORMAT_PARAMETERS);
        if (formatterXML != null) {
            formatterConfig = FormatterConfig.fromXml(formatterXML);
        } else {
            formatterConfig = new FormatterConfig();
        }
        formatterConfig.setOutputType("Product");
        formatterConfig.setOutputFile(outputFile);
        formatterConfig.setOutputFormat(outputFormat);

        metadataSerializer = new MetadataSerializer();
    }

    private void format(TemporalBinSource temporalBinSource, String regionName, String regionWKT) throws Exception {
        Geometry regionGeometry = GeometryUtils.createGeometry(regionWKT);
        final String processingHistoryXml = configuration.get(JobConfigNames.PROCESSING_HISTORY);
        final MetadataElement processingGraphMetadata = metadataSerializer.fromXml(processingHistoryXml);
        // TODO maybe replace region information in metadata if overwritten in formatting request
        Formatter.format(planetaryGrid,
                temporalBinSource,
                featureNames,
                formatterConfig,
                regionGeometry,
                startTime,
                endTime,
                processingGraphMetadata);
    }

    private static ProductData.UTC parseTime(String timeString) {
        try {
            return ProductData.UTC.parse(timeString, "yyyy-MM-dd");
        } catch (ParseException e) {
            throw new IllegalArgumentException("Illegal date format.", e);
        }
    }

    public static void write(TaskInputOutputContext context, TemporalBinSource temporalBinSource,
                             String dateStart, String dateStop,
                             String regionName, String regionWKT,
                             String productName ) throws IOException {

        Configuration conf = context.getConfiguration();
        GpfUtils.init(conf);
        Engine.start();  // required here!  L3Formatter has no ProcessorAdapter
        CalvalusLogger.restoreCalvalusLogFormatter();
        ConverterRegistry.getInstance().setConverter(Product.class, new ProductConverter(conf));
        String format = conf.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
        String compression = conf.get(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, null);
        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        ProductFormatter productFormatter;
        if ("org.esa.snap.binning.support.IsinPlanetaryGrid".equals(binningConfig.getPlanetaryGrid())){
            productFormatter = new ProductFormatter(productName,  "dir",null);
        } else {
            productFormatter = new ProductFormatter(productName, format, compression);
        }
        try {
            File productFile = productFormatter.createTemporaryProductFile();

            L3Formatter formatter = new L3Formatter(dateStart, dateStop,
                                                    productFile.getAbsolutePath(),
                                                    productFormatter.getOutputFormat(),
                                                    conf);
            LOG.info("Start formatting product to file: " + productFile.getName());
            context.setStatus("formatting");
            formatter.format(temporalBinSource, regionName, regionWKT);

            LOG.info("Finished formatting product.");
            context.setStatus("copying");

            if ("org.esa.snap.binning.support.IsinPlanetaryGrid".equals(binningConfig.getPlanetaryGrid())) {
                //For the case of IsinPlanetaryGrid the whole directory has to be copied.
                productFormatter.compressToHDFS(context, productFile.getParentFile());
            } else {
                productFormatter.compressToHDFS(context, productFile);
            }
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product formatted").increment(1);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Formatting failed.", e);
            throw new IOException(e);
        } finally {
            productFormatter.cleanupTempDir();
            context.setStatus("");
        }
    }

    private static class ProductConverter implements Converter<Product> {

        private final Configuration conf;

        private ProductConverter(Configuration conf) {
            this.conf = conf;
        }

        @Override
         public Class<? extends Product> getValueType() {
             return Product.class;
         }

         @Override
         public Product parse(String text) throws ConversionException {
             Path path = new Path(text);
             try {
                 return CalvalusProductIO.readProduct(path, conf, null);
             } catch (IOException e) {
                 throw new ConversionException(e);
             }
         }

         @Override
         public String format(Product value) {
             throw new IllegalStateException();
         }
     }

}
