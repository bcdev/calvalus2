/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.dataio.AbstractProductReader;
import org.esa.snap.core.dataio.DecodeQualification;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.dataio.ProductIOPlugInManager;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.io.SnapFileFilter;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A reader for handing Sentinel-2 data on calvalus.
 * It unzips the products and open from the local file.
 */
public class Sentinel2CalvalusReaderPlugin implements ProductReaderPlugIn {

    private static final String FORMAT_10M = "CALVALUS-SENTINEL-2-MSI-10M";
    private static final String FORMAT_20M = "CALVALUS-SENTINEL-2-MSI-20M";
    private static final String FORMAT_60M = "CALVALUS-SENTINEL-2-MSI-60M";
    private static final String FORMAT_MULTI = "CALVALUS-SENTINEL-2-MSI-MultiRes";

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            String filename = pathConfig.getPath().getName();
            if (filename.matches("^S2.*_PRD_MSI.*")) {
                return DecodeQualification.INTENDED;
            }
        }
        return DecodeQualification.UNABLE;
    }

    @Override
    public Class[] getInputTypes() {
        return new Class[]{PathConfiguration.class};
    }

    @Override
    public ProductReader createReaderInstance() {
        return new Sentinel2CalvalusReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{FORMAT_10M, FORMAT_20M, FORMAT_60M, FORMAT_MULTI};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".none"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Sentinel-2 L1C on Calvalus";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    static class Sentinel2CalvalusReader extends AbstractProductReader {

        private static final Pattern NAME_TIME_PATTERN = Pattern.compile(".*_V([0-9]{8}T[0-9]{6})_([0-9]{8}T[0-9]{6}).*");
        private static final String DATE_FORMAT_PATTERN = "yyyyMMdd'T'HHmmss";

        Sentinel2CalvalusReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                Configuration configuration = pathConfig.getConfiguration();
                File[] unzippedFiles = CalvalusProductIO.unzipFileToLocal(pathConfig.getPath(), configuration);

                // find *SAF*xml file
                File productXML = null;
                for (File file : unzippedFiles) {
                    if (file.getName().matches(".*MTD_SAF.*xml$")) {
                        productXML = file;
                        break;
                    }
                }
                if (productXML == null) {
                    throw new IllegalFileFormatException("input has no MTD_SAF file.");
                }
                System.out.println("productXML = " + productXML);

                String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT, FORMAT_60M);
                System.out.println("inputFormat = " + inputFormat);
                String formatPrefix = inputFormat.substring("CALVALUS-".length()) + "-";
                System.out.println("formatPrefix = " + formatPrefix);

                Product product = readProduct(productXML, formatPrefix);
                if (product != null  && product.getStartTime() == null && product.getEndTime() == null) {
                    setTimeFromFilename(product, productXML.getName());
                }
                return product;
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        static void setTimeFromFilename(Product product, String filename) {
            Matcher matcher = NAME_TIME_PATTERN.matcher(filename);
            if (matcher.matches()) {
                try {
                    ProductData.UTC start = ProductData.UTC.parse(matcher.group(1), DATE_FORMAT_PATTERN);
                    ProductData.UTC end = ProductData.UTC.parse(matcher.group(2), DATE_FORMAT_PATTERN);
                    product.setStartTime(start);
                    product.setEndTime(end);
                    System.out.println("updating time: start = " + start.format() + "  end = " + end.format());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }

        private Product readProduct(File xmlFile, String formatPrefix) throws IOException {
            ProductReaderPlugIn productReaderPlugin = findProductReaderPlugin(xmlFile, formatPrefix);
            if (productReaderPlugin != null) {
                ProductReader productReader = productReaderPlugin.createReaderInstance();
                return productReader.readProductNodes(xmlFile, null);
            }
            return null;
        }

        private ProductReaderPlugIn findProductReaderPlugin(File xmlFile, String formatResolutionPrefix) {
            ProductIOPlugInManager registry = ProductIOPlugInManager.getInstance();
            Iterator<ProductReaderPlugIn> allReaderPlugIns = registry.getAllReaderPlugIns();
            while (allReaderPlugIns.hasNext()) {
                ProductReaderPlugIn readerPlugIn = allReaderPlugIns.next();
                String[] formatNames = readerPlugIn.getFormatNames();
                for (String formatName : formatNames) {
                    if (formatName.startsWith(formatResolutionPrefix)) {
                        DecodeQualification decodeQualification = readerPlugIn.getDecodeQualification(xmlFile);
                        if (decodeQualification == DecodeQualification.INTENDED) {
                            System.out.println("formatName = " + formatName);
                            return readerPlugIn;
                        }
                    }
                }
            }
            return null;
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}

