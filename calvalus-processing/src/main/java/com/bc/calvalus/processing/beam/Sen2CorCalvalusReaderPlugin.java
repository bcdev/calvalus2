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

import com.bc.calvalus.commons.CalvalusLogger;
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
 * A reader for handling Sen2Cor data on calvalus.
 * It unzips the products and opens from the local file.
 */
public class Sen2CorCalvalusReaderPlugin implements ProductReaderPlugIn {

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            String filename = pathConfig.getPath().getName();
            if (filename.matches("^S2A.*_MSIL2A.*")) {
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
        return new Sen2CorCalvalusReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{"Sen2Cor"};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".zip"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Sentinel-2 L2A on Calvalus";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    static class Sen2CorCalvalusReader extends AbstractProductReader {

        //S2A_OPER_PRD_MSIL1C_PDMC_20161201T211507_R108_V20161201T103412_20161201T103412
        private static final Pattern NAME_TIME_PATTERN1 = Pattern.compile(".*_V([0-9]{8}T[0-9]{6})_([0-9]{8}T[0-9]{6}).*");
        //S2A_MSIL1C_20161212T100412_N0204_R122_T33UVT_20161212T100409
        private static final Pattern NAME_TIME_PATTERN2 = Pattern.compile("S2._MSIL2A_(([0-9]{8}T[0-9]{6})).*");
        private static final String DATE_FORMAT_PATTERN = "yyyyMMdd'T'HHmmss";

        Sen2CorCalvalusReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                Configuration configuration = pathConfig.getConfiguration();
                File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToCWD(pathConfig.getPath(), configuration);

                // find *MTD*xml file
                File productXML = null;
                for (File file : unzippedFiles) {
                    if (file.getName().matches("(?:^MTD|.*MTD_SAF).*xml$")) {
                        productXML = file;
                        break;
                    }
                }
                if (productXML == null) {
                    throw new IllegalFileFormatException("input has no MTD_SAF file.");
                }
                CalvalusLogger.getLogger().info("productXML file = " + productXML);

                Product product;
                product = Sentinel2CalvalusReaderPlugin.readProduct(productXML, "SENTINEL-2-MSI-MultiRes");
                if (product.getStartTime() == null && product.getEndTime() == null) {
                    setTimeFromFilename(product, productXML.getName());
                }
                return product;
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        static void setTimeFromFilename(Product product, String filename) {
            Matcher matcher = ("_MSIL2A_".equals(filename.substring(3, 11)) ? NAME_TIME_PATTERN2 : NAME_TIME_PATTERN1).matcher(filename);
            if (matcher.matches()) {
                try {
                    ProductData.UTC start = ProductData.UTC.parse(matcher.group(1), DATE_FORMAT_PATTERN);
                    ProductData.UTC end = ProductData.UTC.parse(matcher.group(2), DATE_FORMAT_PATTERN);
                    product.setStartTime(start);
                    product.setEndTime(end);
                    CalvalusLogger.getLogger().info("updating time: start = " + start.format() + "  end = " + end.format());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}

