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

import com.bc.ceres.core.ProgressMonitor;
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
import java.util.Iterator;
import java.util.Locale;


/**
 * A reader for handing Sentinel-2 L1C data.
 * It unzips the products and open from the local file.
 */
public class Sentinel2L1CReaderPlugin implements ProductReaderPlugIn {

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
        return new Sentinel2L1CReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{"SENTINEL2-L1C-CALVALUS"};
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

    private static class Sentinel2L1CReader extends AbstractProductReader {

        public Sentinel2L1CReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                File[] unzippedFiles = CalvalusProductIO.unzipFileToLocal(pathConfig.getPath(), pathConfig.getConfiguration());

                // find *SAF*xml file
                File productXML = null;
                for (File file : unzippedFiles) {
                    if (file.getName().matches(".*MTD_SAF.*xml$")) {
                        productXML = file;
                        break;
                    }
                }
                System.out.println("productXML = " + productXML);


//                String formatResolutionPrefix = "SENTINEL-2-MSI-10M";
//                String formatResolutionPrefix = "SENTINEL-2-MSI-20M";
                String formatResolutionPrefix = "SENTINEL-2-MSI-60M";
//                String formatResolutionPrefix = "SENTINEL-2-MSI-MultiRes";

                return readProduct(productXML, formatResolutionPrefix);
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        private Product readProduct(File xmlFile, String formatResolutionPrefix) throws IOException {
            ProductReaderPlugIn productReaderPlugin = findProductReaderPlugin(xmlFile, formatResolutionPrefix);
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

