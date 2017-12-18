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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.AbstractProductReader;
import org.esa.snap.core.dataio.DecodeQualification;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.io.SnapFileFilter;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


/**
 * A reader for handling BEAM-DIMAP in zip files on calvalus.
 * It unzips the products and open from the local file.
 */
public class DimapCalvalusReaderPlugin implements ProductReaderPlugIn {

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            Path path = pathConfig.getPath();

            if (path.getName().toLowerCase().endsWith(".zip")) {
                Configuration conf = pathConfig.getConfiguration();
                try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(path.getFileSystem(conf).open(path)))) {
                    ZipEntry zipEntry = zipIn.getNextEntry(); // only look at the first zip entry
                    if (zipEntry != null) {
                        String entryName = zipEntry.getName().toLowerCase();
                        if (entryName.contains(".data/") || entryName.endsWith(".dim")) {
                            return DecodeQualification.INTENDED;
                        }
                    }
                } catch (IOException ignore) {
                    ignore.printStackTrace();
                }
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
        return new DimapCalvalusReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{"BEAM-DIMAP.zip"};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".zip"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "BEAM-DIMAP.zip on Calvalus";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    static class DimapCalvalusReader extends AbstractProductReader {

        DimapCalvalusReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                Configuration conf = pathConfig.getConfiguration();
                File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToCWD(pathConfig.getPath(), conf);

                // find *.dim file
                File productDIM = null;
                for (File file : unzippedFiles) {
                    if (file.getName().toLowerCase().endsWith(".dim")) {
                        productDIM = file;
                        break;
                    }
                }
                if (productDIM == null) {
                    throw new IllegalFileFormatException("input has no dim file.");
                }
                System.out.println("productDIM = " + productDIM);

                return ProductIO.readProduct(productDIM, "BEAM-DIMAP");
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}

