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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.AbstractProductReader;
import org.esa.snap.core.dataio.DecodeQualification;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.io.SnapFileFilter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * A reader to read metadata of an xcube.
 * It provides it as a dummy product with metadata only, in particular the attribute timeDimension.
 */
public class ZarrZmetadataReaderPlugin implements ProductReaderPlugIn {

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration
                && ".zmetadata".equals(((PathConfiguration)input).getPath().getName()))  {
            return DecodeQualification.INTENDED;
        }
        return DecodeQualification.UNABLE;
    }

    @Override
    public Class[] getInputTypes() {
        return new Class[] { PathConfiguration.class };
    }

    @Override
    public ProductReader createReaderInstance() {
        return new ZarrZmetadataReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{"Zarr-Zmetadata"};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".zmetadata"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Reader for metadata of an xcube provided as dummy product";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null;
    }

    static class ZarrZmetadataReader extends AbstractProductReader {

        private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF =
                new TypeReference<Map<String, Object>>() {};

        ZarrZmetadataReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (!(input instanceof PathConfiguration)) {
                throw new IllegalFileFormatException("input of type " + input.getClass() +
                                                             ", PathConfiguration expected");
            }
            final PathConfiguration pathConfig = (PathConfiguration) input;
            final Configuration conf = pathConfig.getConfiguration();
            final Path path = pathConfig.getPath();
            final String name = path.getParent().getName();
            final FileSystem fs = path.getFileSystem(conf);
            final BufferedInputStream in = new BufferedInputStream(fs.open(path));

            final ObjectMapper jsonParser = new ObjectMapper();
            jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            final Map<String, Object> zmetadata = jsonParser.readValue(in, VALUE_TYPE_REF);

            final int[] shape = findShape(zmetadata, path);

            final Product product = new Product(name, "ZarrZmetadata", shape[1], shape[2]);
            product.getMetadataRoot().setAttributeInt("timeDimension", shape[0]);
            return product;
        }

        int[] findShape(Map<String, Object> zmetadata, Path path) {
            Map<String, Object> metadata = (Map<String, Object>) zmetadata.get("metadata");
            for (Map.Entry<String, Object> i: metadata.entrySet()) {
                if (i.getKey().endsWith("/.zarray")) {
                    Object zarray = i.getValue();
                    if (zarray instanceof Map) {
                        for (Map.Entry<?, ?> j: ((Map<?, ?>) zarray).entrySet()) {
                            if ("shape".equals(j.getKey())) {
                                if (j.getValue() instanceof List) {
                                    List shape = (List) j.getValue();
                                    if (shape.size() == 3) {
                                        return new int[] {
                                                Integer.parseInt(String.valueOf(shape.get(0))),
                                                Integer.parseInt(String.valueOf(shape.get(1))),
                                                Integer.parseInt(String.valueOf(shape.get(2)))
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
            }
            throw new NoSuchElementException("cannot find shape with three elements in " + path);
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}

