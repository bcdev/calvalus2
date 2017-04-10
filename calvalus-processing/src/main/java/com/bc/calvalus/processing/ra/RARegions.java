/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.VirtualDir;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.util.FeatureUtils;
import org.esa.snap.core.util.io.FileUtils;
import org.geotools.data.crs.ReprojectFeatureResults;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.index.CloseableIterator;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Converting an ESRI shapefile into a stream of regions.
 */
public class RARegions {

    public static FeatureCollection<SimpleFeatureType, SimpleFeature> openShapefile(Path path, File tempDir, Configuration conf) throws IOException {
        File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToDir(path, tempDir, conf);
        System.out.println("unzippedFiles = " + Arrays.toString(unzippedFiles));
        // find *.dim file
        File shpFile = null;
        File prjFile = null;
        for (File file : unzippedFiles) {
            if (file.getName().toLowerCase().endsWith(".shp")) {
                shpFile = file;
            } else if (file.getName().toLowerCase().endsWith(".prj")) {
                prjFile = file;
            }
        }
        if (shpFile == null) {
            throw new IllegalFileFormatException("input has no <shp> file.");
        }
        if (prjFile != null) {
            mayFixPrjFile(prjFile);
        }
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = FeatureUtils.loadFeatureCollectionFromShapefile(shpFile);
        try {
            return new ReprojectFeatureResults(features, DefaultGeographicCRS.WGS84);
        } catch (SchemaException | TransformException | FactoryException e) {
            throw new IOException("Could not reproject shapefile", e);
        }
    }

    private static void mayFixPrjFile(File prjFile) throws IOException {
        // read existing PRJ file
        Charset cs = Charset.forName("ISO-8859-1");
        List<String> strings = Files.readAllLines(prjFile.toPath(), cs);
        String originalWKT = String.join("", strings);
        String newWKT = removeMetadataFromWkt(originalWKT);
        // write back PRJ file
        List<String> lines = new ArrayList<>();
        lines.add(newWKT);
        Files.write(prjFile.toPath(), lines, cs);
    }

    static String removeMetadataFromWkt(String originalWKT) {
        // remove ",METADATA[...]"
        return originalWKT.replaceAll(",\\s*METADATA\\s*\\[[^\\]]+\\]", "");
    }

    public static FeatureCollection<SimpleFeatureType, SimpleFeature> filterCollection(
            FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection,
            String attributeName,
            String[] attributeValues) {


        Filter filter = Filter.INCLUDE;
        if (attributeName != null && !attributeName.isEmpty() &&
                attributeValues != null && attributeValues.length > 0) {

            filter = new Filter() {
                final Set<String> allowedValues = new HashSet<>(Arrays.asList(attributeValues));

                @Override
                public boolean evaluate(Object object) {
                    if (!(object instanceof SimpleFeature)) {
                        return false;
                    }
                    SimpleFeature feature = (SimpleFeature) object;
                    Object attribute = feature.getAttribute(attributeName);
                    return attribute instanceof String && allowedValues.contains(attribute);
                }

                @Override
                public Object accept(FilterVisitor visitor, Object extraData) {
                    return extraData;
                }
            };
        }
        return featureCollection.subCollection(filter);
    }

    static NameProvider createNameProvider(SimpleFeatureType schema, String attributeName) {
        if (attributeName != null) {
            // try to use given attribute
            AttributeDescriptor descriptor = schema.getDescriptor(attributeName);
            if (descriptor != null) {
                if (descriptor.getType().getBinding() == String.class) {
                    return new AttributeNameProvider(attributeName);
                }
            }
            // try to use first "String" attribute
            for (AttributeDescriptor attributeDescriptor : schema.getAttributeDescriptors()) {
                if (attributeDescriptor.getType().getBinding() == String.class) {
                    return new AttributeNameProvider(attributeName);
                }
            }
        }
        // use index
        return new OrdinalNameProvider();
    }

    static interface NameProvider {
        String getName(SimpleFeature simpleFeature);
    }

    static class OrdinalNameProvider implements NameProvider {

        private int counter = 0;

        @Override
        public String getName(SimpleFeature simpleFeature) {
            return Integer.toString(counter++);
        }
    }

    static class AttributeNameProvider implements NameProvider {

        private final String attributeName;

        AttributeNameProvider(String attributeName) {
            this.attributeName = attributeName;
        }

        @Override
        public String getName(SimpleFeature simpleFeature) {
            return (String) simpleFeature.getAttribute(attributeName);
        }
    }

    static class RegionIteratorFromShapefile implements CloseableIterator<RAConfig.NamedGeometry> {
        private final FeatureIterator<SimpleFeature> featureIterator;
        private final NameProvider nameProvider;
        private final File tempDir;

        RegionIteratorFromShapefile(String shapeFilePath, String filterAttributeName, String filterAttributeValues, Configuration conf) throws IOException {
            tempDir = VirtualDir.createUniqueTempDir();
            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = RARegions.openShapefile(new Path(shapeFilePath), tempDir, conf);
            String[] attributeValues = new String[0];
            if (filterAttributeValues != null) {
                String[] split = filterAttributeValues.split(",");
                attributeValues = new String[split.length];
                for (int i = 0; i < split.length; i++) {
                    attributeValues[i] = split[i].trim();
                }
            }
            collection = filterCollection(collection, filterAttributeName, attributeValues);
            this.featureIterator = collection.features();
            this.nameProvider = createNameProvider(collection.getSchema(), filterAttributeName);
        }

        @Override
        public boolean hasNext() {
            return featureIterator.hasNext();
        }

        @Override
        public RAConfig.NamedGeometry next() {
            SimpleFeature simpleFeature = featureIterator.next();
            Geometry defaultGeometry = (Geometry) simpleFeature.getDefaultGeometry();
            String name = nameProvider.getName(simpleFeature);
            return new RAConfig.NamedGeometry(name, defaultGeometry);
        }

        @Override
        public void close() throws IOException {
            if (tempDir != null) {
                FileUtils.deleteTree(tempDir);
            }
        }
    }

    static class RegionIteratorFromWKT implements CloseableIterator<RAConfig.NamedGeometry> {

        private final Iterator<RAConfig.Region> iterator;

        RegionIteratorFromWKT(RAConfig.Region[] regions) {
            iterator = Arrays.asList(regions).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public RAConfig.NamedGeometry next() {
            RAConfig.Region region = iterator.next();
            String name = region.getName();
            Geometry geometry = GeometryUtils.createGeometry(region.getWkt());
            return new RAConfig.NamedGeometry(name, geometry);
        }

        @Override
        public void close() throws IOException {
            // nothing here.
        }
    }

    public static void main(String[] args) throws IOException, SchemaException, FactoryException, TransformException {
        File shpFile = new File(args[0]);
        File prjFile = new File(shpFile.getParent(), shpFile.getName().replace(".shp", ".prj"));
        mayFixPrjFile(prjFile);
        
        FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = FeatureUtils.loadFeatureCollectionFromShapefile(shpFile);
        FeatureCollection<SimpleFeatureType, SimpleFeature> reprojected = new ReprojectFeatureResults(featureCollection, DefaultGeographicCRS.WGS84);

        int size = reprojected.size();
        System.out.println("size = " + size);
        SimpleFeatureType schema = reprojected.getSchema();
        System.out.println("schema = " + schema);
        System.out.println("schema.crs = " + schema.getCoordinateReferenceSystem());
        System.out.println();

        List<AttributeDescriptor> attributeDescriptors = schema.getAttributeDescriptors();
        for (AttributeDescriptor attributeDescriptor : attributeDescriptors) {
            System.out.println(" " + attributeDescriptor);
        }
        System.out.println();

        FeatureIterator<SimpleFeature> features = reprojected.features();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            System.out.println("feature = " + feature);
        }
    }
}
