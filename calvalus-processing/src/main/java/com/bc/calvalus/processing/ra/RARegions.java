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
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.util.FeatureUtils;
import org.esa.snap.core.util.io.FileUtils;
import org.geotools.data.crs.ReprojectFeatureResults;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

/**
 * Converting an ESRI shapefile into a stream of regions.
 */
public class RARegions {

    private static final String REGION_INDEX = "region_index";

    public static FeatureCollection<SimpleFeatureType, SimpleFeature> openShapefile(Path path, File tempDir, Configuration conf) throws IOException {
        File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToDir(path, tempDir, conf);
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

    private static NameProvider createNameProvider(SimpleFeatureType schema, String attributeName) {
        if (attributeName != null && !attributeName.equals(REGION_INDEX)) {
            // use given attribute name
            AttributeDescriptor descriptor = schema.getDescriptor(attributeName);
            if (descriptor != null) {
                //if (descriptor.getType().getBinding() == String.class) {
                    return new AttributeNameProvider(attributeName);
                //}
            }
        }
        // use index
        return new OrdinalNameProvider();
    }

    interface NameProvider {
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
            return String.valueOf(simpleFeature.getAttribute(attributeName));
        }
    }
    
    public interface RegionIterator extends Iterator<RAConfig.NamedRegion>, AutoCloseable {

        boolean hasNext();

        RAConfig.NamedRegion next();

        void close() throws IOException;
    }

    public static class FilterRegionIterator implements RegionIterator {

        private final RegionIterator delegate;
        private final Pattern[] patterns;
        private RAConfig.NamedRegion nextElement;
        private boolean hasNext;

        public FilterRegionIterator(RegionIterator regionIterator, String filter) {
            this.delegate = regionIterator;
            if (filter != null && !filter.isEmpty()) {
                String[] filters = filter.split(",");
                patterns = new Pattern[filters.length];
                for (int i = 0; i < filters.length; i++) {
                    patterns[i] = Pattern.compile(filters[i].trim());
                }
            } else {
                patterns = null;
            }
            nextMatch();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public RAConfig.NamedRegion next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }
            return nextMatch();        
        }

        private RAConfig.NamedRegion nextMatch() {
            RAConfig.NamedRegion oldMatch = nextElement;
    
            while (delegate.hasNext()) {
                RAConfig.NamedRegion nextCandidate = delegate.next();
                if (accept(nextCandidate)) {
                    hasNext = true;
                    nextElement = nextCandidate;
                    return oldMatch;
                }
            }
            hasNext = false;
            return oldMatch;
        }

        private boolean accept(RAConfig.NamedRegion nextCandidate) {
            String regionName = nextCandidate.name;
            if (patterns == null) {
                return true;
            }
            for (Pattern pattern : patterns) {
                if (pattern.matcher(regionName).find()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            delegate.close();   
        }
    }
    
    static class RegionIteratorFromShapefile implements RegionIterator {
        private final FeatureIterator<SimpleFeature> featureIterator;
        private final NameProvider nameProvider;
        private final File tempDir;

        RegionIteratorFromShapefile(String shapeFilePath, String filterAttributeName, Configuration conf) throws IOException {
            tempDir = VirtualDir.createUniqueTempDir();
            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = RARegions.openShapefile(new Path(shapeFilePath), tempDir, conf);
            this.featureIterator = collection.features();
            this.nameProvider = createNameProvider(collection.getSchema(), filterAttributeName);
        }

        @Override
        public boolean hasNext() {
            return featureIterator.hasNext();
        }

        @Override
        public RAConfig.NamedRegion next() {
            SimpleFeature simpleFeature = featureIterator.next();
            Geometry defaultGeometry = (Geometry) simpleFeature.getDefaultGeometry();
            String name = nameProvider.getName(simpleFeature);
            return new RAConfig.NamedRegion(name, defaultGeometry);
        }

        @Override
        public void close() throws IOException {
            if (tempDir != null) {
                FileUtils.deleteTree(tempDir);
            }
        }
    }

    static class RegionIteratorFromWKT implements RegionIterator {

        private final Iterator<RAConfig.Region> iterator;

        RegionIteratorFromWKT(RAConfig.Region[] regions) {
            iterator = Arrays.asList(regions).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public RAConfig.NamedRegion next() {
            RAConfig.Region region = iterator.next();
            String name = region.getName();
            Geometry geometry = GeometryUtils.createGeometry(region.getWkt());
            return new RAConfig.NamedRegion(name, geometry);
        }

        @Override
        public void close() throws IOException {
            // nothing here.
        }
    }
    
    public static String[][] loadStringAttributes(String shapeFilePath, Configuration conf) throws IOException {
        File tempDir = VirtualDir.createUniqueTempDir();
        try {
            return loadStringAttributes(RARegions.openShapefile(new Path(shapeFilePath), tempDir, conf));
        } finally {
            FileUtils.deleteTree(tempDir);
        }
    }

    private static String[][] loadStringAttributes(FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection) {
        SimpleFeatureType schema = featureCollection.getSchema();
        List<String> attributeNames = new ArrayList<>();
        for (AttributeDescriptor attributeDescriptor : schema.getAttributeDescriptors()) {
            Class<?> binding = attributeDescriptor.getType().getBinding();
            if (binding.equals(String.class)) {
                attributeNames.add(attributeDescriptor.getLocalName());
            }
        }
        List<String[]> entries = new ArrayList<>();
        if (attributeNames.isEmpty()) {
            entries.add(new String[]{REGION_INDEX});
            FeatureIterator<SimpleFeature> features = featureCollection.features();
            int index = 0;
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                entries.add(new String[]{Integer.toString(index)});
                index++;
            }            
        } else {
            entries.add(attributeNames.toArray(new String[0]));
            FeatureIterator<SimpleFeature> features = featureCollection.features();
            while (features.hasNext()) {
                List<String> attributeValues = new ArrayList<>();
                SimpleFeature feature = features.next();
                for (String name : attributeNames) {
                    attributeValues.add((String) feature.getAttribute(name));
                }
                entries.add(attributeValues.toArray(new String[0]));
            }
        }
        return entries.toArray(new String[0][]);
    }

    public static void main(String[] args) throws IOException, SchemaException, FactoryException, TransformException {
        File shpFile = new File(args[0]);
        File prjFile = new File(shpFile.getParent(), shpFile.getName().replace(".shp", ".prj"));
        mayFixPrjFile(prjFile);
        
        FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = FeatureUtils.loadFeatureCollectionFromShapefile(shpFile);
//        FeatureCollection<SimpleFeatureType, SimpleFeature> reprojected = new ReprojectFeatureResults(featureCollection, DefaultGeographicCRS.WGS84);
        String[][] strings = loadStringAttributes(featureCollection);
        int size = 0;
        for (String[] string : strings) {
            for (String s : string) {
                size += s.length();
            }
        }
        System.out.println("size = " + size);

        SimpleFeatureType schema = featureCollection.getSchema();
        System.out.println("schema = " + schema);
        System.out.println("schema.crs = " + schema.getCoordinateReferenceSystem());
        System.out.println();

        List<AttributeDescriptor> attributeDescriptors = schema.getAttributeDescriptors();
        List<String> stringAttrDesc = new ArrayList<>();
        for (AttributeDescriptor attributeDescriptor : attributeDescriptors) {
            System.out.println("attributeDescriptor = " + attributeDescriptor);
            Class<?> binding = attributeDescriptor.getType().getBinding();
            if (binding.equals(String.class)) {
                stringAttrDesc.add(attributeDescriptor.getLocalName());
            }
        }
        System.out.println();
        
        FeatureIterator<SimpleFeature> features = featureCollection.features();
        int counter = 0;
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            System.out.println("feature = " + feature);
            StringBuilder sb = new StringBuilder();
            for (String name : stringAttrDesc) {
                Object attribute = feature.getAttribute(name);
                sb.append((String) attribute).append(", ");
            }
//            for (Object attribute : feature.getAttributes()) {
//                if (attribute instanceof String) {
//                    sb.append((String) attribute).append(", ");
//                }else if (attribute instanceof Number) {
//                    sb.append((Number) attribute).append(", ");
//                }
//            }
            System.out.println(sb);
//            System.out.println("feature = " + feature);
            counter++;
        }
        System.out.println("#features = " + counter);
    }
}
