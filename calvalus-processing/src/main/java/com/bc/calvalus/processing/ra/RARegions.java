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
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.util.FeatureUtils;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Converting an ESRI shapefile into a stream of regions.
 */
public class RARegions {

    public static FeatureCollection<SimpleFeatureType, SimpleFeature> openShapefile(Path path, Configuration conf) throws IOException {
        File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToLocalDir(path, conf);
        // find *.dim file
        File shpFile = null;
        for (File file : unzippedFiles) {
            if (file.getName().toLowerCase().endsWith(".shp")) {
                shpFile = file;
                break;
            }
        }
        if (shpFile == null) {
            throw new IllegalFileFormatException("input has no <shp> file.");
        }
        return FeatureUtils.loadFeatureCollectionFromShapefile(shpFile);
    }

    public static Iterator<NamedRegion> createIterator(FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection,
                                                       String attributeName,
                                                       String[] attributeValues) {

        NameProvider nameProvider = createNameProvider(featureCollection.getSchema(), attributeName);
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
        return new RegionIteratorFromFeatures(featureCollection.subCollection(filter), nameProvider);
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

    static class RegionIteratorFromFeatures implements Iterator<NamedRegion> {
        private final FeatureIterator<SimpleFeature> featureIterator;
        private final NameProvider nameProvider;

        RegionIteratorFromFeatures(FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection, NameProvider nameProvider) {
            this.featureIterator = featureCollection.features();
            this.nameProvider = nameProvider;
        }

        @Override
        public boolean hasNext() {
            return featureIterator.hasNext();
        }

        @Override
        public NamedRegion next() {
            SimpleFeature simpleFeature = featureIterator.next();
            Geometry defaultGeometry = (Geometry) simpleFeature.getDefaultGeometry();
            String name = nameProvider.getName(simpleFeature);
            return new NamedRegion(name, defaultGeometry);
        }
    }

    static class RegionIteratorFromWKT implements Iterator<NamedRegion> {

        private final Iterator<RAConfig.Region> iterator;

        RegionIteratorFromWKT(RAConfig.Region[] regions) {
            iterator = Arrays.asList(regions).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public NamedRegion next() {
            RAConfig.Region region = iterator.next();
            String name = region.getName();
            Geometry geometry = GeometryUtils.createGeometry(region.getWkt());
            return new NamedRegion(name, geometry);
        }
    }

    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = FeatureUtils.loadFeatureCollectionFromShapefile(file);

        int size = featureCollection.size();
        System.out.println("size = " + size);
        SimpleFeatureType schema = featureCollection.getSchema();
        System.out.println("schema = " + schema);
        System.out.println();

        List<AttributeDescriptor> attributeDescriptors = schema.getAttributeDescriptors();
        for (AttributeDescriptor attributeDescriptor : attributeDescriptors) {
            System.out.println(" " + attributeDescriptor);
        }
        System.out.println();

        String[] attributeValues = {"SEA-001", "SEA-002"};
        String attributeName = "HELCOM_ID";

        Iterator<NamedRegion> regionIterator = createIterator(featureCollection, attributeName, attributeValues);
        while (regionIterator.hasNext()) {
            NamedRegion next = regionIterator.next();
            System.out.println("next.name = " + next.name);
        }
    }
}
