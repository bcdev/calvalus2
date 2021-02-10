/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.analysis;

import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.glayer.Layer;
import com.bc.ceres.glayer.LayerContext;
import com.bc.ceres.glayer.LayerType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.FeatureUtils;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Fill;
import org.geotools.styling.LineSymbolizer;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.PolygonSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.SLD;
import org.geotools.styling.Stroke;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.FilterFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.Color;
import java.io.File;

class ShapefileLoader {

    private static final StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory(null);
    private static final FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory(null);


    public Layer createLayer(Product product, File shapeFile, LayerContext layerContext) throws Exception {

        final Geometry clipGeometry = FeatureUtils.createGeoBoundaryPolygon(product);

        FeatureCollection<SimpleFeatureType, SimpleFeature> featureCollection = FeatureUtils.getFeatureSource(shapeFile.toURI().toURL()).getFeatures();
        CoordinateReferenceSystem featureCrs = featureCollection.getSchema().getCoordinateReferenceSystem();

        Style[] styles = createStyle(shapeFile, featureCollection.getSchema());
        Style selectedStyle = styles[0];

        final LayerType type = new FeatureLayerType();
        final PropertySet configuration = type.createLayerConfig(null);
        configuration.setValue(FeatureLayerType.PROPERTY_NAME_FEATURE_COLLECTION_URL, shapeFile.toURI().toURL());
        configuration.setValue(FeatureLayerType.PROPERTY_NAME_FEATURE_COLLECTION, featureCollection);
        configuration.setValue(FeatureLayerType.PROPERTY_NAME_FEATURE_COLLECTION_CRS, featureCrs);
        configuration.setValue(FeatureLayerType.PROPERTY_NAME_FEATURE_COLLECTION_CLIP_GEOMETRY, clipGeometry);
        configuration.setValue(FeatureLayerType.PROPERTY_NAME_SLD_STYLE, selectedStyle);
        Layer featureLayer = type.createLayer(layerContext, configuration);
        featureLayer.setName(shapeFile.getName());
        featureLayer.setVisible(true);
        return featureLayer;
    }

    private static Style[] createStyle(File shapeFile, FeatureType schema) {
        final Style[] styles = SLDUtils.loadSLD(shapeFile);
        if (styles != null && styles.length > 0) {
            return styles;
        }
        Class<?> type = schema.getGeometryDescriptor().getType().getBinding();
        if (type.isAssignableFrom(Polygon.class)
                || type.isAssignableFrom(MultiPolygon.class)) {
            return new Style[]{createPolygonStyle()};
        } else if (type.isAssignableFrom(LineString.class)
                || type.isAssignableFrom(MultiLineString.class)) {
            return new Style[]{createLineStyle()};
        } else {
            return new Style[]{createPointStyle()};
        }
    }

    private static Style createPointStyle() {
        PointSymbolizer symbolizer = styleFactory.createPointSymbolizer();
        symbolizer.getGraphic().setSize(filterFactory.literal(1));

        Rule rule = styleFactory.createRule();
        rule.symbolizers().add(symbolizer);
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle();
        fts.rules().add(rule);

        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        return style;
    }

    private static Style createLineStyle() {
        LineSymbolizer symbolizer = styleFactory.createLineSymbolizer();
        SLD.setLineColour(symbolizer, Color.BLUE);
        symbolizer.getStroke().setWidth(filterFactory.literal(1));
        symbolizer.getStroke().setColor(filterFactory.literal(Color.BLUE));

        Rule rule = styleFactory.createRule();
        rule.symbolizers().add(symbolizer);
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle();
        fts.rules().add(rule);

        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        return style;
    }

    private static Style createPolygonStyle() {
        PolygonSymbolizer symbolizer = styleFactory.createPolygonSymbolizer();
        Fill fill = styleFactory.createFill(
                filterFactory.literal("#FFAA00"),
                filterFactory.literal(0.5)
        );
        final Stroke stroke = styleFactory.createStroke(filterFactory.literal(Color.BLACK),
                                                        filterFactory.literal(1));
        symbolizer.setFill(fill);
        symbolizer.setStroke(stroke);
        Rule rule = styleFactory.createRule();
        rule.symbolizers().add(symbolizer);
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle();
        fts.rules().add(rule);

        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        return style;
    }
}
