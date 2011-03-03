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

package com.bc.calvalus.processing.beam;


import com.bc.calvalus.binning.Aggregator;
import com.bc.calvalus.binning.AggregatorAverage;
import com.bc.calvalus.binning.AggregatorAverageML;
import com.bc.calvalus.binning.AggregatorMinMax;
import com.bc.calvalus.binning.AggregatorOnMaxSet;
import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinManagerImpl;
import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.BinningContextImpl;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.IsinBinningGrid;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.binning.VariableContextImpl;
import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.binding.dom.DefaultDomConverter;
import com.bc.ceres.binding.dom.DomElement;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import org.esa.beam.framework.datamodel.GeoCoding;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.ParameterDescriptorFactory;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.ProductUtils;

import java.awt.Rectangle;
import java.awt.geom.GeneralPath;
import java.awt.geom.Path2D;
import java.awt.geom.PathIterator;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static java.lang.Math.*;

public class BeamL3Config {
    static final String L3_REQUEST_FILENAME = "wps-request.xml";
    private static final String OPERATOR_PARAMETERS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.l3.parameters']/Data/ComplexData";

    public static class VariableConfiguration {
        String name;

        String expr;

        public VariableConfiguration() {
        }

        public VariableConfiguration(String name, String expr) {
            this.name = name;
            this.expr = expr;
        }

        public String getName() {
            return name;
        }

        public String getExpr() {
            return expr;
        }
    }

    public static class AggregatorConfiguration {
        String type;

        String varName;

        String[] varNames;

        Double weightCoeff;

        Double fillValue;

        public AggregatorConfiguration() {
        }

        public AggregatorConfiguration(String type, String varName, Double weightCoeff, Double fillValue) {
            this.type = type;
            this.varName = varName;
            this.weightCoeff = weightCoeff;
            this.fillValue = fillValue;
        }

        public String getType() {
            return type;
        }

        public String getVarName() {
            return varName;
        }

        public String[] getVarNames() {
            return varNames;
        }

        public Double getWeightCoeff() {
            return weightCoeff;
        }

        public Double getFillValue() {
            return fillValue;
        }
    }

    @Parameter
    int numRows;
    @Parameter
    Integer superSampling;
    @Parameter
    String bbox;
    @Parameter
    String regionWkt;
    @Parameter
    String maskExpr;
    @Parameter(itemAlias = "variable")
    VariableConfiguration[] variables;
    @Parameter(itemAlias = "aggregator")
    AggregatorConfiguration[] aggregators;


    public static BeamL3Config create(XmlDoc request) {
        try {
            DomElement parametersElement = new NodeDomElement(request.getNode(OPERATOR_PARAMETERS_XPATH));

            BeamL3Config l3Config = new BeamL3Config();
            ParameterDescriptorFactory parameterDescriptorFactory = new ParameterDescriptorFactory();
            PropertySet parameterSet = PropertyContainer.createObjectBacked(l3Config, parameterDescriptorFactory);
            DefaultDomConverter domConverter = new DefaultDomConverter(BeamL3Config.class, parameterDescriptorFactory);

            domConverter.convertDomToValue(parametersElement, parameterSet);
            return l3Config;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public float[] getSuperSamplingSteps() {
        if (superSampling == null || superSampling < 1) {
            return new float[]{0.5f};
        } else {
            float[] samplingStep = new float[superSampling];
            for (int i = 0; i < samplingStep.length; i++) {
                samplingStep[i] = (i * 2 + 1f) / (2f * superSampling);
            }
            return samplingStep;
        }
    }

    public BinningContext getBinningContext() {
        VariableContext varCtx = getVariableContext();
        return new BinningContextImpl(getBinningGrid(),
                                      varCtx,
                                      getBinManager(varCtx));
    }

    public BinningGrid getBinningGrid() {
        if (numRows == 0) {
            numRows = IsinBinningGrid.DEFAULT_NUM_ROWS;
        }
        return new IsinBinningGrid(numRows);
    }

    private BinManager getBinManager(VariableContext varCtx) {
        Aggregator[] aggs = new Aggregator[aggregators.length];
        for (int i = 0; i < aggs.length; i++) {
            String type = aggregators[i].type;
            Aggregator aggregator;
            if (type.equals("AVG")) {
                aggregator = getAggregatorAverage(varCtx, aggregators[i]);
            } else if (type.equals("AVG_ML")) {
                aggregator = getAggregatorAverageML(varCtx, aggregators[i]);
            } else if (type.equals("MIN_MAX")) {
                aggregator = getAggregatorMinMax(varCtx, aggregators[i]);
            } else if (type.equals("ON_MAX_SET")) {
                aggregator = getAggregatorOnMaxSet(varCtx, aggregators[i]);
            } else {
                throw new IllegalArgumentException("Unknown aggregator type: " + type);
            }
            aggs[i] = aggregator;
        }
        return new BinManagerImpl(aggs);
    }

    public VariableContext getVariableContext() {
        VariableContextImpl variableContext = new VariableContextImpl();
        if (maskExpr == null) {
            maskExpr = "";
        }
        variableContext.setMaskExpr(maskExpr);

        // define declared variables
        //
        if (variables != null) {
            for (VariableConfiguration variable : variables) {
                variableContext.defineVariable(variable.name, variable.expr);
            }
        }

        // define variables of all aggregators
        //
        if (aggregators != null) {
            for (AggregatorConfiguration aggregator : aggregators) {
                String varName = aggregator.varName;
                if (varName != null) {
                    variableContext.defineVariable(varName);
                } else {
                    String[] varNames = aggregator.varNames;
                    if (varNames != null) {
                        for (String varName1 : varNames) {
                            variableContext.defineVariable(varName1);
                        }
                    }
                }
            }
        }
        return variableContext;
    }

    private Aggregator getAggregatorAverage(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorAverage(varCtx, aggregatorConf.varName, aggregatorConf.weightCoeff, aggregatorConf.fillValue);
    }

    private Aggregator getAggregatorAverageML(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorAverageML(varCtx, aggregatorConf.varName, aggregatorConf.weightCoeff, aggregatorConf.fillValue);
    }

    private Aggregator getAggregatorMinMax(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorMinMax(varCtx, aggregatorConf.varName, aggregatorConf.fillValue);
    }

    private Aggregator getAggregatorOnMaxSet(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorOnMaxSet(varCtx, aggregatorConf.varNames);
    }

    public Product getPreProcessedProduct(Product source, BeamL2Config beamConfig) {
        Product product = getProductSpatialSubset(source);
        if (product == null) {
            return null;
        }
        String operatorName = beamConfig.getOperatorName();
        if (operatorName != null && !operatorName.isEmpty()) {
            Map<String, Object> parameters = Collections.EMPTY_MAP;
            try {
                parameters = beamConfig.getOperatorParameters();
            } catch (Exception ignore) {
            }
            product = GPF.createProduct(operatorName, parameters, product);
        }
        return product;
    }

    private Product getProductSpatialSubset(Product product) {
        final Geometry geoRegion = getRegionOfInterest();
        if (geoRegion == null || geoRegion.isEmpty()) {
            return product;
        }

        final Rectangle pixelRegion = computePixelRegion(product, geoRegion);
        if (pixelRegion == null || pixelRegion.isEmpty()) {
            return null;
        }

        final SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(pixelRegion);
        op.setCopyMetadata(false);
        return op.getTargetProduct();
    }

    static Rectangle computePixelRegion(Product product, Geometry geoRegion) {
        return computePixelRegion(product, geoRegion, 1);
    }

    static Rectangle computePixelRegion(Product product, Geometry geoRegion, int numBorderPixels) {
        final Geometry productGeometry = computeProductGeometry(product);
        final Geometry regionIntersection = geoRegion.intersection(productGeometry);
        if (regionIntersection.isEmpty()) {
            return null;
        }
        final PixelRegionFinder pixelRegionFinder = new PixelRegionFinder(product.getGeoCoding());
        regionIntersection.apply(pixelRegionFinder);
        final Rectangle pixelRegion = pixelRegionFinder.getPixelRegion();
        pixelRegion.grow(numBorderPixels, numBorderPixels);
        return pixelRegion.intersection(new Rectangle(product.getSceneRasterWidth(),
                                                      product.getSceneRasterHeight()));
    }

    static Geometry computeProductGeometry(Product product) {
        final GeneralPath[] paths = ProductUtils.createGeoBoundaryPaths(product);
        final Polygon[] polygons = new Polygon[paths.length];
        final GeometryFactory factory = new GeometryFactory();
        for (int i = 0; i < paths.length; i++) {
            polygons[i] = convertAwtPathToJtsPolygon(paths[i], factory);
        }
        final DouglasPeuckerSimplifier peuckerSimplifier = new DouglasPeuckerSimplifier(polygons.length == 1 ? polygons[0] : factory.createMultiPolygon(polygons));
        return peuckerSimplifier.getResultGeometry();
    }

    private static Polygon convertAwtPathToJtsPolygon(Path2D path, GeometryFactory factory) {
        final PathIterator pathIterator = path.getPathIterator(null);
        ArrayList<double[]> coordList = new ArrayList<double[]>();
        int lastOpenIndex = 0;
        while (!pathIterator.isDone()) {
            final double[] coords = new double[6];
            final int segType = pathIterator.currentSegment(coords);
            if (segType == PathIterator.SEG_CLOSE) {
                // we should only detect a single SEG_CLOSE
                coordList.add(coordList.get(lastOpenIndex));
                lastOpenIndex = coordList.size();
            } else {
                coordList.add(coords);
            }
            pathIterator.next();
        }
        final Coordinate[] coordinates = new Coordinate[coordList.size()];
        for (int i1 = 0; i1 < coordinates.length; i1++) {
            final double[] coord = coordList.get(i1);
            coordinates[i1] = new Coordinate(coord[0], coord[1]);
        }

        return factory.createPolygon(factory.createLinearRing(coordinates), null);
    }

    public Geometry getRegionOfInterest() {
        if (regionWkt == null) {
            if (bbox == null) {
                return null;
            }
            final String[] coords = bbox.split(",");
            if (coords.length != 4) {
                throw new IllegalArgumentException(MessageFormat.format("Illegal BBOX value: {0}", bbox));
            }
            String x1 = coords[0];
            String y1 = coords[1];
            String x2 = coords[2];
            String y2 = coords[3];
            regionWkt = String.format("POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))",
                                      x1, y1,
                                      x2, y1,
                                      x2, y2,
                                      x1, y2,
                                      x1, y1);
        }

        final WKTReader wktReader = new WKTReader();
        try {
            return wktReader.read(regionWkt);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            throw new IllegalArgumentException("Illegal region geometry: " + regionWkt, e);
        }
    }

    void validateConfiguration() {
        if (aggregators == null || aggregators.length == 0) {
            throw new IllegalArgumentException("No aggregator specified.");
        }
    }

    private static class PixelRegionFinder implements CoordinateFilter {
        private final GeoCoding geoCoding;
        private int x1;
        private int y1;
        private int x2;
        private int y2;

        public PixelRegionFinder(GeoCoding geoCoding) {
            this.geoCoding = geoCoding;
            x1 = Integer.MAX_VALUE;
            x2 = Integer.MIN_VALUE;
            y1 = Integer.MAX_VALUE;
            y2 = Integer.MIN_VALUE;
        }

        @Override
        public void filter(Coordinate coordinate) {
            final GeoPos geoPos = new GeoPos((float) coordinate.y, (float) coordinate.x);
            final PixelPos pixelPos = geoCoding.getPixelPos(geoPos, null);
            if (pixelPos.isValid()) {
                x1 = min(x1, (int) floor(pixelPos.x));
                x2 = max(x2, (int) ceil(pixelPos.x));
                y1 = min(y1, (int) floor(pixelPos.y));
                y2 = max(y2, (int) ceil(pixelPos.y));
            }
        }

        public Rectangle getPixelRegion() {
            return new Rectangle(x1, y1, x2 - x1 + 1, y2 - y1 + 1);
        }
    }

}
