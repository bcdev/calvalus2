package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.processing.fire.format.pixel.s2.JDAggregator;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;
import org.esa.snap.binning.CompositingType;
import org.esa.snap.binning.operator.BinningOp;
import org.esa.snap.binning.operator.VariableConfig;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2StrategyTest {

    @Test
    public void testGetInputDatePattern() throws Exception {
        assertEquals("201512", S2Strategy.getInputDatePattern(2015, 12));
        assertEquals("201601", S2Strategy.getInputDatePattern(2016, 1));
        assertEquals("201511", S2Strategy.getInputDatePattern(2015, 11));
    }

    @Ignore
    @Test
    public void testAggregation() throws Exception {
        Product product1 = ProductIO.readProduct("C:\\ssd\\BA-T33PTK-20160204T093257.nc");
        Product product2 = ProductIO.readProduct("C:\\ssd\\BA-T33PTK-20160207T094351.nc");
        Product product3 = ProductIO.readProduct("C:\\ssd\\BA-T33PTK-20160217T093845.nc");
        Product product4 = ProductIO.readProduct("C:\\ssd\\BA-T33PTK-20160214T093550.nc");

        BinningOp binningOp = new BinningOp();
        binningOp.setMaskExpr("true");
        binningOp.setAggregatorConfigs(new JDAggregator.Config("JD", "CL", 2016, 2));
        binningOp.setSourceProducts(product1, product2, product3, product4);
        binningOp.setNumRows(1001878);
        binningOp.setSuperSampling(1);
        binningOp.setCompositingType(CompositingType.BINNING);
        binningOp.setPlanetaryGridClass("org.esa.snap.binning.support.PlateCarreeGrid");
        binningOp.setOutputFile("C:\\ssd\\binningtest-comp.nc");
        binningOp.setOutputFormat("NetCDF4-CF");
        binningOp.setOutputTargetProduct(true);
        binningOp.setMetadataAggregatorName("NAME");
        binningOp.setOutputType("Product");
        VariableConfig doyConfig = new VariableConfig("day_of_year", "JD");
        VariableConfig clConfig = new VariableConfig("confidence_level", "CL");
        binningOp.setVariableConfigs(doyConfig, clConfig);
        GeometryFactory gf = new GeometryFactory();
        PixelProductArea pixelProductArea = new S2Strategy().getArea("AREA_24");
//        Geometry regionGeometry = new Polygon(new LinearRing(new PackedCoordinateSequence.Float(new double[]{
//                pixelProductArea.left - 180, pixelProductArea.top - 90,
//                pixelProductArea.right - 180, pixelProductArea.top - 90,
//                pixelProductArea.right - 180, pixelProductArea.bottom - 90,
//                pixelProductArea.left - 180, pixelProductArea.bottom - 90,
//                pixelProductArea.left - 180, pixelProductArea.top - 90
//        }, 2), gf), new LinearRing[0], gf);
        Geometry regionGeometry = new Polygon(new LinearRing(new PackedCoordinateSequence.Float(new double[]{
                12.2, 9.05,
                13.2, 9.05,
                13.2, 8,
                12.2, 8,
                12.2, 9.05
        }, 2), gf), new LinearRing[0], gf);
        binningOp.setRegion(regionGeometry);
        binningOp.getTargetProduct();

//        ProductIO.writeProduct(binningOp.getTargetProduct(), "C:\\ssd\\binningtest.nc", "NetCDF-CF");
    }
}