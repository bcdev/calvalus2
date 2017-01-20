package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.processing.fire.format.S2Strategy;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.File;
import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.pixel.s2.S2FinaliseMapper.TILE_SIZE;
import static org.junit.Assert.assertEquals;

public class S2FinaliseMapperTest {

    @Ignore
    @Test
    public void testRemap() throws Exception {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        Product lcProduct = ProductIO.readProduct("c:\\ssd\\2010.nc");
        lcProduct.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        String baseFilename = S2FinaliseMapper.createBaseFilename("2016", "02", "fv4.2", new S2Strategy().getArea("AREA_24"));
        Product product = S2FinaliseMapper.remap(new File("C:\\ssd\\L3_2016-02-01_2016-02-29.nc"), baseFilename, lcProduct, () -> System.out.println("S2FinaliseMapperTest.progress"));

        ProductIO.writeProduct(product, "C:\\ssd\\" + baseFilename + "_test256.tif", BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
    }

    @Test
    public void testFindNeighbourValue_1() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[56] = 20; // right next to the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width);
        assertEquals(20, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_1() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[44] = 10; // upper left of the source value
        sourceJdArray[56] = 20; // right next to the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width);
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_2() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[45] = 10; // upper center of the source value
        sourceJdArray[46] = 20; // upper right of the source value
        sourceJdArray[54] = 30; // center left of the source value
        sourceJdArray[56] = 40; // center right of the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width);
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[1] = 10; // center right of the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 0, destRect.width);
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge2() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[88] = 10; // upper left of the source value
        sourceJdArray[89] = 20; // upper center of the source value
        sourceJdArray[98] = 30; // center left of the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 99, destRect.width);
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_exc() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[55] = 23;
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width);
        assertEquals(23, neighbourValue);
    }

    @Test
    public void name() throws Exception {
        String[] areas = new String[]{
                "AREA_1(150, 90, 160, 95)",
                "AREA_2(155, 90, 160, 95)",
                "AREA_3(160, 90, 165, 95)",
                "AREA_4(165, 90, 170, 95)",
                "AREA_5(170, 90, 175, 95)",
                "AREA_6(175, 90, 180, 95)",
                "AREA_7(180, 90, 185, 95)",
                "AREA_8(185, 90, 190, 95)",
                "AREA_9(190, 90, 195, 95)",
                "AREA_10(195, 90, 200, 95)",
                "AREA_11(200, 90, 205, 95)",
                "AREA_12(205, 90, 210, 95)",
                "AREA_13(210, 90, 215, 95)",
                "AREA_14(215, 90, 220, 95)",
                "AREA_15(220, 90, 225, 95)",
                "AREA_16(225, 90, 230, 95)",
                "AREA_81(230, 90, 230, 95)",
                "AREA_17(150, 95, 155, 100)",
                "AREA_18(155, 95, 160, 100)",
                "AREA_19(160, 95, 165, 100)",
                "AREA_20(165, 95, 170, 100)",
                "AREA_21(170, 95, 175, 100)",
                "AREA_22(175, 95, 180, 100)",
                "AREA_23(180, 95, 185, 100)",
                "AREA_24(185, 95, 190, 100)",
                "AREA_25(190, 95, 195, 100)",
                "AREA_26(195, 95, 200, 100)",
                "AREA_27(200, 95, 205, 100)",
                "AREA_28(205, 95, 210, 100)",
                "AREA_29(210, 95, 215, 100)",
                "AREA_30(215, 95, 220, 100)",
                "AREA_31(220, 95, 225, 100)",
                "AREA_32(225, 95, 230, 100)",
                "AREA_82(230, 95, 235, 100)",
                "AREA_33(150, 100, 155, 105)",
                "AREA_34(155, 100, 160, 105)",
                "AREA_35(160, 100, 165, 105)",
                "AREA_36(165, 100, 170, 105)",
                "AREA_37(170, 100, 175, 105)",
                "AREA_38(175, 100, 180, 105)",
                "AREA_39(180, 100, 185, 105)",
                "AREA_40(185, 100, 190, 105)",
                "AREA_41(190, 100, 195, 105)",
                "AREA_42(195, 100, 200, 105)",
                "AREA_43(200, 100, 205, 105)",
                "AREA_44(205, 100, 210, 105)",
                "AREA_45(210, 100, 215, 105)",
                "AREA_46(215, 100, 220, 105)",
                "AREA_47(220, 100, 225, 105)",
                "AREA_48(225, 100, 230, 105)",
                "AREA_83(230, 100, 235, 105)",
                "AREA_49(150, 105, 155, 110)",
                "AREA_50(155, 105, 160, 110)",
                "AREA_51(160, 105, 165, 110)",
                "AREA_52(165, 105, 170, 110)",
                "AREA_53(170, 105, 175, 110)",
                "AREA_54(175, 105, 180, 110)",
                "AREA_55(180, 105, 185, 110)",
                "AREA_56(185, 105, 190, 110)",
                "AREA_57(190, 105, 195, 110)",
                "AREA_58(195, 105, 200, 110)",
                "AREA_59(200, 105, 205, 110)",
                "AREA_60(205, 105, 210, 110)",
                "AREA_61(210, 105, 215, 110)",
                "AREA_62(215, 105, 220, 110)",
                "AREA_63(220, 105, 225, 110)",
                "AREA_64(225, 105, 230, 110)",
                "AREA_84(230, 105, 235, 110)",
                "AREA_65(150, 110, 155, 115)",
                "AREA_66(155, 110, 160, 115)",
                "AREA_67(160, 110, 165, 115)",
                "AREA_68(165, 110, 170, 115)",
                "AREA_69(170, 110, 175, 115)",
                "AREA_70(175, 110, 180, 115)",
                "AREA_71(180, 110, 185, 115)",
                "AREA_72(185, 110, 190, 115)",
                "AREA_73(190, 110, 195, 115)",
                "AREA_74(195, 110, 200, 115)",
                "AREA_75(200, 110, 205, 115)",
                "AREA_76(205, 110, 210, 115)",
                "AREA_77(210, 110, 215, 115)",
                "AREA_78(215, 110, 220, 115)",
                "AREA_79(220, 110, 225, 115)",
                "AREA_80(225, 110, 230, 115)",
                "AREA_85(230, 110, 235, 115)",
        };
        for (String area : areas) {
            String[] splittedString = area.split(",");
            int minLon = Integer.parseInt(splittedString[0].split("\\(")[1]);
            int minLat = Integer.parseInt(splittedString[1].trim());
            int maxLon = Integer.parseInt(splittedString[2].trim());
            int maxLat = Integer.parseInt(splittedString[3].split("\\)")[0].trim());
            String name = "h" + (minLon / 5) + "v" + (minLat / 5);
            System.out.println(name + "(" + minLon + ", " + minLat + ", " + maxLon + ", " + maxLat + "),");
        }

    }

    @Test
    public void name2() throws Exception {
        int firstNumber = (int) (Math.random() * 100);
        int secondNumber = (int) (Math.random() * 100);
        double operatorRandom = Math.random();
        if (operatorRandom <= 0.25) {
            System.out.println(firstNumber + secondNumber);
        } else if (operatorRandom <= 0.5) {
            System.out.println(firstNumber + secondNumber);
        } else if (operatorRandom <= 0.5) {
            System.out.println(firstNumber - secondNumber);
        }

    }
}