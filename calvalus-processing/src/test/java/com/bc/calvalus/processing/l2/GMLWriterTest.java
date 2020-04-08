package com.bc.calvalus.processing.l2;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * @author Marco Peters
 */
public class GMLWriterTest {

    private final static double[] COORDS = new double[]{
            15.130960464477539, 59.974773406982421, 15.6457309722900391, 59.973258972167971,
            16.1604099273681641, 59.96973800659181, 16.6749248504638671, 59.964210510253906,
            17.18920326232911, 59.95668029785156, 17.7031745910644531, 59.947147369384766,
            18.2167644500732421, 59.93561553955078, 18.7299022674560551, 59.92208480834961,
            19.2425155639648441, 59.90656661987305, 19.273254394531251, 59.90557098388672,
            19.2404327392578121, 59.64824676513672, 19.2081928253173831, 59.39090347290039,
            19.1765251159667971, 59.13353729248047, 19.1454105377197271, 58.87615203857422,
            19.1282234191894531, 58.73200988769531, 19.0985202789306641, 58.73295593261719,
            18.6032066345214841, 58.747772216796875, 18.1074237823486331, 58.76068878173828,
            17.6112365722656251, 58.77169418334961, 17.114711761474611, 58.78079605102539,
            16.6179103851318361, 58.78798294067383, 16.120897293090821, 58.79325866699219,
            15.6237401962280271, 58.7966194152832, 15.1265001296997071, 58.798065185546875,
            15.1270284652709961, 58.94258499145508, 15.1279859542846681, 59.200645446777344,
            15.1289596557617191, 59.45869827270508, 15.1299514770507811, 59.716739654541016,
            15.1309604644775391, 59.97477340698242
    };

    @Test
    public void testResultDoesNotContainUselessWhitespaces() throws Exception {
        // to many white-spaces break the display of the geometry in a GeoServer
        GeometryFactory gf = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[COORDS.length / 2];
        for (int i = 0; i < coordinates.length; i++) {
            double lon = COORDS[i * 2];
            double lat = COORDS[(i * 2) + 1];
            coordinates[i] = new Coordinate(lon, lat);
        }
        Polygon geometry = gf.createPolygon(gf.createLinearRing(coordinates), null);
        String result = L2Mapper.getGML(geometry);

        Pattern pattern = Pattern.compile("\\s{2,}"); // 2 or more succeeding white-spaces
        Matcher matcher = pattern.matcher(result);
        assertFalse(matcher.find());
    }
}
