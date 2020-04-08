package com.bc.calvalus.processing;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import ucar.ma2.Array;
import ucar.ma2.ArrayChar;
import ucar.nc2.Attribute;
import ucar.nc2.Group;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.iosp.hdf4.ODLparser;

import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Extracts start-time,end-time and bounding polygon from a MODIS L1A file
 */
public class EosRectangleCalculator {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: EosRectangleCalculator <FILE>");
            System.exit(1);
        }
        Product product = ProductIO.readProduct(new File(args[0]));
        Geometry productGeometry = determineProductGeometry(product);
        System.out.println(productGeometry.toString());
        Geometry geometry = GeometryUtils.createGeometry("POLYGON ((12 59.7, 14.45 59.7, 14.45 58.1, 12 58.1, 12 59.7))");
        Rectangle rectangle = computePixelRegion(product, geometry, 1);
        System.out.println(rectangle);
    }

    public static Rectangle computePixelRegion(Product product, Geometry geoRegion, int numBorderPixels) throws IOException {
        Geometry productGeometry = determineProductGeometry(product);
        Geometry regionIntersection = geoRegion.intersection(productGeometry);
        CalvalusLogger.getLogger().info("EOS product region: " + productGeometry);
        if (regionIntersection.isEmpty()) {
            return new Rectangle();
        } else {
            CalvalusLogger.getLogger().info("EOS product isect.: " + regionIntersection);
            Rectangle2D.Float productBox = boundingBoxOf(productGeometry, productGeometry.getCoordinates()[0]);
            Rectangle2D.Float intersectionBox = boundingBoxOf(regionIntersection, productGeometry.getCoordinates()[0]);
            float scalex = productBox.width / product.getSceneRasterWidth();
            float scaley = productBox.height / product.getSceneRasterHeight();
            return new Rectangle((int) Math.floor((intersectionBox.x - productBox.x) / scalex),
                                 (int) Math.floor((intersectionBox.y - productBox.y) / scaley),
                                 (int) Math.ceil(intersectionBox.width / scalex),
                                 (int) Math.ceil(intersectionBox.height / scaley));
        }
    }

    private static Rectangle2D.Float boundingBoxOf(Geometry region, Coordinate ref) {
        double minx = 360.0;
        double maxx = -360.0;
        double miny = 180.0;
        double maxy = -180.0;
        Coordinate[] coordinates = region.getCoordinates();
        for (Coordinate c : coordinates) {
            double cx = (c.x - ref.x + 180.0) % 360.0 - 180.0;
            double cy = c.y - ref.y;
            if (cx < minx) {
                minx = cx;
            }
            if (cx > maxx) {
                maxx = cx;
            }
            if (cy < miny) {
                miny = cy;
            }
            if (cy > maxy) {
                maxy = cy;
            }
        }
        return new Rectangle2D.Float((float) minx, (float) miny, (float) ((maxx - minx + 360.0) % 360.0), (float) (maxy - miny));
    }

    private static Geometry determineProductGeometry(Product product) throws IOException {
        File file = product.getFileLocation();
        File geoFile = new File(file.getPath().replace(".L1A_SNPP.nc", ".GEO-M_SNPP.nc"));
        if (!geoFile.exists()) {
            throw new FileNotFoundException(geoFile + " not found for geometry extraction");
        }
        String absolutePath = geoFile.getAbsolutePath();
        String name = geoFile.getName();
        NetcdfFile netcdfFile = NetcdfFile.open(absolutePath);
        try {
            if (name.startsWith("A")) {
                return analyzeModisFile(absolutePath, netcdfFile);
            } else if (name.startsWith("V")) {
                return analyzeViirsFile(absolutePath, netcdfFile);
            } else {
                throw new IllegalArgumentException("product " + product + " not MODIS nor VIIRS");
            }
        } finally {
            netcdfFile.close();
        }
    }

    private static Geometry analyzeViirsFile(String absolutePath, NetcdfFile netcdfFile) {
        //String startTime = getTime(netcdfFile, "time_coverage_start");
        //String endTime = getTime(netcdfFile, "time_coverage_end");
        String polygon = getPolygon(netcdfFile);
        return GeometryUtils.createGeometry(polygon);
    }

    private static String getPolygon(NetcdfFile netcdfFile) {
        Group navigation_data = netcdfFile.findGroup("navigation_data");
        Attribute ringPointLongitude = null;
        Attribute ringPointLatitude = null;
        Attribute ringPointSequence = null;
        if (navigation_data != null) {
            ringPointLongitude = navigation_data.findAttribute("gringpointlongitude");
            ringPointLatitude = navigation_data.findAttribute("gringpointlatitude");
            ringPointSequence = navigation_data.findAttribute("gringpointsequence");
        }
        if (ringPointLatitude == null || ringPointLongitude == null || ringPointSequence == null) {
            ringPointLongitude = netcdfFile.findGlobalAttribute("GRingPointLongitude");
            ringPointLatitude = netcdfFile.findGlobalAttribute("GRingPointLatitude");
            ringPointSequence = netcdfFile.findGlobalAttribute("GRingPointSequenceNo");
        }

        double[] lonValues = new double[4];
        for (int i = 0; i < lonValues.length; i++) {
            lonValues[i] = ringPointLongitude.getNumericValue(i).doubleValue();
        }
        double[] latValues = new double[4];
        for (int i = 0; i < latValues.length; i++) {
            latValues[i] = ringPointLatitude.getNumericValue(i).doubleValue();
        }
        int[] sequenceNo = new int[4];
        for (int i = 0; i < sequenceNo.length; i++) {
            sequenceNo[i] = ringPointSequence.getNumericValue(i).intValue();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("POLYGON((");
        for (int aSequenceNo : sequenceNo) {
            int sequenceIndex = aSequenceNo - 1;
            sb.append(lonValues[sequenceIndex]).append(" ").append(latValues[sequenceIndex]).append(",");
        }
        int sequenceIndex = sequenceNo[0] - 1;
        sb.append(lonValues[sequenceIndex]).append(" ").append(latValues[sequenceIndex]);
        sb.append("))");
        return sb.toString();

    }

    private static String getTime(NetcdfFile netcdfFile, String attributeName) {
        String startTime = netcdfFile.findGlobalAttribute(attributeName).getStringValue();
        return startTime.substring(0, startTime.length() - 1);
    }

    private static Geometry analyzeModisFile(String absolutePath, NetcdfFile netcdfFile) throws IOException {
        Group rootGroup = netcdfFile.getRootGroup();
        Element coreElem = getEosElement("CoreMetadata", rootGroup);
        Element inventoryMetadata = coreElem.getChild("INVENTORYMETADATA");
        Element masterGroup = inventoryMetadata.getChild("MASTERGROUP");
        //Element rangeDateTime = masterGroup.getChild("RANGEDATETIME");
        //String startTime = getIsoDateTime(rangeDateTime, "RANGEBEGINNINGDATE", "RANGEBEGINNINGTIME");
        //String endTime = getIsoDateTime(rangeDateTime, "RANGEENDINGDATE", "RANGEENDINGTIME");
        Element spatialDomainContainer = masterGroup.getChild("SPATIALDOMAINCONTAINER");
        Element horizontalSpatialComainContainer = spatialDomainContainer.getChild("HORIZONTALSPATIALDOMAINCONTAINER");
        Element gPolygon = horizontalSpatialComainContainer.getChild("GPOLYGON");
        Element gPolygonContainer = gPolygon.getChild("GPOLYGONCONTAINER");
        String polygon = getBoundingPolygon(gPolygonContainer);
        return GeometryUtils.createGeometry(polygon);
    }

    private static void printElement(Element element) {
        XMLOutputter fmt = new XMLOutputter(Format.getPrettyFormat());
        try {
            fmt.output(element, System.out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getBoundingPolygon(Element gPolygonContainer) {
        Element gRingPoint = gPolygonContainer.getChild("GRINGPOINT");
        double[] lonValues = getValues(gRingPoint.getChild("GRINGPOINTLONGITUDE"));
        double[] latValues = getValues(gRingPoint.getChild("GRINGPOINTLATITUDE"));
        double[] sequenceNo = getValues(gRingPoint.getChild("GRINGPOINTSEQUENCENO"));
        StringBuilder sb = new StringBuilder();
        sb.append("POLYGON((");
        for (double aSequenceNo : sequenceNo) {
            int sequenceIndex = ((int) aSequenceNo) - 1;
            sb.append(lonValues[sequenceIndex]).append(" ").append(latValues[sequenceIndex]).append(",");
        }
        int sequenceIndex = ((int) sequenceNo[0]) - 1;
        sb.append(lonValues[sequenceIndex]).append(" ").append(latValues[sequenceIndex]);
        sb.append("))");
        return sb.toString();
    }

    private static double[] getValues(Element element) {
        int numValues = Integer.parseInt(element.getChildTextTrim("NUM_VAL"));
        Element valueElement = element.getChild("VALUE");
        List<Element> children = valueElement.getChildren();
        if (children.size() != numValues) {
            String msg = String.format("Error in parsing GRINGPOINT. %d childs, but 'num_val' is %d", children.size(),
                                       numValues);
            throw new IllegalArgumentException(msg);
        }
        double[] values = new double[numValues];
        for (int i = 0; i < values.length; i++) {
            values[i] = Double.parseDouble(children.get(i).getTextTrim());
        }
        return values;
    }

    static String getIsoDateTime(Element rangeDateTimeElem, String dateName, String timeName) {
        String date = rangeDateTimeElem.getChild(dateName).getChildTextTrim("VALUE");
        String time = rangeDateTimeElem.getChild(timeName).getChildTextTrim("VALUE");
        return date + "T" + time;
    }

    static Element getEosElement(String name, Group eosGroup) throws IOException {
        String smeta = getEosMetadata(name, eosGroup);
        if (smeta == null) {
            return null;
        }
        smeta = smeta.replaceAll("\\s+=\\s+", "=");
        smeta = smeta.replaceAll("\\?", "_"); // XML names cannot contain the character "?".

        StringBuilder sb = new StringBuilder(smeta.length());
        StringTokenizer lineFinder = new StringTokenizer(smeta, "\t\n\r\f");
        while (lineFinder.hasMoreTokens()) {
            String line = lineFinder.nextToken().trim();
            sb.append(line);
            sb.append("\n");
        }

        ODLparser parser = new ODLparser();
        return parser.parseFromString(sb.toString());// now we have the ODL in JDOM elements
    }

    private static String getEosMetadata(String name, Group eosGroup) throws IOException {
        StringBuilder sbuff = null;
        String structMetadata = null;

        int n = 0;
        while (true) {
            Variable structMetadataVar = eosGroup.findVariable(name + "." + n);
            if (structMetadataVar == null) {
                break;
            }
            if ((structMetadata != null) && (sbuff == null)) { // more than 1 StructMetadata
                sbuff = new StringBuilder(64000);
                sbuff.append(structMetadata);
            }

            Array metadataArray = structMetadataVar.read();
            structMetadata = ((ArrayChar) metadataArray).getString(); // common case only StructMetadata.0, avoid extra copy

            if (sbuff != null) {
                sbuff.append(structMetadata);
            }
            n++;
        }
        return (sbuff != null) ? sbuff.toString() : structMetadata;
    }

}
