package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.commons.jxpath.xml.DOMParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.esa.snap.collocation.CollocateOp;
import org.esa.snap.collocation.ResamplingType;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class S2PixelFinaliseMapper extends PixelFinaliseMapper {

    static final int NOT_OBSERVED_RESULT = -1;
    static final int NOT_BURNABLE_RESULT = -2;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        nanHandler = new S2NanHandler(context.getConfiguration());
        super.run(context);
    }

    @Override
    protected Product collocateWithSource(Product lcProduct, Product source) {
        CollocateOp collocateOp = new CollocateOp();
        collocateOp.setMasterProduct(source);
        collocateOp.setSlaveProduct(lcProduct);
        collocateOp.setResamplingType(ResamplingType.NEAREST_NEIGHBOUR);

        return collocateOp.getTargetProduct();
    }

    @Override
    protected ClScaler getClScaler() {
        return cl -> cl * 100;
    }

    static class S2NanHandler implements NanHandler {

        private final Configuration conf;

        public S2NanHandler(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public PositionAndValue handleNaN(float[] sourceJdArray, int[] lcArray, int pixelIndex, int width, int imageType, GeoPos geoPos, String month) {
            PositionAndValue neighbourValue = findNeighbourValue(sourceJdArray, lcArray, pixelIndex, width, true, true);
            if (Float.isNaN(neighbourValue.value)) {
                // case if the whole tile has no BA image. At this point we know that the pixel cannot have burned.
                int currentSen2CorValue = -1;
                String[] tiles = getTilesForGeoPos(geoPos);
                for (String tile : tiles) {
                    Product[] sen2CorProducts;
                    try {
                        sen2CorProducts = fetchSen2CorProducts(tile, month);
                        for (Product sen2CorProduct : sen2CorProducts) {
                            int sen2CorValue = getValueOfPos(geoPos, sen2CorProduct, imageType);
                            currentSen2CorValue = preferredValue(sen2CorValue, currentSen2CorValue, imageType);
                        }
                    } catch (IOException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
                return new PositionAndValue(pixelIndex, currentSen2CorValue);
            } else {
                return neighbourValue;
            }
        }

        static int preferredValue(int one, int other, int imageType) {
            if (one < -2 || one > 1 || other < -2 || other > 1) {
                throw new IllegalArgumentException("one < -2 || one > 1 || other < -2 || other > 1");
            }
            switch (imageType) {
                case JD:
                    if (one == 0 || other == 0) {
                        return 0;
                    } else if (one == NOT_BURNABLE_RESULT || other == NOT_BURNABLE_RESULT) {
                        return NOT_BURNABLE_RESULT;
                    } else if (one == NOT_OBSERVED_RESULT && other == NOT_OBSERVED_RESULT) {
                        return NOT_OBSERVED_RESULT;
                    }
                case CL:
                    if (one == 1 || other == 1) {
                        return 1;
                    } else if (one == 0 && other == 0) {
                        return 0;
                    }
                case LC:
                    return 0;
            }
            throw new IllegalStateException();
        }

        private int getValueOfPos(GeoPos geoPos, Product sen2CorProduct, int imageType) {
            final int NOT_OBSERVED = 0;
            final int CLOUD_MED = 8;
            final int CLOUD_HIGH = 9;
            final int THIN_CIRRUS = 10;
            final int NOT_BURNABLE = 6;
            PixelPos pixelPos = sen2CorProduct.getSceneGeoCoding().getPixelPos(geoPos, null);
            int v = (int) ProductUtils.getGeophysicalSampleAsLong(sen2CorProduct.getBand("quality_scene_classification"), (int) pixelPos.x, (int) pixelPos.y, 0);
            switch (imageType) {
                case JD:
                    if (v == NOT_OBSERVED || v == CLOUD_MED || v == CLOUD_HIGH || v == THIN_CIRRUS) {
                        return NOT_OBSERVED_RESULT;
                    } else if (v == NOT_BURNABLE) {
                        return NOT_BURNABLE_RESULT;
                    } else {
                        return 0;
                    }
                case CL:
                    if (v == NOT_OBSERVED || v == CLOUD_MED || v == CLOUD_HIGH || v == THIN_CIRRUS || v == NOT_BURNABLE) {
                        return 0;
                    } else {
                        return 1; // lowest possible value in burnable, non-burned areas.
                    }
                case LC:
                    return 0;
                default:
                    throw new IllegalStateException("Unknown image type: " + imageType);
            }
        }

        private Product[] fetchSen2CorProducts(String tile, String month) throws IOException {
            List<Product> products = new ArrayList<>();
            String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/s2-pre/2016/" + month + "/.*" + tile + ".*tif";
            HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(conf, "eodata");
            InputPathResolver inputPathResolver = new InputPathResolver();
            List<String> inputPatterns = inputPathResolver.resolve(inputPathPattern);
            FileStatus[] fileStatuses = hdfsInventoryService.globFileStatuses(inputPatterns, conf);
            for (FileStatus fileStatus : fileStatuses) {
                File localFile = new File(".", fileStatus.getPath().getName());
                File file;
                if (!localFile.exists()) {
                    file = CalvalusProductIO.copyFileToLocal(fileStatus.getPath(), conf);
                } else {
                    file = localFile;
                }
                products.add(ProductIO.readProduct(file));
            }
            return products.toArray(new Product[0]);
        }

        String[] getTilesForGeoPos(GeoPos geoPos) {
            List<String> tiles = new ArrayList<>();

            Document xml = (Document) new DOMParser().parseXML(getClass().getResourceAsStream("s2-tiles.kml"));
            NodeList placemarks = xml.getElementsByTagNameNS("http://www.opengis.net/kml/2.2", "Placemark");
            for (int i = 0; i < placemarks.getLength(); i++) {
                Node placemark = placemarks.item(i);
                for (int j = 0; j < placemark.getChildNodes().getLength(); j++) {
                    Node childNode = placemark.getChildNodes().item(j);
                    if (childNode.getNodeName().equals("MultiGeometry")) {
                        Node polygonNode = childNode.getChildNodes().item(1);
                        Node outerBoundaryNode = polygonNode.getChildNodes().item(1);
                        Node linearRingNode = outerBoundaryNode.getChildNodes().item(1);
                        Node coordinatesNode = linearRingNode.getChildNodes().item(1);
                        Geometry geometry;
                        Geometry point;
                        try {
                            String polygon = coordinatesNode.getTextContent()
                                    .replace(",0 ", "| ")
                                    .replace(",", " ")
                                    .replace("| ", ", ")
                                    .replace("|", "")
                                    .replaceFirst(" 0$", "");
                            geometry = new WKTReader().read("POLYGON((" + polygon + "))");
                            point = new WKTReader().read(String.format("POINT(%s %s)", geoPos.lon, geoPos.lat));
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        if (geometry.contains(point)) {
                            tiles.add(placemark.getChildNodes().item(1).getTextContent());
                        }
                    }
                }
            }
            tiles.sort(String::compareTo);
            return tiles.toArray(new String[0]);
        }
    }
}
