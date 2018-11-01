package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.Mask;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.PlainFeatureFactory;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.VectorDataNode;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;
import org.esa.snap.core.util.FeatureUtils;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    public static final double MODIS_AREA_SIZE = 53664.6683222854702276;
    private final Product[] products;
    private final Product[] lcProducts;
    private final String targetCell; // "800,312"
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final int SIZE = 4800;

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, String targetCell) {
        super(-1, -1);
        this.products = products;
        this.lcProducts = lcProducts;
        this.targetCell = targetCell;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        LOG.info("x=" + x + ", y=" + y);
        boolean isInBrokenLcZone = isInBrokenLcZone(x, y);

        double lon0 = getLeftLon(x, targetCell);
        double lat0 = getTopLat(y, targetCell);

        List<Integer> productIndices = new ArrayList<>();

        for (int i = 0; i < products.length; i++) {
            Product sourceProduct = products[i];
            Product jdSubset = getSubset(lon0, lat0, sourceProduct);
            if (jdSubset != null) {
                productIndices.add(i);
            }
        }

        if (productIndices.isEmpty()) {
           //  grid cell is covered by water completely - that's fine.
            LOG.info("Completely covered by water? x=" + x + ", y=" + y);
            return null;
        }

        int targetPixelIndex = 0;

        SourceData data = new SourceData(SIZE, SIZE);
        data.reset();

        for (Integer i : productIndices) {
            Product sourceProduct = products[i];
            Product lcProduct = lcProducts[i];

            Mask mask = addMask(lon0, lat0, sourceProduct);

            Band lc = lcProduct.getBand("lccs_class");
            Band jd = sourceProduct.getBand("classification");
            Band cl = sourceProduct.getBand("uncertainty");
            Band no = sourceProduct.getBand("numObs1");

            GeoCoding sceneGeoCoding = lcProduct.getSceneGeoCoding();
            PixelPos pp = new PixelPos();
            GeoPos gp = new GeoPos();
            for (int lineIndex = 0; lineIndex < SIZE; lineIndex++) {
                int[] maskPixels = new int[SIZE];
                mask.readPixels(0, lineIndex, SIZE, 1, maskPixels);

                boolean noMatchingPixels = Arrays.stream(maskPixels).allMatch(value -> value == 0);
                if (noMatchingPixels && !isInBrokenLcZone) {
                    continue;
                }

                int[] jdPixels = new int[SIZE];
                float[] clPixels = new float[SIZE];
                int[] lcPixels = new int[SIZE];
                int[] numObsPixels = new int[SIZE];

                jd.readPixels(0, lineIndex, SIZE, 1, jdPixels);
                if (cl != null) {
                    cl.readPixels(0, lineIndex, SIZE, 1, clPixels);
                } else {
                    Arrays.fill(clPixels, 0.0F);
                }
                no.readPixels(0, lineIndex, SIZE, 1, numObsPixels);
                lc.readPixels(0, lineIndex, SIZE, 1, lcPixels);

                for (int x0 = 0; x0 < SIZE; x0++) {
                    if (maskPixels[x0] == 0 && !isInBrokenLcZone) {
                        continue;
                    }

                    if (isInBrokenLcZone) {
                        pp.x = x0;
                        pp.y = lineIndex;
                        sceneGeoCoding.getGeoPos(pp, gp);
                        boolean pixelInsideGridCell = gp.isValid() && gp.lat <= lat0 && gp.lon > lon0 && gp.lat >= lat0 - 0.25 && gp.lon < lon0 + 0.25;
                        if (!pixelInsideGridCell) {
                            continue;
                        }
                    }

                    int sourceJD = jdPixels[x0];
                    float sourceCL = clPixels[x0];
                    int sourceLC = lcPixels[x0];
                    int numbObs = numObsPixels[x0];

                    data.burnable[targetPixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                    boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                    if (isValidPixel) {
                        // set burned pixel value consistently with CL value -- both if burned pixel is valid
                        data.burnedPixels[targetPixelIndex] = sourceJD;
                        data.probabilityOfBurn[targetPixelIndex] = sourceCL;
                    }

                    data.lcClasses[targetPixelIndex] = sourceLC;
                    if (numbObs == 0) {
                        data.statusPixels[targetPixelIndex] = 1;
                    } else {
                        data.statusPixels[targetPixelIndex] = 0;
                    }

                    data.areas[targetPixelIndex] = MODIS_AREA_SIZE;
                    targetPixelIndex++;
                }
            }
        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 4800, 4800), GridFormatUtils.make2Dims(data.burnable, 4800, 4800));
        return data;
    }

    private boolean isInBrokenLcZone(int x, int y) {
        int targetGridCellX = Integer.parseInt(targetCell.split(",")[0]) + x;
        int targetGridCellY = Integer.parseInt(targetCell.split(",")[1]) + y;
        final String pixel = targetGridCellX + " " + targetGridCellY;
        return Arrays.asList(BROKEN_LC_ZONE_PIXELS).contains(pixel);
    }

    private static Mask addMask(double lon0, double lat0, Product jd) {
        Mask mask = new Mask("mask", jd.getSceneRasterWidth(), jd.getSceneRasterHeight(), Mask.VectorDataType.INSTANCE);
        VectorDataNode vdn = createVDN(getWktString(lon0, lat0), jd);
        Mask.VectorDataType.setVectorData(mask, vdn);
        jd.getMaskGroup().add(mask);
        jd.getVectorDataGroup().add(vdn);
        vdn.setOwner(jd);
        mask.setOwner(jd);
        return mask;
    }

    private static VectorDataNode createVDN(String wkt, Product p) {
        SimpleFeatureType wktFeatureType = PlainFeatureFactory.createDefaultFeatureType(p.getSceneGeoCoding().getGeoCRS());
        ListFeatureCollection newCollection = new ListFeatureCollection(wktFeatureType);
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(wktFeatureType);
        SimpleFeature wktFeature = featureBuilder.buildFeature("ID" + Long.toHexString(0L));
        Geometry geometry;
        try {
            geometry = new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }
        wktFeature.setDefaultGeometry(geometry);
        newCollection.add(wktFeature);

        FeatureCollection<SimpleFeatureType, SimpleFeature> productFeatures = FeatureUtils.clipFeatureCollectionToProductBounds(
                newCollection,
                p,
                null,
                ProgressMonitor.NULL);
        return new VectorDataNode("geo", productFeatures);
    }

    protected static double getTopLat(int y, String tile) {
        return 90 - Integer.parseInt(tile.split(",")[1]) / 4.0 - y * 0.25;
    }

    protected static double getLeftLon(int x, String tile) {
        return -180 + Integer.parseInt(tile.split(",")[0]) / 4.0 + x * 0.25;
    }

    private Product getLcSubset(Product sourceProduct, Product lcProduct) {
        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setParameterDefaultValues();
        reprojectionOp.setSourceProduct("collocationProduct", sourceProduct);
        reprojectionOp.setSourceProduct(lcProduct);
        return reprojectionOp.getTargetProduct();
    }


    private Product getSubset(double lon0, double lat0, Product sourceProduct) {
        SubsetOp subsetOp = new SubsetOp();
        Geometry geometry;
        String polygonString;
        try {
            polygonString = getWktString(lon0, lat0);
            geometry = new WKTReader().read(polygonString);
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }

        subsetOp.setGeoRegion(geometry);
        subsetOp.setSourceProduct(sourceProduct);
        Product targetProduct = null;
        try {
            targetProduct = subsetOp.getTargetProduct();
        } catch (OperatorException exception) {
            if (exception.getMessage().contains("No intersection with source product boundary")) {
                // ignore - not all products are contained in each grid cell
                return null;
            }
        }
        if (targetProduct == null || targetProduct.getSceneRasterWidth() == 0 || targetProduct.getSceneRasterHeight() == 0) {
            return null;
        }
        return targetProduct;
    }

    private static String getWktString(double lon0, double lat0) {
        return String.format("POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))",
                lon0, lat0,
                lon0 + 0.25, lat0,
                lon0 + 0.25, lat0 - 0.25,
                lon0, lat0 - 0.25,
                lon0, lat0);
    }

    private static final String[] BROKEN_LC_ZONE_PIXELS = new String[]{"0 71", "1 71", "2 71", "3 71", "4 71", "5 71", "6 71", "7 71", "8 71", "9 71", "10 71", "10 72", "10 73", "10 74", "10 75", "10 76", "10 77", "9 77", "8 77", "7 77", "6 77", "5 77", "4 77", "3 77", "2 77", "1 77", "0 77", "0 76", "0 75", "0 74", "0 73", "0 72", "0 71", "68 74", "69 74", "70 74", "71 74", "72 74", "73 74", "74 74", "75 74", "76 74", "77 74", "78 74", "79 74", "80 74", "81 74", "82 74", "83 74", "84 74", "85 74", "86 74", "87 74", "88 74", "89 74", "90 74", "91 74", "92 74", "93 74", "94 74", "95 74", "96 74", "97 74", "98 74", "99 74", "100 74", "101 74", "102 74", "103 74", "104 74", "105 74", "106 74", "107 74", "108 74", "109 74", "110 74", "111 74", "112 74", "113 74", "114 74", "115 74", "116 74", "117 74", "118 74", "119 74", "120 74", "121 74", "122 74", "123 74", "124 74", "125 74", "126 74", "127 74", "128 74", "129 74", "130 74", "131 74", "132 74", "133 74", "134 74", "135 74", "136 74", "137 74", "138 74", "139 74", "140 74", "141 74", "142 74", "143 74", "144 74", "145 74", "146 74", "147 74", "148 74", "149 74", "149 75", "149 76", "149 77", "149 78", "149 79", "148 79", "147 79", "146 79", "145 79", "144 79", "143 79", "142 79", "141 79", "140 79", "139 79", "138 79", "137 79", "136 79", "135 79", "134 79", "133 79", "132 79", "131 79", "130 79", "129 79", "128 79", "127 79", "126 79", "125 79", "124 79", "123 79", "122 79", "121 79", "120 79", "119 79", "118 79", "117 79", "116 79", "115 79", "114 79", "113 79", "112 79", "111 79", "110 79", "109 79", "108 79", "107 79", "106 79", "105 79", "104 79", "103 79", "102 79", "101 79", "100 79", "99 79", "98 79", "97 79", "96 79", "95 79", "94 79", "93 79", "92 79", "91 79", "90 79", "89 79", "88 79", "87 79", "86 79", "85 79", "84 79", "83 79", "82 79", "81 79", "80 79", "79 79", "78 79", "77 79", "76 79", "75 79", "74 79", "73 79", "72 79", "71 79", "70 79", "69 79", "68 79", "68 78", "68 77", "68 76", "68 75", "68 74", "1193 79", "1194 79", "1195 79", "1196 79", "1197 78", "1198 78", "1199 78", "1200 78", "1201 78", "1202 78", "1203 78", "1204 77", "1205 77", "1206 77", "1207 77", "1208 77", "1209 77", "1210 77", "1211 76", "1212 76", "1213 76", "1214 76", "1215 76", "1216 76", "1217 76", "1218 75", "1219 75", "1220 75", "1221 75", "1222 75", "1223 75", "1224 75", "1225 75", "1226 74", "1227 74", "1228 74", "1229 74", "1230 74", "1231 74", "1232 74", "1233 73", "1234 73", "1235 73", "1236 73", "1237 73", "1238 73", "1239 73", "1240 72", "1241 72", "1242 72", "1243 72", "1244 72", "1245 72", "1246 72", "1247 71", "1248 71", "1249 71", "1250 71", "1251 71", "1252 71", "1253 71", "1254 70", "1255 70", "1256 70", "1257 70", "1258 70", "1259 70", "1260 70", "1261 69", "1262 69", "1263 69", "1264 69", "1265 69", "1266 69", "1267 69", "1268 68", "1269 68", "1270 68", "1271 68", "1272 68", "1273 68", "1274 68", "1275 68", "1276 67", "1277 67", "1278 67", "1279 67", "1280 67", "1281 67", "1282 67", "1283 66", "1284 66", "1285 66", "1286 66", "1287 66", "1288 66", "1289 66", "1290 65", "1291 65", "1292 65", "1293 65", "1294 65", "1295 65", "1296 66", "1297 66", "1298 66", "1299 66", "1300 66", "1301 67", "1302 67", "1303 67", "1304 67", "1305 68", "1306 68", "1307 68", "1308 68", "1309 68", "1310 69", "1311 69", "1312 69", "1313 69", "1314 69", "1315 70", "1316 70", "1317 70", "1318 70", "1319 70", "1320 71", "1321 71", "1322 71", "1323 71", "1324 72", "1325 72", "1326 72", "1327 72", "1328 72", "1329 73", "1330 73", "1331 73", "1332 73", "1333 73", "1334 74", "1335 74", "1336 74", "1337 74", "1338 75", "1339 75", "1340 75", "1341 75", "1342 75", "1343 76", "1344 76", "1345 76", "1346 76", "1347 76", "1348 77", "1349 77", "1350 77", "1351 77", "1352 77", "1353 78", "1354 78", "1355 78", "1356 78", "1357 79", "1358 79", "1359 79", "1360 79", "1361 79", "1362 80", "1363 80", "1364 80", "1363 80", "1362 80", "1361 80", "1360 80", "1359 80", "1358 80", "1357 80", "1356 80", "1355 80", "1354 80", "1353 80", "1352 80", "1351 80", "1350 80", "1349 80", "1348 80", "1347 80", "1346 80", "1345 80", "1344 80", "1343 80", "1342 80", "1341 80", "1340 80", "1339 80", "1338 80", "1337 80", "1336 80", "1335 80", "1334 80", "1333 80", "1332 80", "1331 80", "1330 80", "1329 80", "1328 80", "1327 80", "1326 80", "1325 80", "1324 80", "1323 80", "1322 80", "1321 80", "1320 80", "1319 80", "1318 80", "1317 80", "1316 80", "1315 80", "1314 80", "1313 80", "1312 80", "1311 80", "1310 80", "1309 80", "1308 80", "1307 80", "1306 80", "1305 80", "1304 80", "1303 80", "1302 80", "1301 80", "1300 80", "1299 80", "1298 80", "1297 80", "1296 80", "1295 80", "1294 80", "1293 80", "1292 80", "1291 80", "1290 80", "1289 80", "1288 80", "1287 80", "1286 80", "1285 80", "1284 80", "1283 80", "1282 80", "1281 80", "1280 80", "1279 80", "1278 79", "1277 79", "1276 79", "1275 79", "1274 79", "1273 79", "1272 79", "1271 79", "1270 79", "1269 79", "1268 79", "1267 79", "1266 79", "1265 79", "1264 79", "1263 79", "1262 79", "1261 79", "1260 79", "1259 79", "1258 79", "1257 79", "1256 79", "1255 79", "1254 79", "1253 79", "1252 79", "1251 79", "1250 79", "1249 79", "1248 79", "1247 79", "1246 79", "1245 79", "1244 79", "1243 79", "1242 79", "1241 79", "1240 79", "1239 79", "1238 79", "1237 79", "1236 79", "1235 79", "1234 79", "1233 79", "1232 79", "1231 79", "1230 79", "1229 79", "1228 79", "1227 79", "1226 79", "1225 79", "1224 79", "1223 79", "1222 79", "1221 79", "1220 79", "1219 79", "1218 79", "1217 79", "1216 79", "1215 79", "1214 79", "1213 79", "1212 79", "1211 79", "1210 79", "1209 79", "1208 79", "1207 79", "1206 79", "1205 79", "1204 79", "1203 79", "1202 79", "1201 79", "1200 79", "1199 79", "1198 79", "1197 79", "1196 79", "1195 79", "1194 79", "1193 79"};
}
