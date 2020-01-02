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
import org.esa.snap.core.util.FeatureUtils;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    public static final double MODIS_AREA_SIZE = 53664.6683222854702276;
    private final Product[] products;
    private final Product[] lcProducts;
    private final String targetCell; // "800,312"
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final int SIZE = 4800;
    private static List<String> brokenLcPixels;

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, String targetCell) {
        super(-1, -1);
        this.products = products;
        this.lcProducts = lcProducts;
        this.targetCell = targetCell;
        initBrokenZones();
    }

    private void initBrokenZones() {
        brokenLcPixels = new ArrayList<>();
        for (int i = 1; i <= 8; i++) {
            try (ObjectInputStream in = new ObjectInputStream(getClass().getResourceAsStream("broken_pixels_" + i + ".array"))) {
                String[] currentBrokenLcPixels = (String[]) in.readObject();
                Collections.addAll(brokenLcPixels, currentBrokenLcPixels);
            } catch (IOException | ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }
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
            //  grid cell is covered by water completely - that's probably fine.
            LOG.info("Completely covered by water? x=" + x + ", y=" + y);
            return null;
        }

        SourceData data = new SourceData(SIZE, SIZE);
        data.reset();
        Arrays.fill(data.probabilityOfBurn, 255);

        for (Integer i : productIndices) {
            int targetPixelIndex = 0;
            Product sourceProduct = products[i];
            Product lcProduct = lcProducts[i];

            Mask mask = addMask(lon0, lat0, sourceProduct);

            Band lc = lcProduct.getBand("lccs_class");
            Band jd = sourceProduct.getBand("classification");
            Band cl = sourceProduct.getBand("uncertainty");
            Band no = sourceProduct.getBand("numObs1");

            GeoCoding sceneGeoCoding = sourceProduct.getSceneGeoCoding();
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

                Arrays.fill(clPixels, 255);

                jd.readPixels(0, lineIndex, SIZE, 1, jdPixels);
                if (cl != null) {
                    cl.readPixels(0, lineIndex, SIZE, 1, clPixels);
                }
                no.readPixels(0, lineIndex, SIZE, 1, numObsPixels);
                lc.readPixels(0, lineIndex, SIZE, 1, lcPixels);

                for (int x0 = 0; x0 < SIZE; x0++) {
                    if (maskPixels[x0] == 0 && !isInBrokenLcZone) {
                        targetPixelIndex++;
                        continue;
                    }

                    if (isInBrokenLcZone) {
                        pp.x = x0;
                        pp.y = lineIndex;
                        sceneGeoCoding.getGeoPos(pp, gp);
                        boolean pixelInsideGridCell = gp.isValid() && gp.lat <= lat0 && gp.lon > lon0 && gp.lat >= lat0 - 0.25 && gp.lon < lon0 + 0.25;
                        if (!pixelInsideGridCell) {
                            targetPixelIndex++;
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
                        data.burnedPixels[targetPixelIndex] = sourceJD;
                    }

                    data.probabilityOfBurn[targetPixelIndex] = sourceCL;
                    data.lcClasses[targetPixelIndex] = sourceLC;
                    if (numbObs == 0 || numbObs == 4) { // 0=observed, 3=not observed, 4=unburnable
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
        return brokenLcPixels.contains(pixel);
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

    static double getTopLat(int y, String tile) {
        return 90 - Integer.parseInt(tile.split(",")[1]) / 4.0 - y * 0.25;
    }

    static double getLeftLon(int x, String tile) {
        return -180 + Integer.parseInt(tile.split(",")[0]) / 4.0 + x * 0.25;
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

}
