package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.image.SingleBandedOpImage;
import org.geotools.geometry.jts.LiteCoordinateSequence;

import javax.media.jai.PlanarImage;
import java.awt.Dimension;
import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class JdImage extends SingleBandedOpImage {

    private final Band sourceJdBand;
    private final Band watermask;
    private final GeoCoding gc;
    private final Map<String, Geometry> tilesMap;
    private final String year;
    private final String month;
    private final Configuration conf;
    private final HdfsInventoryService inventoryService;

    JdImage(Band sourceJdBand, Band watermask, GeoCoding gc, Configuration conf) {
        super(DataBuffer.TYPE_INT, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(S2FinaliseMapper.TILE_SIZE, S2FinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceJdBand = sourceJdBand;
        this.watermask = watermask;
        this.gc = gc;
        year = conf.get("calvalus.year");
        month = conf.get("calvalus.month");
        this.conf = conf;
        Properties tiles = new Properties();
        try {
            tiles.load(getClass().getResourceAsStream("bounding_boxes.properties"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        tilesMap = new HashMap<>();
        Enumeration<?> tileNames = tiles.propertyNames();
        while (tileNames.hasMoreElements()) {
            String tile = tileNames.nextElement().toString();
            String boundingBoxString = tiles.getProperty(tile);
            WKTReader wkt = new WKTReader();
            Geometry boundingBox;
            try {
                boundingBox = wkt.read(boundingBoxString);
            } catch (ParseException e) {
                throw new IllegalStateException(e);
            }
            tilesMap.put(tile, boundingBox);
        }
        inventoryService = new HdfsInventoryService(conf, "projects");
    }

    @Override
    protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
        float[] sourceJdArray = new float[destRect.width * destRect.height];
        byte[] watermaskArray = new byte[destRect.width * destRect.height];
        try {
            sourceJdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(sourceJdArray));
            watermask.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Byte(watermaskArray));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        int pixelIndex = 0;
        PixelPos pixelPos = new PixelPos();
        GeoPos geoPos = new GeoPos();
        GeometryFactory factory = new GeometryFactory();
        for (int y = destRect.y; y < destRect.y + destRect.height; y++)
            for (int x = destRect.x; x < destRect.x + destRect.width; x++) {
                pixelPos.x = x;
                pixelPos.y = y;
                byte watermask = watermaskArray[pixelIndex];

                if (watermask > 0) {
                    dest.setSample(x, y, 0, -2);
                    pixelIndex++;
                    continue;
                }

                float sourceJd = sourceJdArray[pixelIndex];
                if (Float.isNaN(sourceJd)) {
                    sourceJd = S2FinaliseMapper.findNeighbourValue(sourceJdArray, pixelIndex, destRect.width).neighbourValue;
                }

                int targetJd;
                if (sourceJd < 900) {
                    targetJd = (int) sourceJd;
                } else {
                    targetJd = -1;
                }


                if (targetJd == -1) {
                    String tile = findTile(pixelPos, geoPos, factory);
                    if (exists(tile)) {
                        targetJd = 0;
                    }
                }
                dest.setSample(x, y, 0, targetJd);

                pixelIndex++;
            }
    }

    private String findTile(PixelPos pixelPos, GeoPos geoPos, GeometryFactory factory) {
        String tile = null;
        for (Map.Entry<String, Geometry> entry : tilesMap.entrySet()) {
            gc.getGeoPos(pixelPos, geoPos);
            Point point = new Point(new LiteCoordinateSequence(new double[]{geoPos.lon, geoPos.lat}), factory);
            if (entry.getValue().contains(point)) {
                tile = entry.getKey();
                System.out.println("Pixel in tile '" + tile);
                break;
            }
        }

        if (tile == null) {
            throw new IllegalStateException("No tile found for pixel " + pixelPos.toString());
        }
        return tile;
    }

    private boolean exists(String tile) {
        String pathPattern = String.format("hdfs://calvalus/calvalus/projects/fire/s2-ba/%s/BA-%s-%s%s.*nc", tile, tile, year, month);
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(pathPattern);
        FileStatus[] fileStatuses;
        try {
            fileStatuses = inventoryService.globFileStatuses(inputPatterns, conf);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return fileStatuses.length > 0;
    }

}
