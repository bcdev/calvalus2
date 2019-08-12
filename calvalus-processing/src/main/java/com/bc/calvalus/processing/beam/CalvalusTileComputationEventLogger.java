package com.bc.calvalus.processing.beam;

import com.sun.media.jai.util.SunTileCache;
import org.esa.snap.core.gpf.internal.OperatorImage;
import org.esa.snap.core.gpf.monitor.TileComputationEvent;
import org.esa.snap.core.gpf.monitor.TileComputationObserver;
import org.esa.snap.core.image.RasterDataNodeOpImage;
import org.esa.snap.core.image.VirtualBandOpImage;

import javax.media.jai.CachedTile;
import javax.media.jai.JAI;
import javax.media.jai.TileCache;
import java.awt.image.RenderedImage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusTileComputationEventLogger extends TileComputationObserver {

    private static class TileEvent {
        private final OperatorImage image;
        private final int tileX;
        private final int tileY;
        private final double duration;

        TileEvent(TileComputationEvent event) {
            this.image = event.getImage();
            this.tileX = event.getTileX();
            this.tileY = event.getTileY();
            this.duration = nanosToRoundedSecs((event.getEndNanos() - event.getStartNanos()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TileEvent that = (TileEvent) o;

            if (tileX != that.tileX) return false;
            if (tileY != that.tileY) return false;
            if (image != that.image) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = image.hashCode();
            result = 31 * result + tileX;
            result = 31 * result + tileY;
            return result;
        }

        @Override
        public String toString() {
            return String.format("%s tile=%d,%d (%d,%d) time=%f",
                                 image, tileY, tileX, image.getTileHeight(), image.getTileWidth(), duration);
        }

        private static double nanosToRoundedSecs(long nanos) {
            double secs = nanos * 1.0E-9;
            return Math.round(1000.0 * secs) / 1000.0;
        }
    }

    private final Set<TileEvent> recordedEventSet = new HashSet<TileEvent>();

    @Override
    public void start() {
        getLogger().log(Level.INFO, "Starting TileComputationPrinter");
    }

    int noOfPrintedReports = 0;

    @Override
    public void tileComputed(TileComputationEvent event) {
        TileEvent tileEvent = new TileEvent(event);
        String message = tileEvent.toString();
        boolean newEvent = false;
        synchronized (recordedEventSet) {
            if (!recordedEventSet.contains(tileEvent)) {
                recordedEventSet.add(tileEvent);
                newEvent = true;
            }
        }
        if (tileEvent.image.getTileCache() instanceof SunTileCache) {
            message += " cache=" + ((SunTileCache)tileEvent.image.getTileCache()).getCacheTileCount()
                     + " (" + (((SunTileCache)tileEvent.image.getTileCache()).getCacheMemoryUsed() / 1024 / 1024)
                     + "/" + (((SunTileCache)tileEvent.image.getTileCache()).getMemoryCapacity() / 1024 / 1024)
                     + ")";
        }
        if (newEvent) {
            getLogger().log(Level.INFO, "Tile computed: " + message);
        } else {
            getLogger().log(Level.WARNING, "Tile re-computed: " + message);
        }
        if (noOfPrintedReports < 2 || !newEvent) {
            printTileCacheReport();
            ++noOfPrintedReports;
        }
    }

    void printTileCacheReport() {
        TileCache tileCache = JAI.getDefaultInstance().getTileCache();
        if (tileCache instanceof SunTileCache) {
            SunTileCache sunTileCache = (SunTileCache) tileCache;
            long cacheTileCount = sunTileCache.getCacheTileCount();
            System.out.println("Tiles in Cache = " + cacheTileCount);
            long memUsed = sunTileCache.getCacheMemoryUsed() / (1024 * 1024);
            long memCapacity = sunTileCache.getMemoryCapacity() / (1024 * 1024);
            System.out.printf("Memory used %d MB (capacity %d MB)%n", memUsed, memCapacity);
            printCachedTiles(((Hashtable) sunTileCache.getCachedObject()).values());
        }
    }

    private static void printCachedTiles(Collection<CachedTile> tiles) {
        try {
            final Map<String, Long> numTiles = new HashMap<>(100);
            final Map<String, Long> sizeTiles = new HashMap<>(100);
            for (CachedTile sct : tiles) {
                RenderedImage owner = sct.getOwner();
                if (owner == null) {
                    continue;
                }
                String name = owner.getClass().getSimpleName() + " " + getImageComment(owner);
                increment(numTiles, name, 1);
                increment(sizeTiles, name, sct.getTileSize());
            }
            List<Map.Entry<String, Long>> sortedBySize = new ArrayList<>(sizeTiles.entrySet());
            Collections.sort(sortedBySize, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));
            for (Map.Entry<String, Long> entry : sortedBySize) {
                String name = entry.getKey();
                Long sizeBytes = entry.getValue();
                Long tileCount = numTiles.get(name);

                System.out.printf("size=%8.2fMB  ", (sizeBytes / (1024.0 * 1024.0)));
                System.out.printf("#tiles=%5d   ", tileCount);
                System.out.print("(" + name + ")  ");
                System.out.println();
            }
        } catch (ConcurrentModificationException _) {
            System.out.println("*** Cannot print tile cache content due to concurrent modifiations");
        }
    }

    private static String getImageComment(RenderedImage image) {
        if (image instanceof RasterDataNodeOpImage) {
            RasterDataNodeOpImage rdnoi = (RasterDataNodeOpImage) image;
            return rdnoi.getRasterDataNode().getName();
        } else if (image instanceof VirtualBandOpImage) {
            VirtualBandOpImage vboi = (VirtualBandOpImage) image;
            return vboi.getExpression();
        } else if (image instanceof OperatorImage) {
            final String s = image.toString();
            final int p1 = s.indexOf('[');
            final int p2 = s.indexOf(']', p1 + 1);
            if (p1 > 0 && p2 > p1) {
                return s.substring(p1 + 1, p2 - 1);
            }
            return s;
        } else {
            return "";
        }
    }

    private static void increment(Map<String, Long> numImages, String name, long amount) {
        Long count = numImages.get(name);
        if (count == null) {
            numImages.put(name, amount);
        } else {
            numImages.put(name, count.intValue() + amount);
        }
    }

    @Override
    public void stop() {
        recordedEventSet.clear();
        getLogger().log(Level.INFO, "Stopping TileComputationPrinter");
    }
}
