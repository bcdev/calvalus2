package org.esa.beam.binning;

import com.bc.calvalus.binning.*;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.Debug;
import org.esa.beam.util.StopWatch;
import org.esa.beam.util.io.FileUtils;
import org.junit.Ignore;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

/**
 * <p>
 * Usage: <code>TestBinner <i>sourceDir</i> <i>regionWkt</i> <i>binnerConfig</i> <i>formatterConfig</i> [<i>formatterConfig</i> ...]</code>
 * </p>
 * with
 * <ul>
 * <li><code><i>sourceDir</i></code> Directory containing input product files</li>
 * <li><code><i>regionWkt</i></code> File with region geometry WKT, e.g. "POLYGON((1 47,27 47,27 33,1 33,1 47))"</li>
 * <li><code><i>binnerConfig</i></code> File with binning configuration XML (see /org/esa/beam/binning/BinningConfigTest.xml)</li>
 * <li><code><i>formatterConfig</i></code> File with formatter configuration XML (see /org/esa/beam/binning/FormatterConfigTest.xml)</li>
 * </ul>
 *
 * The test demonstrates the usage of various binning API classes such as
 * <ul>
 * <li>{@link SpatialBinner}</li>
 * <li>{@link TemporalBinner}</li>
 * <li>{@link Formatter}</li>
 * </ul>
 *
 *
 * @author Norman Fomferra
 */
@Ignore
public class TestBinner {
    public static void main(String[] args) throws Exception {

        String sourceDirFile = args[0];
        String regionWktFile = args[1];
        String binnerConfigFile = args[2];
        String[] outputterConfigFiles = new String[args.length - 3];
        System.arraycopy(args, 3, outputterConfigFiles, 0, outputterConfigFiles.length);

        File[] sourceFiles = new File(sourceDirFile).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".N1");
            }
        });
        String regionWkt = FileUtils.readText(new File(regionWktFile));
        BinningConfig binningConfig = BinningConfig.fromXml(FileUtils.readText(new File(binnerConfigFile)));

        Debug.setEnabled(true);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        BinningContext binningContext = binningConfig.createBinningContext();
        // Step 1: Spatial binning - creates time-series of spatial bins for each bin ID ordered by ID. The tree map structure is <ID, time-series>
        SortedMap<Long, List<SpatialBin>> spatialBinMap = doSpatialBinning(binningContext, binningConfig, sourceFiles);
        // Step 2: Temporal binning - creates a list of temporal bins, sorted by bin ID
        List<TemporalBin> temporalBins = doTemporalBinning(binningContext, spatialBinMap);
        // Step 3: Formatting
        for (String outputterConfigFile : outputterConfigFiles) {
            FormatterConfig formatterConfig = FormatterConfig.fromXml(FileUtils.readText(new File(outputterConfigFile)));
            doOutputting(regionWkt, formatterConfig, binningContext, temporalBins);
        }

        stopWatch.stopAndTrace(String.format("Total time for binning %d product(s)", sourceFiles.length));
    }

    private static SortedMap<Long, List<SpatialBin>> doSpatialBinning(BinningContext binningContext, BinningConfig binningConfig, File[] sourceFiles) throws IOException {
        final SpatialBinStore spatialBinStore = new SpatialBinStore();
        final SpatialBinner spatialBinner = new SpatialBinner(binningContext, spatialBinStore);
        for (File sourceFile : sourceFiles) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            System.out.println("reading " + sourceFile);
            final Product product = ProductIO.readProduct(sourceFile);
            System.out.println("processing " + sourceFile);
            final long numObs = SpatialProductBinner.processProduct(product, spatialBinner, binningConfig.getSuperSampling(), ProgressMonitor.NULL);
            System.out.println("done, " + numObs + " observations processed");

            stopWatch.stopAndTrace("Spatial binning of product took");
        }
        return spatialBinStore.getSpatialBinMap();
    }

    private static List<TemporalBin> doTemporalBinning(BinningContext binningContext, SortedMap<Long, List<SpatialBin>> spatialBinMap) throws IOException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        final TemporalBinner temporalBinner = new TemporalBinner(binningContext);
        final ArrayList<TemporalBin> temporalBins = new ArrayList<TemporalBin>();
        for (Map.Entry<Long, List<SpatialBin>> entry : spatialBinMap.entrySet()) {
            final TemporalBin temporalBin = temporalBinner.processSpatialBins(entry.getKey(), entry.getValue());
            temporalBins.add(temporalBin);
        }

        stopWatch.stopAndTrace("Temporal binning took");

        return temporalBins;
    }

    private static void doOutputting(String regionWKT, FormatterConfig formatterConfig, BinningContext binningContext, List<TemporalBin> temporalBins) throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Formatter.format(binningContext,
                         new MyTemporalBinSource(temporalBins), formatterConfig,
                         new WKTReader().read(regionWKT),
                         new ProductData.UTC(),
                         new ProductData.UTC(),
                         new MetadataElement("TODO_add_metadata_here")
        );

        stopWatch.stopAndTrace("Writing output took");
    }

    private static class SpatialBinStore implements SpatialBinConsumer {
        // Note, we use a sorted map in order to sort entries on-the-fly
        final private SortedMap<Long, List<SpatialBin>> spatialBinMap = new TreeMap<Long, List<SpatialBin>>();

        public SortedMap<Long, List<SpatialBin>> getSpatialBinMap() {
            return spatialBinMap;
        }

        @Override
        public void consumeSpatialBins(BinningContext binningContext, List<SpatialBin> spatialBins) {

            for (SpatialBin spatialBin : spatialBins) {
                List<SpatialBin> spatialBinList = spatialBinMap.get(spatialBin.getIndex());
                if (spatialBinList == null) {
                    spatialBinList = new ArrayList<SpatialBin>();
                    spatialBinMap.put(spatialBin.getIndex(), spatialBinList);
                }
                spatialBinList.add(spatialBin);
            }
        }
    }

    private static class MyTemporalBinSource implements TemporalBinSource {
        private final List<TemporalBin> temporalBins;

        public MyTemporalBinSource(List<TemporalBin> temporalBins) {
            this.temporalBins = temporalBins;
        }

        @Override
        public int open() throws IOException {
            return 1;
        }

        @Override
        public Iterator<? extends TemporalBin> getPart(int index) throws IOException {
            return temporalBins.iterator();
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    }
}