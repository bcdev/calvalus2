package org.esa.beam.binning;

import com.bc.calvalus.binning.*;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.FileUtils;
import org.junit.Ignore;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

/**
 * @author Norman Fomferra
 */
@Ignore
public class TestBinner {
    public static void main(String[] args) throws Exception {
        String binnerConfigPath = args[0];
        String outputterConfigPath = args[1];
        String sourceDirPath = args[2];
        String regionWKT = "polygon((16.77 29.97, 35.80 30.00, 42.27 41.98, 39.63 47.43, 31.50 47.47, 25.95 41.74, 21.20 40.95, 13.85 46.17,  3.50 43.75, -5.50 36.20, -5.50 35.30, 16.77 29.97))";

        BinningConfig binningConfig = BinningConfig.fromXml(FileUtils.readText(new File(binnerConfigPath)));
        OutputterConfig outputterConfig = OutputterConfig.fromXml(FileUtils.readText(new File(outputterConfigPath)));
        File[] sourceFiles = new File(sourceDirPath).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".N1");
            }
        });

        BinningContext binningContext = binningConfig.createBinningContext();
        TreeMap<Long, List<SpatialBin>> spatialBinMap = doSpatialBinning(binningContext, binningConfig, sourceFiles);
        List<TemporalBin> temporalBins = doTemporalBinning(binningContext, spatialBinMap);
        Outputter.output(binningContext,
                         outputterConfig,
                         new WKTReader().read(regionWKT),
                         new ProductData.UTC(),
                         new ProductData.UTC(),
                         new MetadataElement("TODO_add_metadata_here"),
                         new MyTemporalBinSource(temporalBins));
    }

    private static TreeMap<Long, List<SpatialBin>> doSpatialBinning(BinningContext binningContext, BinningConfig binningConfig, File[] sourceFiles) throws IOException {
        final MySpatialBinProcessor spatialBinProcessor = new MySpatialBinProcessor();
        final SpatialBinner spatialBinner = new SpatialBinner(binningContext, spatialBinProcessor);
        for (File sourceFile : sourceFiles) {
            System.out.println("reading " + sourceFile);
            final Product product = ProductIO.readProduct(sourceFile);
            System.out.println("processing " + sourceFile);
            final long numObs = SpatialProductBinner.processProduct(product, spatialBinner, binningConfig.getSuperSampling(), ProgressMonitor.NULL);
            System.out.println("done, " + numObs + " observations processed");
        }
        return spatialBinProcessor.getSpatialBinMap();
    }

    private static List<TemporalBin> doTemporalBinning(BinningContext binningContext, TreeMap<Long, List<SpatialBin>> spatialBinMap) throws IOException {
        final TemporalBinner temporalBinner = new TemporalBinner(binningContext);
        final ArrayList<TemporalBin> temporalBins = new ArrayList<TemporalBin>();
        for (Map.Entry<Long, List<SpatialBin>> entry : spatialBinMap.entrySet()) {
            final TemporalBin temporalBin = temporalBinner.processSpatialBins(entry.getKey(), entry.getValue());
            temporalBins.add(temporalBin);
        }
        return temporalBins;
    }

    private static class MySpatialBinProcessor implements SpatialBinProcessor {
        final private TreeMap<Long, List<SpatialBin>> spatialBinMap = new TreeMap<Long, List<SpatialBin>>();

        public TreeMap<Long, List<SpatialBin>> getSpatialBinMap() {
            return spatialBinMap;
        }

        @Override
        public void processSpatialBinSlice(BinningContext ctx, List<SpatialBin> spatialBins) throws Exception {
            
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
