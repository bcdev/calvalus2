package org.esa.beam.binning;

import com.bc.calvalus.binning.*;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Norman Fomferra
 */
public class BinnerMain {
    public static void main(String[] args) throws Exception {
        String configPath = args[0];
        String sourceDirPath = args[1];

        BinnerConfig binnerConfig = BinnerConfig.fromXml(FileUtils.readText(new File(configPath)));
        File[] sourceFiles = new File(sourceDirPath).listFiles();

        BinnerContext binnerContext = binnerConfig.createBinningContext();

        TreeMap<Long, List<SpatialBin>> spatialBinMap = doSpatialBinning(binnerContext, binnerConfig, sourceFiles);
        List<TemporalBin> temporalBins = doTemporalBinning(binnerContext, spatialBinMap);
        doFormatting(binnerContext, temporalBins);
    }

    private static void doFormatting(BinnerContext binnerContext, List<TemporalBin> temporalBins) throws Exception {
        final BinReprojector binReprojector = new BinReprojector(binnerContext, new ProductBinRasterizer(null, null, null, null, null, null, null), new Rectangle(0, 0, 0, 0));
        binReprojector.begin();
        try {
            binReprojector.processBins(temporalBins.iterator());
        } finally {
            binReprojector.end();
        }
    }

    private static TreeMap<Long, List<SpatialBin>> doSpatialBinning(BinnerContext binnerContext, BinnerConfig binnerConfig, File[] sourceFiles) throws IOException {
        final MySpatialBinProcessor spatialBinProcessor = new MySpatialBinProcessor();
        final SpatialBinner spatialBinner = new SpatialBinner(binnerContext, spatialBinProcessor);
        for (int i = 0; i < sourceFiles.length; i++) {
            File sourceFile = sourceFiles[i];
            System.out.println("reading " + sourceFile);
            final Product product = ProductIO.readProduct(sourceFile);
            System.out.println("processing " + sourceFile);
            final long numObs = SpatialProductBinner.processProduct(product, spatialBinner, binnerConfig.getSuperSampling(), ProgressMonitor.NULL);
            System.out.println("done, " + numObs + " observations processed");
        }
        return spatialBinProcessor.getSpatialBinMap();
    }

    private static List<TemporalBin> doTemporalBinning(BinnerContext binnerContext, TreeMap<Long, List<SpatialBin>> spatialBinMap) throws IOException {
        final TemporalBinner temporalBinner = new TemporalBinner(binnerContext);
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
        public void processSpatialBinSlice(BinnerContext ctx, List<SpatialBin> spatialBins) throws Exception {
            
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
}
