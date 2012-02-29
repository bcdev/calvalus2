package org.esa.beam.binning;

import com.bc.calvalus.binning.*;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorException;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.SourceProducts;
import org.esa.beam.framework.gpf.annotations.TargetProduct;
import org.esa.beam.util.Debug;
import org.esa.beam.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * An operator that is used to perform spatial and temporal aggregations into "bin" cells for any number of source
 * product. The output is either a file comprising the resulting bins or a reprojected "map" of the bin cells
 * represented by a usual data product.
 * <p/>
 * Unlike most other operators, that can compute single {@link org.esa.beam.framework.gpf.Tile tiles},
 * the binning operator processes all
 * of its source products in its {@link #initialize()} method.
 *
 * @author Norman Fomferra
 * @author Thomas Storm
 */
public class BinningOp extends Operator {

    @SourceProducts(count = -1, description = "The source products to be binned. Must be all of the same structure.")
    Product[] sourceProducts;

    @TargetProduct
    Product targetProduct;

    @Parameter(description = "The considered geographical region as a geometry in well-known text format (WKT). If not given, the Globe is considered.")
    String regionWkt;

    @Parameter(notNull = true, description = "The start date.", format = "YYYY-mm-DD")
    String startDate;

    @Parameter(notNull = true, description = "The end date.", format = "YYYY-mm-DD")
    String endDate;

    @Parameter(notNull = true,
               description = "The configuration used for the binning process. Specifies the binning grid, any variables and their aggregators.")
    BinningConfig binningConfig;

    @Parameter(notNull = true,
               description = "The configuration used for the output formatting process.")
    FormatterConfig formatterConfig;

    public BinningConfig getBinningConfig() {
        return binningConfig;
    }

    public void setBinningConfig(BinningConfig binningConfig) {
        this.binningConfig = binningConfig;
    }

    public FormatterConfig getFormatterConfig() {
        return formatterConfig;
    }

    public void setFormatterConfig(FormatterConfig formatterConfig) {
        this.formatterConfig = formatterConfig;
    }

    /**
     * Processes all source products and writes the output file.
     * The target product represents the written output file
     *
     * @throws OperatorException If a processing error occurs.
     */
    @Override
    public void initialize() throws OperatorException {
        if (binningConfig == null) {
            throw new OperatorException("Missing operator parameter 'binningConfig'");
        }
        if (binningConfig.getMaskExpr() == null) {
            throw new OperatorException("Missing operator parameter 'binningConfig.maskExpr'");
        }
        if (binningConfig.getNumRows() <= 2) {
            throw new OperatorException("Operator parameter 'binningConfig.numRows' must be greater than 2");
        }
        if (formatterConfig == null) {
            throw new OperatorException("Missing operator parameter 'formatterConfig'");
        }
        if (formatterConfig.getOutputFile() == null) {
            throw new OperatorException("Missing operator parameter 'formatterConfig.outputFile'");
        }
        for (Product sourceProduct : sourceProducts) {
            System.out.println("sourceProduct = " + sourceProduct);
        }

        Debug.setEnabled(true);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        BinningContext binningContext = binningConfig.createBinningContext();

        try {
            // Step 1: Spatial binning - creates time-series of spatial bins for each bin ID ordered by ID. The tree map structure is <ID, time-series>
            SortedMap<Long, List<SpatialBin>> spatialBinMap = doSpatialBinning(binningContext, sourceProducts);
            // Step 2: Temporal binning - creates a list of temporal bins, sorted by bin ID
            List<TemporalBin> temporalBins = doTemporalBinning(binningContext, spatialBinMap);
            // Step 3: Formatting
            doOutputting(regionWkt, formatterConfig, binningContext, temporalBins);

            targetProduct = ProductIO.readProduct(new File(formatterConfig.getOutputFile()));

        } catch (OperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new OperatorException(e);
        }

        stopWatch.stopAndTrace(String.format("Total time for binning %d product(s)", sourceProducts.length));


        //targetProduct = new Product("N", "T", 360, 180);
        //targetProduct.setFileLocation(new File(formatterConfig.getOutputFile()));

    }


    private static SortedMap<Long, List<SpatialBin>> doSpatialBinning(BinningContext binningContext, Product[] sourceProducts) throws IOException {
        final SpatialBinStore spatialBinStore = new SpatialBinStore();
        final SpatialBinner spatialBinner = new SpatialBinner(binningContext, spatialBinStore);
        for (Product sourceProduct : sourceProducts) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            System.out.println("processing " + sourceProduct);
            final long numObs = SpatialProductBinner.processProduct(sourceProduct, spatialBinner, binningContext.getSuperSampling(), ProgressMonitor.NULL);
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
