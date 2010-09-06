package org.esa.beam.gpf.operators.matchup;

import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorException;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.SourceProduct;
import org.esa.beam.framework.gpf.annotations.TargetProperty;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.math.MathUtils;

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;


public class MatchupOp extends Operator {

    @SourceProduct(alias = "source")
    private Product sourceProduct;

    @Parameter
    private String site;

    @Parameter
    private ProductData.UTC startTime;

    @Parameter
    private ProductData.UTC endTime;

    @Parameter(defaultValue = "3600", unit = "seconds")
    private double deltaTime;

    @Parameter(defaultValue = "3", unit = "pixel")
    private int windowSize;

    @Parameter
    private String validMask; // TODO

    @TargetProperty
    private MatchupDataset[] matchupDatasets;


    private ReferenceDatabase referenceDatabase;

    public void setReferenceDatabase(ReferenceDatabase referenceDatabase) {
        this.referenceDatabase = referenceDatabase;
    }

    @Override
    public void initialize() throws OperatorException {
        validateInput();
        setTargetProduct(sourceProduct);
        if (!isProductInTimeRange(sourceProduct, startTime, endTime)) {
            return;
        }
        if (referenceDatabase == null) {
            //TODO get reference Database, if not set
            return;
        }
        List<ReferenceMeasurement> measurementList = referenceDatabase.findReferenceMeasurement(site, sourceProduct, deltaTime);
        if (measurementList.isEmpty()) {
            return;
        }
        List<MatchupDataset> matchupDatasetList = new ArrayList<MatchupDataset>(measurementList.size());
        for (ReferenceMeasurement referenceMeasurement : measurementList) {
            Product subset = createSubset(sourceProduct, referenceMeasurement.getLocation(), windowSize);
            Product result = processProduct(subset);
            MatchupDataset matchupDataset = extractMatchup(result, referenceMeasurement);
            if (matchupDataset != null) {
                matchupDatasetList.add(matchupDataset);
            }
        }
        matchupDatasets = matchupDatasetList.toArray(new MatchupDataset[matchupDatasetList.size()]);
    }

    private void validateInput() {
        if (sourceProduct.getGeoCoding() == null) {
            throw new OperatorException("Source product has not geo-coding.");
        }
        if (sourceProduct.getStartTime() == null) {
            throw new OperatorException("Source product has not start time.");
        }
        if (sourceProduct.getEndTime() == null) {
            throw new OperatorException("Source product has not end time.");
        }
    }

    static boolean isProductInTimeRange(Product product, ProductData.UTC startUTC, ProductData.UTC endUTC) {
        Date productStart = product.getStartTime().getAsDate();
        Date productEnd = product.getEndTime().getAsDate();
        if (startUTC != null && endUTC != null) {
            Date start = startUTC.getAsDate();
            Date end = endUTC.getAsDate();
            if (productStart.after(start) && productStart.before(end)) {
                return true;
            }
            if (productEnd.after(start) && productEnd.before(end)) {
                return true;
            }
        } else if (startUTC != null) {
            Date start = startUTC.getAsDate();
            if (productStart.after(start) || productEnd.after(start)) {
                return true;
            }
        } else if (endUTC != null) {
            Date end = endUTC.getAsDate();
            if (productStart.before(end) || productEnd.before(end)) {
                return true;
            }
        } else {
            return true;
        }
        return false;
    }

    static Product createSubset(Product source, GeoPos location, int windowSize) {
        SubsetOp subsetOp = new SubsetOp();
        subsetOp.setSourceProduct(source);

        PixelPos centerPos = source.getGeoCoding().getPixelPos(location, null);
        int offset = MathUtils.floorInt(windowSize / 2);
        int upperLeftX = MathUtils.floorInt(centerPos.x - offset);
        int upperLeftY = MathUtils.floorInt(centerPos.y - offset);
        Rectangle region = new Rectangle(upperLeftX, upperLeftY, windowSize, windowSize);
        subsetOp.setRegion(region);

        subsetOp.setCopyMetadata(true);

        return subsetOp.getTargetProduct();
    }

    private Product processProduct(Product subset) {
        return subset;  //TODO
    }

    private MatchupDataset extractMatchup(Product product, ReferenceMeasurement measurement) {
        GeoPos location = measurement.getLocation();
        ProductData.UTC observationTime = getObservationTime(product, location);
        MatchupDataset dataset = new MatchupDataset(location, observationTime);
        dataset.setProductName(product.getName());
        Band[] bands = product.getBands();
        for (Band band : bands) {
            Raster data = band.getGeophysicalImage().getData();
            double sampleSum = 0;
            int sampleCount = 0;
            Rectangle bounds = data.getBounds();
            double[] samples = data.getSamples(bounds.x, bounds.y, bounds.width, bounds.height, 0, (double[])null);
            int[] validMask = null;
            if (band.isValidMaskUsed()) {
                Raster validData = band.getValidMaskImage().getData();
                validMask = validData.getSamples(bounds.x, bounds.y, bounds.width, bounds.height, 0, (int[])null);
            }
            for (int i = 0; i < samples.length; i++) {
                if (!band.isValidMaskUsed() || validMask[i] != 0) {
                    final double sample;
                    if (band.getDataType() == ProductData.TYPE_INT8) {
                        sample = (byte) samples[i];
                    } else if (band.getDataType() == ProductData.TYPE_UINT32) {
                        sample = ((int)samples[i]) & 0xFFFFFFFFL;
                    } else {
                        sample = samples[i];
                    }
                    sampleSum += sample;
                    sampleCount++;
                }
            }
            if (sampleCount > 0) {
                double sample =  sampleSum/sampleCount;
                if (band.isScalingApplied()) {
                    sample = band.scale(sample);
                }
                dataset.setBandData(band.getName(), sample, sampleCount);
            }
        }
        return dataset;
    }

    private static ProductData.UTC getObservationTime(Product product, GeoPos location) {
        PixelPos pixelPos = product.getGeoCoding().getPixelPos(location, null);

        final ProductData.UTC startTime = product.getStartTime();
        final ProductData.UTC endTime = product.getEndTime();

        final double dStart = startTime.getMJD();
        final double dEnd = endTime.getMJD();
        final double vPerLine = (dEnd - dStart) / (product.getSceneRasterHeight() - 1);
        final double currentLine = vPerLine * pixelPos.y + dStart;
        return new ProductData.UTC(currentLine);
    }
}
