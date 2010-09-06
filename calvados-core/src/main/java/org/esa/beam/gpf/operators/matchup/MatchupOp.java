package org.esa.beam.gpf.operators.matchup;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


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

    @Parameter(defaultValue = "5", unit = "pixel")
    private int windowSize;

    @Parameter
    private String validMask;

    @TargetProperty
    private Matchup[] matchups;


    private ReferenceDatabase referenceDatabase;

    public void setReferenceDatabase(ReferenceDatabase referenceDatabase) {
        this.referenceDatabase = referenceDatabase;
    }

    @Override
    public void initialize() throws OperatorException {
        setTargetProduct(sourceProduct);
        if (!isProductInTimeRange(sourceProduct, startTime, endTime)) {
            return;
        }
        //TODO get reference Database, if not set
        List<ReferenceMeasurement> measurementList = referenceDatabase.findReferenceMeasurement(site, sourceProduct, deltaTime);
        if (measurementList.isEmpty()) {
            return;
        }
        List<Matchup>  matchupList = new ArrayList<Matchup>(measurementList.size());
        for (ReferenceMeasurement referenceMeasurement : measurementList) {
            Product subset = createSubset(sourceProduct, referenceMeasurement.getLocation(), windowSize);
            Product result = processProduct(subset);
            Matchup matchup = extractMatchup(result, referenceMeasurement);
            if (matchup != null) {
                matchupList.add(matchup);
            }
        }
        matchups = matchupList.toArray(new Matchup[matchupList.size()]);

    }

    static boolean isProductInTimeRange(Product product, ProductData.UTC startUTC, ProductData.UTC endUTC) {
        if (startUTC != null && endUTC != null) {
            Date start = startUTC.getAsDate();
            Date end = endUTC.getAsDate();
            ProductData.UTC productStart = product.getStartTime();
            if (productStart != null) {
                Date date = productStart.getAsDate();
                if (date.after(start) && date.before(end)) {
                    return true;
                }
            }
            ProductData.UTC productEnd = product.getEndTime();
            if (productEnd != null) {
                Date date = productEnd.getAsDate();
                if (date.after(start) && date.before(end)) {
                    return true;
                }
            }
        } else if (startUTC != null) {
            Date start = startUTC.getAsDate();
            ProductData.UTC productStart = product.getStartTime();
            if (productStart != null && productStart.getAsDate().after(start)) {
                return true;
            }
            ProductData.UTC productEnd = product.getEndTime();
            if (productEnd != null && productEnd.getAsDate().after(start)) {
                return true;
            }
        } else if (endUTC != null) {
            Date end = endUTC.getAsDate();
            ProductData.UTC productStart = product.getStartTime();
            if (productStart != null && productStart.getAsDate().before(end)) {
                return true;
            }
            ProductData.UTC productEnd = product.getEndTime();
            if (productEnd != null && productEnd.getAsDate().before(end)) {
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

    private Matchup extractMatchup(Product result, ReferenceMeasurement measurement) {
        return null;  //TODO
    }
}
