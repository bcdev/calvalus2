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
import org.esa.beam.gpf.operators.matchup.ReferenceDatabase;

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

    @Parameter(defaultValue = "1000", unit = "m")
    private double windowSize;

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
        setTargetProduct(new Product("DUMMY","t",0,0 ));
        if (!isProductInTimeRange(sourceProduct, startTime, endTime)) {
            return;
        }
        //TODO get reference Database, if not set
        List<ReferenceMeasurement> measurements = referenceDatabase.findReferenceMeasurement(site, sourceProduct);
        if (measurements.isEmpty()) {
            return;
        }
        List<Matchup>  matchupList = new ArrayList<Matchup>(measurements.size());
        for (ReferenceMeasurement measurement : measurements) {
            Product subset = createSubset(sourceProduct, measurement.getLocation(), windowSize);
            Product result = processProduct(subset);
            Matchup matchup = extractMatchup(result, measurement);
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

    private Product createSubset(Product source, GeoPos location, double windowSize) {
        return null;  //TODO
    }

    private Product processProduct(Product subset) {
        return subset;  //TODO
    }

    private Matchup extractMatchup(Product result, ReferenceMeasurement measurement) {
        return null;  //TODO
    }
}
