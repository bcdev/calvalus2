package org.esa.beam.gpf.operators.matchup;

import org.esa.beam.framework.datamodel.GeoCoding;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class DefaultReferenceDatabase implements ReferenceDatabase {

    private final List<ReferenceMeasurement> referenceMeasurementList;

    public DefaultReferenceDatabase() {
        this.referenceMeasurementList = new ArrayList<ReferenceMeasurement>();
    }

    public void addReferenceMeasurement(ReferenceMeasurement measurement) {
        referenceMeasurementList.add(measurement);
    }

    @Override
    public List<ReferenceMeasurement> findReferenceMeasurement(String site, Product product, double deltaTime) {
        GeoCoding geoCoding = product.getGeoCoding();
        List<ReferenceMeasurement> result = new ArrayList<ReferenceMeasurement>();
        for (ReferenceMeasurement measurement : referenceMeasurementList) {
            if (site == null ||measurement.getSite().equals(site)) {
                PixelPos pixelPos = geoCoding.getPixelPos(measurement.getLocation(), null);
                if (product.containsPixel(pixelPos)) {
                    ProductData.UTC observationUTC = measurement.getObservationTime(product);
                    long observationMillis = observationUTC.getAsDate().getTime();
                    long measurementMillis = measurement.getTime().getTime();
                    long timeDifference = Math.abs(observationMillis - measurementMillis) / 1000;
                    if (timeDifference < deltaTime) {
                        result.add(measurement);
                    }
                }
            }
        }
        return Collections.unmodifiableList(result);
    }
}
