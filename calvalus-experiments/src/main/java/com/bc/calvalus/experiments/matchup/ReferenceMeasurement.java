package com.bc.calvalus.experiments.matchup;

import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.util.Date;


public class ReferenceMeasurement {
    private final String id;
    private final String site;
    private final Date time;
    private final GeoPos location;

    public ReferenceMeasurement(String id, String site, Date time, GeoPos location) {
        this.id = id;
        this.site = site;
        this.time = time;
        this.location = location;
    }

    public String getId() {
        return id;
    }

    public String getSite() {
        return site;
    }

    public Date getTime() {
        return time;
    }

    public GeoPos getLocation() {
        return location;
    }

    ProductData.UTC getObservationTime(Product product) {
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
