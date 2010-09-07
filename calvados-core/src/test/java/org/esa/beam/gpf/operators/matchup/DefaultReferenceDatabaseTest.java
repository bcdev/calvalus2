package org.esa.beam.gpf.operators.matchup;

import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class DefaultReferenceDatabaseTest {

    @Test
    public void testFindReferenceMeasurement() throws Exception {
        DefaultReferenceDatabase referenceDatabase = new DefaultReferenceDatabase();

        Date date1 = ProductData.UTC.parse("22-JAN-2005 11:11:11").getAsDate();
        String northSeaSite = "northsea";
        ReferenceMeasurement northseaMeasurement = new ReferenceMeasurement("id1", northSeaSite, date1, new GeoPos(55f, 2f));
        referenceDatabase.addReferenceMeasurement(northseaMeasurement);

        Date date2 = ProductData.UTC.parse("22-JAN-2005 11:11:11").getAsDate();
        String gulfSite = "Gulf of Mexico";
        ReferenceMeasurement gulfMeasurement = new ReferenceMeasurement("id1", gulfSite, date2, new GeoPos(24f, -90f));
        referenceDatabase.addReferenceMeasurement(gulfMeasurement);

        final int deltaTime = 60 * 60;
        Product product = createTestProduct();
        List<ReferenceMeasurement> result = referenceDatabase.findReferenceMeasurement(null, product, deltaTime);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertSame(gulfMeasurement, result.get(0));

        result = referenceDatabase.findReferenceMeasurement("africa", product, deltaTime);
        assertNotNull(result);
        assertEquals(0, result.size());

        result = referenceDatabase.findReferenceMeasurement(gulfSite, product, deltaTime);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertSame(gulfMeasurement, result.get(0));

        result = referenceDatabase.findReferenceMeasurement(gulfSite, product, 60);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    private static Product createTestProduct() throws Exception {
        Rectangle bounds = new Rectangle(180, 180);
        Product product = new Product("test", "TEST", bounds.width, bounds.height);

        product.setStartTime(ProductData.UTC.parse("22-JAN-2005 10:00:00"));
        product.setEndTime(ProductData.UTC.parse("22-JAN-2005 12:00:00"));

        AffineTransform i2mTransform = new AffineTransform();
        final int northing = 90;
        final int easting = -180;
        i2mTransform.translate(easting, northing);
        final double scaleX = 180 / bounds.width;
        final double scaleY = 180 / bounds.height;
        i2mTransform.scale(scaleX, -scaleY);
        CrsGeoCoding geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, bounds, i2mTransform);
        product.setGeoCoding(geoCoding);

        return product;
    }

}
