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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultReferenceDatabaseTest {

    @Test
    public void testFindReferenceMeasurement() throws Exception {
        DefaultReferenceDatabase referenceDatabase = new DefaultReferenceDatabase();
        GeoPos geoPos = new GeoPos(42f, 56f);
        Date date = ProductData.UTC.parse("22-JAN-2005 11:11:11").getAsDate();
        String site = "northsea";
        ReferenceMeasurement rm = new ReferenceMeasurement("id1", site, date, geoPos);
        referenceDatabase.addReferenceMeasurement(rm);

        final int deltaTime = 60 * 60;
        List<ReferenceMeasurement> result = referenceDatabase.findReferenceMeasurement(null, createTestProduct(), deltaTime);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    private static Product createTestProduct() throws Exception {
        Rectangle bounds = new Rectangle(360, 180);
        Product product = new Product("test", "TEST", bounds.width, bounds.height);

        product.setStartTime(ProductData.UTC.parse("22-JAN-2005 10:00:00"));
        product.setEndTime(ProductData.UTC.parse("22-JAN-2005 12:00:00"));

        AffineTransform i2mTransform = new AffineTransform();
        final int northing = 90;
        final int easting = -180;
        i2mTransform.translate(easting, northing);
        final double scaleX = 360 / bounds.width;
        final double scaleY = 180 / bounds.height;
        i2mTransform.scale(scaleX, -scaleY);
        CrsGeoCoding geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, bounds, i2mTransform);
        product.setGeoCoding(geoCoding);

        return product;
    }

}
