package com.bc.calvalus.processing.l2;

import org.geotools.referencing.CRS;
import org.junit.Test;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.util.InternationalString;

import static org.junit.Assert.*;

/**
 * @author Marco Peters
 */
public class L2MapperTest {

    @Test
    public void testEPSGDescriptionRetrieval() throws Exception {
        CRSAuthorityFactory authorityFactory = CRS.getAuthorityFactory(true);
        InternationalString descriptionText = authorityFactory.getDescriptionText("EPSG:4326");
        if (descriptionText != null) {
            String epsgDescription = descriptionText.toString();
            assertEquals("WGS 84", epsgDescription);
        }
    }
}
