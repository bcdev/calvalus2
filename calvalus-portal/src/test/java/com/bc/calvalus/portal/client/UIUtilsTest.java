package com.bc.calvalus.portal.client;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.bc.calvalus.portal.shared.DtoTimeSelection;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class UIUtilsTest {

    @Test
    public void parseParametersFromContext() throws Exception {
        List<String> productIdentifiers = new ArrayList<>();
        productIdentifiers.add("EOP:CODE-DE:S2_MSI_L1C:/S2A_MSIL1C_20171118T111341_N0206_R137_T30UYC_20171118T113620");
        productIdentifiers.add("EOP:CODE-DE:S2_MSI_L1C:/S2A_MSIL1C_20171118T111341_N0206_R137_T31UEA_20171118T113620");
        DtoTimeSelection timeSelection = new DtoTimeSelection("2017-11-18T00:00:00Z", "2017-11-28T00:00:00Z");
        DtoInputSelection inputSelection = new DtoInputSelection("S2_MSI_L1C_RE",
                                                                 productIdentifiers,
                                                                 timeSelection,
                                                                 "POLYGON((10.782978346427502 51.38449514420001, 11.903583815177502 51.38449514420001, 11.903583815177502 52.08762014420001, 10.782978346427502 52.08762014420001, 10.782978346427502 51.38449514420001))");

        Map<String, String> parametersMap = UIUtils.parseParametersFromContext(inputSelection);

        assertThat(parametersMap.size(), equalTo(6));
        assertThat(parametersMap.get("productIdentifiers"),
                   equalTo("S2A_MSIL1C_20171118T111341_N0206_R137_T30UYC_20171118T113620,S2A_MSIL1C_20171118T111341_N0206_R137_T31UEA_20171118T113620"));
        assertThat(parametersMap.get("minDate"), equalTo("2017-11-18"));
        assertThat(parametersMap.get("maxDate"), equalTo("2017-11-28"));
        assertThat(parametersMap.get("regionWKT"), equalTo("POLYGON((10.782978346427502 51.38449514420001, 11.903583815177502 51.38449514420001, 11.903583815177502 52.08762014420001, 10.782978346427502 52.08762014420001, 10.782978346427502 51.38449514420001))"));
        assertThat(parametersMap.get("geoInventory"), equalTo("S2_MSI_L1C_RE"));
        assertThat(parametersMap.get("collectionName"), equalTo("S2_MSI_L1C_RE"));
    }

}