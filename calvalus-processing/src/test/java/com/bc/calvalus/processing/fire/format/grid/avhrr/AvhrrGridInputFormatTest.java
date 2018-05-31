package com.bc.calvalus.processing.fire.format.grid.avhrr;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class AvhrrGridInputFormatTest {

    @Test
    public void testSorting() throws Exception {
        String dates = "BA_1982_10_Dates.tif";
        String porc = "BA_1982_10_Porcentage.tif";
        String unc = "BA_1982_10_Uncertainty.tif";

        List<String> strings = new ArrayList<>();
        strings.add(dates);
        strings.add(porc);
        strings.add(unc);

        for (int i = 0; i < 1000; i++) {
            Collections.shuffle(strings);

            strings.sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            });

            assertEquals(dates, strings.get(0));
            assertEquals(porc, strings.get(1));
            assertEquals(unc, strings.get(2));
        }
    }
}
