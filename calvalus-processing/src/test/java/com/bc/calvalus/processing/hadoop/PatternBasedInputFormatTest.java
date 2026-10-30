package com.bc.calvalus.processing.hadoop;

import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class PatternBasedInputFormatTest {

    @Test
    public void testSoblooQuery() {
        String searchUrl = "https://sobloo.eu/api/v1/services/search?f=identification.externalId:like:MSIL1C&f=timeStamp:range:%5B1530403200000%3C1530662399000%5D&gintersect=POLYGON%20%28%2810.734080652848661%2045.581416898369106,%2010.734080652848661%2045.53814805784238,%2010.658549646989286%2045.53814805784238,%2010.658549646989286%2045.581416898369106,%2010.734080652848661%2045.581416898369106%29%29&include=identification&from=0&size=20";
        new GetMethod(searchUrl);
    }

    /**
     * Tests that the _N0500 substring is correctly deleted from a pathPattern containing the L2A_N0500 subpath.
     * Older products with baseline > N0500 will be placed in this path, newer products are in L2A
     * (without the L2A_N0500) instead. Products with a baseline below N0500 are never placed under the L2A_N0500
     * subpath.
     * The switch date is an empirical value and may be set in the request.
     */
    @Test
    public void testReplacePathPatternN0500() {
        // These are real products
        String n0500Before = "S2A_MSIL2A_20221115T212751_N0510_R043_T06VUQ_20240706T161319.SAFE";
        String n0500After = "S2B_MSIL2A_20260103T212809_N0511_R043_T06VUQ_20260103T225909.SAFE";
        String n0400Before = "S2A_MSIL2A_20220129T212731_N0400_R043_T06VUQ_20220129T220526.SAFE";

        // No example of an N0400 product after switch date available, unclear if it exists. Made-up example
        String n0400After = "S2B_MSIL2A_20260103T212809_N0400_R043_T06VUQ_20260103T225909.SAFE";

        // TODO
        Pattern pathPattern = Pattern.compile("(S2._MSIL2A_(....)(..)(..)(T......).*.SAFE)");
        String pathReplacement = "s3a://eodata/Sentinel-2/MSI/L2A_N0500/$2/$3/$4/$1";
        String acquisitionDateReplacement = "$2$3$4$5";
        String n05xxSwitchdate = "20231213T081331";

        String l2aSubPath = "s3a://eodata/Sentinel-2/MSI/L2A/";
        String l2aN0500SubPath = "s3a://eodata/Sentinel-2/MSI/L2A_N0500/";

        PatternBasedInputFormat inputFormat = new PatternBasedInputFormat();

        String replacedN0500Before = inputFormat.replacePathPattern(n0500Before, pathPattern, pathReplacement, n05xxSwitchdate, acquisitionDateReplacement);
        Assert.assertTrue(replacedN0500Before.startsWith(l2aN0500SubPath));

        String replacedN0500After = inputFormat.replacePathPattern(n0500After, pathPattern, pathReplacement, n05xxSwitchdate, acquisitionDateReplacement);
        Assert.assertTrue(replacedN0500After.startsWith(l2aSubPath));

        String replacedN0400Before = inputFormat.replacePathPattern(n0400Before, pathPattern, pathReplacement, n05xxSwitchdate, acquisitionDateReplacement);
        Assert.assertTrue(replacedN0400Before.startsWith(l2aSubPath));

        String replacedN0400After = inputFormat.replacePathPattern(n0400After, pathPattern, pathReplacement, n05xxSwitchdate, acquisitionDateReplacement);
        Assert.assertTrue(replacedN0400After.startsWith(l2aSubPath));
    }
}