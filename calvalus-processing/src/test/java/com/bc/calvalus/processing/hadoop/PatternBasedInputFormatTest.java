package com.bc.calvalus.processing.hadoop;

import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.Test;

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
}