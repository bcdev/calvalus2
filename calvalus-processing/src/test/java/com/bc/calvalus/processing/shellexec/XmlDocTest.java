package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.processing.shellexec.XmlDoc;
import org.junit.Assert;
import org.junit.Test;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class XmlDocTest {

    @Test
    public void testXPath() throws Exception {
        XmlDoc doc = new XmlDoc("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><x:a><b>c</b><d>e</d></x:a>");
        Assert.assertEquals("value retrieval", "e", doc.getString("/a/d"));
    }
}
