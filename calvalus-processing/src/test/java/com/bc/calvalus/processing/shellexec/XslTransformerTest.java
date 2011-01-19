package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.bc.calvalus.processing.shellexec.XslTransformer;
import org.junit.Test;

import java.io.File;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class XslTransformerTest {

    @Test
    public void testXPath() throws Exception {
        XslTransformer xslt = new XslTransformer(new File("/home/boe/modules/calvalus/calvalus-experiments/bin/seadas-6.1-l2gen-call.xsl"));
        XmlDoc doc = new XmlDoc("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><x:a><b>c</b><d>e</d></x:a>");
        xslt.transform(doc.getDocument());
        //Assert.assertEquals("value retrieval", "e", doc.getString("/a/d"));
    }
}
