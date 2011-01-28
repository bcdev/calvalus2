package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.bc.calvalus.processing.shellexec.XslTransformer;
import org.junit.Assert;
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
        XslTransformer xslt = new XslTransformer(new File("calvalus-processing/src/test/resources/seadas-6.1-l2gen-call.xsl"));
        final String request = FileUtil.readFile("calvalus-processing/src/test/resources/l2gen-request.xml");
        XmlDoc doc = new XmlDoc(request);
        final String result = xslt.transform(doc.getDocument());
        Assert.assertEquals("transformed string", ". /home/hadoop/opt/seadas-6.1/config/seadas.env ; mkdir -p /home/hadoop/tmp/default-task-id ; /home/hadoop/opt/seadas-6.1/bin/l2gen ifile= ofile=/home/hadoop/tmp/default-task-id/.l2.hdf eline=100 ; mv /home/hadoop/tmp/default-task-id/.l2.hdf /mnt/hdfs/calvalus/outputs/meris-l2gen-99/ ; rm -r /home/hadoop/tmp/default-task-id", result);
    }
}
