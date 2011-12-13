package com.bc.calvalus.processing.converter;

import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.WpsConfig;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3Formatter;
import com.bc.calvalus.processing.l3.L3FormatterConfig;
import com.bc.calvalus.processing.shellexec.ProcessUtil;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import com.bc.io.IOUtils;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 *
 * @author Boe
 */
@Deprecated
public class LcL3Converter implements FormatConverter {

    private static final String TMP_DIR = "/home/hadoop/tmp";

    private static final String PREFIX_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.format.prefix']/Data/LiteralData";
    private static final String SUFFIX_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.format.suffix']/Data/LiteralData";

    public void convert(String taskId, Path inputPath, String outputDir, String targetFormat, Configuration configuration)
        throws Exception
    {
        final String formattingWpsRequest = configuration.get("calvalus.request");
        WpsConfig formattingWpsConfig = new WpsConfig(formattingWpsRequest);
        L3FormatterConfig formatterConfig = L3FormatterConfig.fromXml(formattingWpsConfig.getFormatterParameters());

        final String filename = l3FilenameOf(inputPath, formattingWpsConfig);
        final File tmpDir = new File(TMP_DIR + File.separator + taskId);
        tmpDir.mkdirs();
        final String tmpFile = String.format("%s%s%s", tmpDir.getPath(), File.separator, filename);
        formatterConfig.setOutputFile(tmpFile);

        String processingWps = loadProcessingWpsXml(inputPath, configuration);
        WpsConfig level3Wpsconfig = new WpsConfig(processingWps);
        L3Config l3Config = L3Config.fromXml(level3Wpsconfig.getLevel3Parameters());
        Geometry roiGeometry = JobUtils.createGeometry(level3Wpsconfig.getGeometry());

        L3Formatter formatter = new L3Formatter(configuration);
        formatter.format(formatterConfig, l3Config, inputPath.getParent().toUri().toString(), roiGeometry);

        final ProcessUtil copyProcess = new ProcessUtil(new ProcessUtil.OutputObserver() {
            public void handle(String line) {
                System.out.println(line);
            }
        });
        copyProcess.directory(tmpDir);

        try {
            final int returnCode;
            if (outputDir.startsWith("hdfs:")) {
                returnCode = copyProcess.run("/bin/bash", "-c", "gzip " + filename + "; hadoop fs -copyFromLocal " + filename + ".gz " + outputDir + "/" + filename + ".gz; rm -rf " + tmpDir.getPath() + "; echo \"" + filename + " copied to " + outputDir + "\"");
                System.out.println("cmdline input : " + "gzip " + filename + "; hadoop fs -copyFromLocal " + filename + ".gz " + outputDir + "/" + filename + ".gz; rm -rf " + tmpDir.getPath());
            } else {
                final String outputPath = (outputDir.startsWith("file:"))
                        ? new Path(outputDir).toUri().getPath()
                        : outputDir;
                returnCode = copyProcess.run("/bin/bash", "-c", "date; gzip *.nc; mkdir -p " + outputPath + "; cp -r * " + outputPath + "/; rm -rf " + tmpDir.getPath() + "; echo \"" + filename + " copied to " + outputDir + "\"");
                System.out.println("cmdline input : " + "gzip *.nc; mkdir -p " + outputPath + "; cp -r * " + outputPath + "/; rm -rf " + tmpDir.getPath());
            }
            System.out.println("|" + copyProcess.getOutputString() + "|");
            if (returnCode != 0) {
                throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + copyProcess.getOutputString());
            }
        } catch (InterruptedException e) {
            throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + e);
        }
    }

    private String l3FilenameOf(Path inputPath, WpsConfig formattingWpsConfig) throws XPathExpressionException {
        String[] nameParts = inputPath.getParent().toUri().getPath().split("/");
        String region = nameParts.length > 3 ? nameParts[nameParts.length-3] : "anywhere";
        String name = nameParts.length >= 1 ? nameParts[nameParts.length-1] : "anytime";
        int pos = name.indexOf('-');
        name = name.substring(0, pos+1) + name.substring(pos+1).replaceAll("-", "");
        String prefix = formattingWpsConfig.getXmlDoc().getString(PREFIX_XPATH, "CCI-LC-MERIS-SR-L3-300m");
        String suffix = formattingWpsConfig.getXmlDoc().getString(SUFFIX_XPATH, "nc");
        final String version = "v2.0";
        final String centre = "BC";
        final String procTime = "201108";
        return String.format("%s-%s-%s-%s-%s-%s.%s", prefix, name, version, region, centre, procTime, suffix);
    }

    private String loadProcessingWpsXml(Path wpsRequestPath, Configuration configuration) throws IOException {
        FileSystem fs = wpsRequestPath.getFileSystem(configuration);
        InputStream is = fs.open(wpsRequestPath);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(is, baos);
        return baos.toString();
    }
}

