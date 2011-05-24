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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 *
 * @author Boe
 */
public class LcL3Converter implements FormatConverter {

    private static final String TMP_DIR = "/home/hadoop/tmp";

    private static final String PREFIX_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.format.prefix']/Data/LiteralData";
    private static final String SUFFIX_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.format.suffix']/Data/LiteralData";

    public void convert(String taskId, Path inputPath, String outputDir, String targetFormat, Configuration configuration)
        throws Exception
    {
        final String formattingWpsRequest = configuration.get("calvalus.request");
        WpsConfig formattingWpsConfig = new WpsConfig(formattingWpsRequest);
        L3FormatterConfig formatterConfig = L3FormatterConfig.create(formattingWpsConfig.getFormatterParameters());

        String[] nameParts = inputPath.getParent().toUri().getPath().split("/");
        String region = nameParts.length > 3 ? nameParts[nameParts.length-3] : "anywhere";
        String name = nameParts.length >= 1 ? nameParts[nameParts.length-1] : "anytime";
        String prefix = formattingWpsConfig.getXmlDoc().getString(PREFIX_XPATH, "l3-sr");
        String suffix = formattingWpsConfig.getXmlDoc().getString(SUFFIX_XPATH, "nc");
        final String filename = String.format("%s-%s-%s.%s", prefix, region, name, suffix);
        final String tmpDir = TMP_DIR + File.separator + taskId;
        final String tmpFile = String.format("%s%s%s", tmpDir, File.separator, filename);
        formatterConfig.setOutputFile(tmpFile);

        String processingWps = loadProcessingWpsXml(inputPath, configuration);
        WpsConfig level3Wpsconfig = new WpsConfig(processingWps);
        L3Config l3Config = L3Config.fromXml(level3Wpsconfig.getLevel3Parameters());
        Geometry roiGeometry = JobUtils.createGeometry(level3Wpsconfig.getGeometry());

        L3Formatter formatter = new L3Formatter(configuration);
        formatter.format(formatterConfig, l3Config, inputPath.getParent().toUri().toString(), roiGeometry);

        final ProcessUtil copyProcess = new ProcessUtil();
        copyProcess.directory(new File(tmpDir));
        try {
            final int returnCode = copyProcess.run("/bin/bash", "-c", "cp -r * " + outputDir + "; rm -rf " + tmpDir);
            if (returnCode != 0) {
                throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + copyProcess.getOutputString());
            }
        } catch (InterruptedException e) {
            throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + e);
        }
    }

    private String loadProcessingWpsXml(Path wpsRequestPath, Configuration configuration) throws IOException {
        FileSystem fs = wpsRequestPath.getFileSystem(configuration);
        InputStream is = fs.open(wpsRequestPath);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(is, baos);
        return baos.toString();
    }
}

