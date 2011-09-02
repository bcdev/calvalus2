package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;

import javax.imageio.ImageIO;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Generates the files {@link #ANALYSIS_SUMMARY_XML}, {@link #ANALYSIS_SUMMARY_XSL} and n {@link #SCATTER_PLOT_PNG}s
 * from the job configuration and n plot datasets.
 * <p/>
 * <i>Important note: If you change the output this class generated, then also adapt /com/bc/calvalus/processing/ma/analysis-summary.xsl</i>
 *
 * @author Norman
 */
public class ReportGenerator {
    static final Logger LOG = CalvalusLogger.getLogger();
    public static final String ANALYSIS_SUMMARY_XSL = "analysis-summary.xsl";
    public static final String ANALYSIS_SUMMARY_XML = "analysis-summary.xml";
    private static final String SCATTER_PLOT_PNG = "scatter-plot-%03d-%s-%s.png";
    private static final String STYLESET_CSS = "styleset.css";

    public static void generateReport(OutputStreamFactory outputStreamFactory,
                                      Configuration configuration,
                                      int recordIndex,
                                      PlotDatasetCollector.PlotDataset[] plotDatasets) throws IOException, InterruptedException {
        LOG.info(String.format("Generating %d plot(s)...", plotDatasets.length));

        PrintStream summaryFileWriter = new PrintStream(outputStreamFactory.createOutputStream(ANALYSIS_SUMMARY_XML));
        summaryFileWriter.print("" +
                                        "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" +
                                        "<?xml-stylesheet type=\"text/xsl\" href=\"analysis-summary.xsl\"?>\n" +
                                        "\n" +
                                        "<analysisSummary>\n");

        summaryFileWriter.println();
        summaryFileWriter.printf("<performedAt>%s</performedAt>\n", new Date());
        summaryFileWriter.printf("<recordCount>%s</recordCount>\n", recordIndex);
        summaryFileWriter.println();

        ArrayList<Map.Entry<String, String>> entries = getConfigurationList(configuration);
        summaryFileWriter.println();
        summaryFileWriter.print("<configuration>\n");
        for (Map.Entry<String, String> entry : entries) {
            summaryFileWriter.printf("    <property>\n");
            summaryFileWriter.printf("        <name>%s</name>\n", entry.getKey());
            summaryFileWriter.printf("        <value>%s</value>\n", entry.getValue());
            summaryFileWriter.printf("    </property>\n");
        }
        summaryFileWriter.print("</configuration>\n");
        summaryFileWriter.println();

        final PlotGenerator plotGenerator = new PlotGenerator();
        plotGenerator.setImageWidth(400);
        plotGenerator.setImageHeight(400);
        for (int i = 0; i < plotDatasets.length; i++) {
            PlotDatasetCollector.PlotDataset plotDataset = plotDatasets[i];
            String title = String.format("%s @ %s",
                                         plotDataset.getVariablePair().satelliteAttributeName,
                                         plotDataset.getGroupName());
            String subTitle = String.format("%s, %s",
                                            configuration.get(JobConfigNames.CALVALUS_L2_BUNDLE),
                                            configuration.get(JobConfigNames.CALVALUS_L2_OPERATOR));
            String imageFilename = String.format(SCATTER_PLOT_PNG,
                                                 i + 1,
                                                 mkFilename(plotDataset.getGroupName()),
                                                 mkFilename(plotDataset.getVariablePair().satelliteAttributeName));
            PlotGenerator.Result result = plotGenerator.createResult(title, subTitle, plotDataset);

            summaryFileWriter.printf("<dataset>\n");
            summaryFileWriter.printf("    <referenceVariable>%s</referenceVariable>\n", plotDataset.getVariablePair().referenceAttributeName);
            summaryFileWriter.printf("    <satelliteVariable>%s</satelliteVariable>\n", plotDataset.getVariablePair().satelliteAttributeName);
            summaryFileWriter.printf("    <statistics>\n");
            summaryFileWriter.printf("        <numDataPoints>%s</numDataPoints>\n", plotDataset.getPoints().length);
            summaryFileWriter.printf("        <regressionInter>%s</regressionInter>\n", result.regressionCoefficients[0]);
            summaryFileWriter.printf("        <regressionSlope>%s</regressionSlope>\n", result.regressionCoefficients[1]);
            summaryFileWriter.printf("        <scatterPlotImage>%s</scatterPlotImage>\n", imageFilename);
            summaryFileWriter.printf("    </statistics>\n");
            summaryFileWriter.printf("</dataset>\n");

            writeImage(result, outputStreamFactory, imageFilename);
        }

        summaryFileWriter.println("</analysisSummary>");
        summaryFileWriter.close();

        copyResource(outputStreamFactory, ANALYSIS_SUMMARY_XSL);
        copyResource(outputStreamFactory, STYLESET_CSS);
    }

    private static void writeImage(PlotGenerator.Result result, OutputStreamFactory outputStreamFactory, String imageFilename) throws IOException, InterruptedException {
        OutputStream outputStream = outputStreamFactory.createOutputStream(imageFilename);
        try {
            LOG.warning(String.format("Writing %s", imageFilename));
            ImageIO.write(result.plotImage, "PNG", outputStream);
        } catch (IOException e) {
            LOG.warning(String.format("Failed to write %s: %s", imageFilename, e.getMessage()));
        } finally {
            outputStream.close();
        }
    }

    private static String mkFilename(String s) {
        return s
                .replace(' ', '_')
                .replace(':', '_')
                .replace('\\', '_')
                .replace('/', '_');
    }

    static ArrayList<Map.Entry<String, String>> getConfigurationList(Configuration jobConfig) {
        ArrayList<Map.Entry<String, String>> entries = new ArrayList<Map.Entry<String, String>>(256);
        for (Map.Entry<String, String> entry : jobConfig) {
            entries.add(entry);
        }
        Collections.sort(entries, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        return entries;
    }

    public static void copyResource(OutputStreamFactory outputStreamFactory, String fileName) throws IOException, InterruptedException {
        InputStream inputStream = ReportGenerator.class.getResourceAsStream(fileName);
        if (inputStream == null) {
            return;
        }
        OutputStream outputStream = outputStreamFactory.createOutputStream(fileName);
        try {
            copy(inputStream, outputStream);
        } finally {
            outputStream.close();
            inputStream.close();
        }
    }

    static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[64 * 1024];
        while (true) {
            int n = inputStream.read(buffer);
            if (n > 0) {
                outputStream.write(buffer, 0, n);
            } else {
                break;
            }
        }
    }

}
