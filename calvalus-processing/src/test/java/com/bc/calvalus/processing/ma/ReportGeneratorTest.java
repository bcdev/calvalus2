package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Norman Fomferra
 */
@Ignore
public class ReportGeneratorTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        final File outputDir = new File("reports/" + System.currentTimeMillis());
        List<PlotDatasetCollector.Point> points1 = Arrays.asList(new PlotDatasetCollector.Point(0.1, 0.1, 0.2, 6),
                                                                 new PlotDatasetCollector.Point(1.1, 1.2, 0.1, 5),
                                                                 new PlotDatasetCollector.Point(3.1, 2.5, 0.3, 4),
                                                                 new PlotDatasetCollector.Point(4.1, 3.7, 0.5, 9));
        PlotDatasetCollector.PlotDataset plotDataset1 = new PlotDatasetCollector.PlotDataset("northsea",
                                                                                             new PlotDatasetCollector.VariablePair("CHL", -1, "chl", -1),
                                                                                             points1);
        List<PlotDatasetCollector.Point> points2 = Arrays.asList(new PlotDatasetCollector.Point(1.3, 1.1, 0.2, 6),
                                                                 new PlotDatasetCollector.Point(1.1, 1.2, 0.1, 5),
                                                                 new PlotDatasetCollector.Point(0.1, 0.3, 0.3, 4),
                                                                 new PlotDatasetCollector.Point(0.6, 1.1, 0.3, 4),
                                                                 new PlotDatasetCollector.Point(2.1, 2.4, 0.5, 9));
        PlotDatasetCollector.PlotDataset plotDataset2 = new PlotDatasetCollector.PlotDataset("northsea",
                                                                                             new PlotDatasetCollector.VariablePair("TSM", -1, "tsm", -1),
                                                                                             points2);
        PlotDatasetCollector.PlotDataset plotDataset3 = new PlotDatasetCollector.PlotDataset("northsea",
                                                                                             new PlotDatasetCollector.VariablePair("K_490", -1, "k_490", -1),
                                                                                             points1);
        Configuration configuration = new Configuration();
        configuration.set(JobConfigNames.CALVALUS_MA_PARAMETERS, "" +
                "<parameters>\n" +
                "    <recordSourceSpiClassName>com.bc.calvalus.processing.ma.CsvRecordSource$Spi</recordSourceSpiClassName>\n" +
                "    <recordSourceUrl>hdfs://master00:9000/calvalus/home/norman/cc-matchup-test-insitu.csv</recordSourceUrl>\n" +
                "    <outputGroupName>SITE</outputGroupName>\n" +
                "    <copyInput>true</copyInput>\n" +
                "    <goodPixelExpression>!l2_flags.CLOUD</goodPixelExpression>\n" +
                "    <goodRecordExpression>conc_chl.cv &lt; 0.15</goodRecordExpression>\n" +
                "    <filteredMeanCoeff>1.5</filteredMeanCoeff>\n" +
                "    <macroPixelSize>5</macroPixelSize>\n" +
                "    <maxTimeDifference>5.0</maxTimeDifference>\n" +
                "</parameters>");
        configuration.set(JobConfigNames.CALVALUS_SNAP_BUNDLE, "snap-1.2.3" );
        configuration.set(JobConfigNames.CALVALUS_CALVALUS_BUNDLE, "calvalus-4.5.6" );
        configuration.set(JobConfigNames.CALVALUS_BUNDLES, "coastcolour-processing-1.2-SNAPSHOT" );
        configuration.set(JobConfigNames.CALVALUS_L2_OPERATOR, "CoastColour.L2W" );
                configuration.set(JobConfigNames.CALVALUS_L2_PARAMETERS, "" +
                "<parameters>\n" +
                "    <averageSalinity>35</averageSalinity>\n" +
                "    <averageTemperature>15</averageTemperature>\n" +
                "</parameters>");
        OutputStreamFactory outputStreamFactory = new OutputStreamFactory() {
            @Override
            public OutputStream createOutputStream(String path) throws IOException {
                outputDir.mkdirs();
                return new FileOutputStream(new File(outputDir, path));
            }
        };
        Map<String, Integer> annotatedRecordCounts = new LinkedHashMap<String, Integer>();
        annotatedRecordCounts.put("Total", 120);
        annotatedRecordCounts.put("Excluded (RECORD_EXPRESSION)", 40);
        annotatedRecordCounts.put("Excluded (OVERLAPPING)", 20);
        annotatedRecordCounts.put("Good", 60);
        ReportGenerator.generateReport(outputStreamFactory,
                                       configuration,
                                       annotatedRecordCounts,
                                       new PlotDatasetCollector.PlotDataset[]{plotDataset1, plotDataset2, plotDataset3});
    }
}
