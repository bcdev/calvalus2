package com.bc.calvalus.processing;

import com.bc.calvalus.processing.l3.SequenceFileBinIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.TemporalBinSource;
import org.esa.snap.binning.operator.formatter.Formatter;
import org.esa.snap.binning.operator.formatter.FormatterConfig;
import org.esa.snap.binning.operator.formatter.FormatterFactory;
import org.esa.snap.binning.operator.formatter.IsinFormatter;
import org.esa.snap.binning.support.IsinPlanetaryGrid;
import org.esa.snap.binning.support.SEAGrid;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

public class IntegrationTestPartFiles {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testReadISIN_part() throws Exception {
//        final Path seqFilePath = new Path("D:/Satellite/s3mpc/grid-test/ISIN_L3_2018-11-11_2018-11-11/part-r-00000");
        final Path seqFilePath = new Path("/data/EOdata/S3MPC/grid-test/ISIN_L3_2018-11-11_2018-11-11/part-r-00000");

        final IsinPlanetaryGrid grid = new IsinPlanetaryGrid(21600);

//        final IsinFormatter formatter = new IsinFormatter();
        final Formatter formatter = FormatterFactory.get("default");
        writeSequenceFile(seqFilePath, grid, formatter);
    }

    @Test
    public void testReadSEA_part() throws Exception {
//        final Path seqFilePath = new Path("D:/Satellite/s3mpc/grid-test/SeaWifs_L3_2018-11-11_2018-11-11/part-r-00000");
        final Path seqFilePath = new Path("/data/EOdata/S3MPC/grid-test/SeaWifs_L3_2018-11-11_2018-11-11/part-r-00000");

        final SEAGrid grid = new SEAGrid(21600);

        final Formatter defaultFormatter = FormatterFactory.get("default");
        writeSequenceFile(seqFilePath, grid, defaultFormatter);
    }

    private void writeSequenceFile(Path seqFilePath, PlanetaryGrid planetaryGrid, Formatter formatter) throws Exception {
        final SequenceFile.Reader.Option file = SequenceFile.Reader.file(seqFilePath);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, file)) {
            final SequenceFileBinIterator binIterator = new SequenceFileBinIterator(reader);
            final TestBinSource binSource = new TestBinSource(binIterator);
            final String[] featureNames = {"OGVI_mean", "OGVI_sigma", "OGVI_count", "OTCI_mean", "OTCI_sigma", "OTCI_count"};
            final FormatterConfig formatterConfig = new FormatterConfig();
            formatterConfig.setOutputType("Product");
//            formatterConfig.setOutputFormat("GeoTIFF");
//            formatterConfig.setOutputFormat("NetCDF4-BEAM");
            formatterConfig.setOutputFormat("BEAM-DIMAP");
//            formatterConfig.setOutputFile("D:/Satellite/s3mpc/grid-test");
            formatterConfig.setOutputFile("/data/EOdata/S3MPC/grid-test/isin_netcdf");
            final ProductData.UTC startDate = ProductData.UTC.create(new Date(1541894400000L), 0);
            final ProductData.UTC endDate = ProductData.UTC.create(new Date(1541894400000L), 0);

            formatter.format(planetaryGrid, binSource, featureNames, formatterConfig, null, startDate, endDate, new MetadataElement[0]);
        }
    }


    private class TestBinSource implements TemporalBinSource {

        final SequenceFileBinIterator binIterator;

        public TestBinSource(SequenceFileBinIterator binIterator) {
            this.binIterator = binIterator;
        }

        @Override
        public int open() throws IOException {
            return 1;
        }

        @Override
        public Iterator<? extends TemporalBin> getPart(int index) throws IOException {
            return binIterator;
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {

        }

        @Override
        public void close() throws IOException {
        }
    }
}
