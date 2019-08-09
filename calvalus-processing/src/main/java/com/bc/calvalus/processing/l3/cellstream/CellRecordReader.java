package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import ucar.nc2.NetcdfFile;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * A record reader for reading binned data out of netcdf files.
 * The actual reading is done by implementations of {@link }AbstractNetcdfCellReader}.
 */
class CellRecordReader extends RecordReader<LongWritable, L3TemporalBin> {

    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");

    private final AbstractNetcdfCellReader cellReader;
    private final LongWritable key;
    private final L3TemporalBin value;
    private boolean hasMore;

    public CellRecordReader(Path path, Configuration conf) throws IOException {
        File localFile = copyToLocal(path, conf);
        NetcdfFile netcdfFile = NetcdfFile.open(localFile.getAbsolutePath());
        cellReader = createReader(netcdfFile);
        key = new LongWritable();
        value = new L3TemporalBin(-1, cellReader.getFeatureNames().length);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        hasMore = true;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!hasMore) {
            return false;
        }
        try {
            hasMore = cellReader.readNext(key, value);
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
        return hasMore;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public L3TemporalBin getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return Math.min(1.0f, (cellReader.getNumReadBins() / (float) cellReader.getNumBins()));
    }

    @Override
    public void close() throws IOException {
        cellReader.close();
    }

    public String[] getFeatureNames() {
        return cellReader.getFeatureNames();
    }

    public String getStartDate() {
        Date startDate = cellReader.getStartDate();
        if (startDate != null) {
            return DATE_FORMAT.format(startDate);
        }
        return "";
    }

    public String getEndDate() {
        Date endDate = cellReader.getEndDate();
        if (endDate != null) {
            return DATE_FORMAT.format(endDate);
        }
        return "";
    }


    public int getNumRows() {
        return cellReader.getNumRows();
    }

    /**
     * Copies the file given to the local input directory for access as a ordinary {@link java.io.File}.
     *
     * @param inputPath The path to the file in the HDFS.
     * @return the local file that contains the copy.
     * @throws java.io.IOException
     */
    private static File copyToLocal(Path inputPath, Configuration conf) throws IOException {
        File localFile = new File(".", inputPath.getName());
        if (!localFile.exists()) {
            FileSystem fs = inputPath.getFileSystem(conf);
            FileUtil.copy(fs, inputPath, localFile, false, conf);
        }
        return localFile;
    }

    private static AbstractNetcdfCellReader createReader(NetcdfFile netcdfFile) throws IOException {
        if (netcdfFile.findVariable("bl_bin_num") != null &&
                netcdfFile.findVariable("bl_nscenes") != null &&
                netcdfFile.findVariable("bl_nobs") != null) {
            return new SnapSparseCellReader(netcdfFile);
        } else if (netcdfFile.findVariable("Level-3_Binned_Data/BinList.bin_num") != null &&
                netcdfFile.findVariable("Level-3_Binned_Data/BinList.nobs") != null &&
                netcdfFile.findVariable("Level-3_Binned_Data/BinList.nscenes") != null) {
            return new SeadasBinnnedCellReader(netcdfFile);
        }
        throw new IOException("unsupported netcdf file " + netcdfFile);
    }

    public static void main(String[] args) throws Exception {
        NetcdfFile netcdfFile = NetcdfFile.open(args[0]);


        String cdl = netcdfFile.toString();
        System.out.println("Netcdf as CDL");
        System.out.println(cdl);

        AbstractNetcdfCellReader reader = createReader(netcdfFile);
        System.out.println("reader.getClass().getName() = " + reader.getClass().getName());
        System.out.println("featureNames = " + Arrays.toString(reader.getFeatureNames()));
        System.out.println("NumBins = " + reader.getNumBins());
        System.out.println("NumRows = " + reader.getNumRows());

        LongWritable key = new LongWritable();
        L3TemporalBin value = new L3TemporalBin(-1, reader.getFeatureNames().length);

        int totalBinsRead = 0;
        long t0 = System.currentTimeMillis();
        while (reader.readNext(key, value)) {
            totalBinsRead++;
//            System.out.println("key = " + key);
//            System.out.println("value = " + value);
//            System.out.println("currentIndex = " + reader.getCurrentIndex());
//            System.out.println("---------------------------------");
//            if (totalBinsRead > 8118) break;
        }
        long t1 = System.currentTimeMillis();
        long time = t1 - t0;
        System.out.println("time = " + time);
        System.out.println("totalBinsRead = " + totalBinsRead);

        System.out.println("startDate = " + reader.getStartDate());
        System.out.println("endDate = " + reader.getEndDate());
    }
}
