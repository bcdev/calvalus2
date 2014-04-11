package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.productinventory.ProductInventory;
import com.bc.calvalus.processing.productinventory.ProductInventoryEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.esa.beam.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An input format that maps each input file to a single (file) split.
 * <p/>
 * Input files are given by the configuration parameter
 * {@link com.bc.calvalus.processing.JobConfigNames#CALVALUS_INPUT_PATH_PATTERNS CALVALUS_INPUT_PATH_PATTERNS}.
 * Its value is expected to be a comma-separated list of file path patterns (HDFS URLs).
 * These patterns can contain dates and region names.
 *
 * @author Martin
 * @author MarcoZ
 * @author MarcoP
 * @author Norman
 */
public class PatternBasedInputFormat extends InputFormat {

    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Maps each input file to a single (file) split.
     * <p/>
     * Input files are given by the configuration parameter
     * {@link com.bc.calvalus.processing.JobConfigNames#CALVALUS_INPUT_PATH_PATTERNS}. Its value is expected to
     * be a comma-separated list of file path patterns (HDFS URLs). These patterns can contain dates and region names.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        try {
            // parse request
            Configuration configuration = job.getConfiguration();
            String inputPathPatterns = configuration.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
            String regionName = configuration.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME);
            String dateRangesString = configuration.get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES);

            List<DateRange> dateRanges = createDateRangeList(dateRangesString);

            FileSystem fs = FileSystem.get(configuration);
            HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(fs, "eodata");

            ProductInventory productInventory = ProductInventory.createInventory(configuration);
            List<InputSplit> splits = new ArrayList<InputSplit>(1000);
            for (DateRange dateRange : dateRanges) {
                FileStatus[] fileStatuses = getFileStatuses(hdfsInventoryService,
                                                            inputPathPatterns,
                                                            dateRange.getStartDate(),
                                                            dateRange.getStopDate(),
                                                            regionName);
                createSplits(fs, productInventory, fileStatuses, splits);
            }
            LOG.info("Total files to process : " + splits.size());
            return splits;
        } catch (IOException e) {
            LOG.log(Level.SEVERE, String.format("Failed to compute list of input splits: %s", e.getMessage()), e);
            throw e;
        }
    }

    private List<DateRange> createDateRangeList(String dateRangesString) throws IOException {
        List<DateRange> dateRanges = new ArrayList<DateRange>();
        boolean isDateRangeSet = StringUtils.isNotNullAndNotEmpty(dateRangesString);
        if (isDateRangeSet) {
            String[] dateRangesStrings = dateRangesString.split(",");
            for (String dateRangeString : dateRangesStrings) {
                try {
                    dateRanges.add(DateRange.parseDateRange(dateRangeString));
                } catch (ParseException e) {
                    throw new IOException(e);
                }
            }
        } else {
            dateRanges.add(DateRange.OPEN_RANGE);
        }
        return dateRanges;
    }


    private void createSplits(FileSystem fs, ProductInventory productInventory,
                              FileStatus[] fileStatuses, List<InputSplit> splits) throws IOException {
        for (FileStatus file : fileStatuses) {
            long fileLength = file.getLen();
            BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, fileLength);
            if (blocks != null && blocks.length > 0) {
                BlockLocation block = blocks[0];
                // create a split for the input
                if (productInventory == null) {
                    // no inventory, process whole product
                    splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts()));
                } else {
                    ProductInventoryEntry entry = productInventory.getEntry(file.getPath().getName());
                    if (entry != null && entry.getProcessLength() > 0) {
                        // when listed process the given subset
                        int start = entry.getProcessStartLine();
                        int length = entry.getProcessLength();
                        splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts(), start, length));
                    } else if (entry == null) {
                        // when not listed process whole product
                        splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts()));
                    }
                }
            } else {
                String msgFormat = "Failed to retrieve block location for file '%s'. Ignoring it.";
                throw new IOException(String.format(msgFormat, file.getPath()));
            }
        }
    }

    private static FileStatus[] getFileStatuses(HdfsInventoryService inventoryService, String inputPathPatterns,
                                                Date minDate, Date maxDate, String regionName) throws IOException {
        InputPathResolver inputPathResolver = new InputPathResolver();
        inputPathResolver.setMinDate(minDate);
        inputPathResolver.setMaxDate(maxDate);
        inputPathResolver.setRegionName(regionName);
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return inventoryService.globFileStatuses(inputPatterns);
    }

    /**
     * Creates a {@link NoRecordReader} because records are not used with this input format.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context) throws IOException,
                                                                                                          InterruptedException {
        return new NoRecordReader();
    }
}
