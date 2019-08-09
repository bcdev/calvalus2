package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.geodb.GeodbInputFormat;
import com.bc.calvalus.processing.productinventory.ProductInventory;
import com.bc.calvalus.processing.productinventory.ProductInventoryEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.esa.snap.core.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    protected static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Maps each input file to a single (file) split.
     * <p/>
     * Input files are given by the configuration parameter
     * {@link com.bc.calvalus.processing.JobConfigNames#CALVALUS_INPUT_PATH_PATTERNS}. Its value is expected to
     * be a comma-separated list of file path patterns (HDFS URLs). These patterns can contain dates and region names.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        // parse request
        Configuration conf = job.getConfiguration();
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
        String regionName = conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME);
        String dateRangesString = conf.get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES);
        String geoInventory = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);

        if (geoInventory != null && inputPathPatterns == null) {
            Set<String> paths = GeodbInputFormat.queryGeoInventory(true, conf);
            List<InputSplit> splits = GeodbInputFormat.createInputSplits(conf, paths);
            LOG.info(String.format("%d splits added (from %d returned from geo-inventory '%s').", splits.size(), paths.size(), geoInventory));
            return splits;
        } else if (geoInventory == null && inputPathPatterns != null) {

            JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
            HdfsFileSystemService hdfsFileSystemService = new HdfsFileSystemService(jobClientsMap);

            ProductInventory productInventory = ProductInventory.createInventory(conf);
            List<InputSplit> splits = new ArrayList<>(1000);
            if (InputPathResolver.containsDateVariables(inputPathPatterns)) {
                List<DateRange> dateRanges = createDateRangeList(dateRangesString);
                for (DateRange dateRange : dateRanges) {
                    FileStatus[] fileStatuses = getFileStatuses(hdfsFileSystemService,
                                                                inputPathPatterns,
                                                                dateRange.getStartDate(),
                                                                dateRange.getStopDate(),
                                                                regionName,
                                                                conf);
                    createSplits(productInventory, fileStatuses, splits, conf);
                }
            } else {
                FileStatus[] fileStatuses = getFileStatuses(hdfsFileSystemService,
                                                            inputPathPatterns,
                                                            null,
                                                            null,
                                                            regionName,
                                                            conf);
                createSplits(productInventory, fileStatuses, splits, conf);
            }

            LOG.info("Total files to process : " + splits.size());
            return splits;
        } else if (geoInventory != null && inputPathPatterns != null) {
            // --> update index: splits for all products that are NOT in the geoDB
            Set<String> pathStringInDB = GeodbInputFormat.queryGeoInventory(false, conf);
            Set<Path> qualifiedPath = makeQualified(pathStringInDB, conf);

            JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
            HdfsFileSystemService hdfsFileSystemService = new HdfsFileSystemService(jobClientsMap);

            ProductInventory productInventory = ProductInventory.createInventory(conf);
            List<InputSplit> splits = new ArrayList<>(1000);
            if (InputPathResolver.containsDateVariables(inputPathPatterns)) {
                List<DateRange> dateRanges = createDateRangeList(dateRangesString);
                for (DateRange dateRange : dateRanges) {
                    FileStatus[] fileStatuses = getFileStatuses(hdfsFileSystemService,
                                                                inputPathPatterns,
                                                                dateRange.getStartDate(),
                                                                dateRange.getStopDate(),
                                                                regionName,
                                                                conf);
                    fileStatuses = filterWithGeoDB(fileStatuses, qualifiedPath);
                    createSplits(productInventory, fileStatuses, splits, conf);
                }
            } else {
                FileStatus[] fileStatuses = getFileStatuses(hdfsFileSystemService,
                                                            inputPathPatterns,
                                                            null,
                                                            null,
                                                            regionName,
                                                            conf);
                fileStatuses = filterWithGeoDB(fileStatuses, qualifiedPath);
                createSplits(productInventory, fileStatuses, splits, conf);
            }

            LOG.info("Total files to process : " + splits.size());
            return splits;
        } else {
            throw new IOException(String.format("Missing job parameter for inputFormat. Neither %s nor %s had been set.",
                                                            JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS,
                                                            JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY));
        }
    }

    private Set<Path> makeQualified(Set<String> pathStringInDB, Configuration conf) throws IOException {
        Set<Path> qualifiedPath = new HashSet<>();
        for (String stringPath : pathStringInDB) {
            Path path = new Path(stringPath);
            qualifiedPath.add(path.getFileSystem(conf).makeQualified(path));
        }
        return qualifiedPath;
    }

    private FileStatus[] filterWithGeoDB(FileStatus[] fileStatuses, Set<Path> pathInDB) {
        List<FileStatus> remaining = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            Path fileStatusPath = fileStatus.getPath();
            if (!pathInDB.contains(fileStatusPath)) {
                remaining.add(fileStatus);
            }
        }
        return remaining.toArray(new FileStatus[0]);
    }

    private List<DateRange> createDateRangeList(String dateRangesString) throws IOException {
        List<DateRange> dateRanges = new ArrayList<>();
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


    protected void createSplits(ProductInventory productInventory,
                                FileStatus[] fileStatuses,
                                List<InputSplit> splits,
                                Configuration conf) throws IOException {
        for (FileStatus file : fileStatuses) {
            long fileLength = file.getLen();
            FileSystem fs = file.getPath().getFileSystem(conf);
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

    protected FileStatus[] getFileStatuses(HdfsFileSystemService fileSystemService,
                                           String inputPathPatterns,
                                           Date minDate,
                                           Date maxDate,
                                           String regionName,
                                           Configuration conf) throws IOException {
        InputPathResolver inputPathResolver = new InputPathResolver();
        inputPathResolver.setMinDate(minDate);
        inputPathResolver.setMaxDate(maxDate);
        inputPathResolver.setRegionName(regionName);
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return fileSystemService.globFileStatuses(inputPatterns, conf);
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
