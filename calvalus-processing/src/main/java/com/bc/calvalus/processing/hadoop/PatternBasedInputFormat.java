package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.FileSystemPathIterator;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.geodb.GeodbInputFormat;
import com.bc.calvalus.processing.geodb.GeodbScanMapper;
import com.bc.calvalus.processing.productinventory.ProductInventory;
import com.bc.calvalus.processing.productinventory.ProductInventoryEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
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
import java.util.Iterator;
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
        int requestSizeLimit = conf.getInt(JobConfigNames.CALVALUS_REQUEST_SIZE_LIMIT, 0);
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
        String regionName = conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME);
        String dateRangesString = conf.get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES);
        String geoInventory = conf.get(JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY);
        Set<String> productIdentifiers = new HashSet<>(conf.getStringCollection(
                    JobConfigNames.CALVALUS_INPUT_PRODUCT_IDENTIFIERS));

        List<InputSplit> splits;
        if (geoInventory != null && inputPathPatterns == null) {
            Set<String> paths = GeodbInputFormat.queryGeoInventory(true, conf);
            LOG.info(String.format("%d files returned from geo-inventory '%s'.", paths.size(), geoInventory));
            if (!productIdentifiers.isEmpty()) {
                Iterator<String> pathIterator = paths.iterator();
                while (pathIterator.hasNext()) {
                    String path = pathIterator.next();
                    String filename = path.substring(path.lastIndexOf("/") + 1);
                    String filenameWithoutExtension = stripExtension(filename);
                    if (!productIdentifiers.contains(filenameWithoutExtension)) {
                        pathIterator.remove();
                    }
                }
                LOG.info(String.format("filtered using %d productIdentifiers: %d files remaining'.",
                                       productIdentifiers.size(), paths.size()));
            }
            splits = GeodbInputFormat.createInputSplits(conf, paths, requestSizeLimit);
            LOG.info(String.format("%d splits created.", splits.size()));
        } else if (geoInventory == null && inputPathPatterns != null) {

            JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
            HdfsFileSystemService hdfsFileSystemService = new HdfsFileSystemService(jobClientsMap);

            ProductInventory productInventory = ProductInventory.createInventory(conf);
            splits = new ArrayList<>(1000);
            if (InputPathResolver.containsDateVariables(inputPathPatterns)) {
                List<DateRange> dateRanges = createDateRangeList(dateRangesString);
                for (DateRange dateRange : dateRanges) {
                    List<String> inputPatterns = getInputPatterns(inputPathPatterns, dateRange.getStartDate(),
                                                                  dateRange.getStopDate(), regionName);
                    RemoteIterator<LocatedFileStatus> fileStatusIt = getFileStatuses(hdfsFileSystemService,
                                                                                     inputPatterns, conf, null);
                    if (!productIdentifiers.isEmpty()) {
                        fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                    }
                    createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit);
                    if (requestSizeLimit > 0 && splits.size() >= requestSizeLimit) {
                        splits = splits.subList(0, requestSizeLimit);
                        break;
                    }
                }
            } else {
                List<String> inputPatterns = getInputPatterns(inputPathPatterns, null, null, regionName);
                RemoteIterator<LocatedFileStatus> fileStatusIt = getFileStatuses(hdfsFileSystemService, inputPatterns,
                                                                                 conf, null);
                if (!productIdentifiers.isEmpty()) {
                    fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                }
                createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit);
            }
        } else if (geoInventory != null && inputPathPatterns != null) {
            // --> update index: splits for all products that are NOT in the geoDB
            Set<String> pathInDB = GeodbInputFormat.queryGeoInventory(false, conf);
            JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
            HdfsFileSystemService hdfsFileSystemService = new HdfsFileSystemService(jobClientsMap);

            ProductInventory productInventory = ProductInventory.createInventory(conf);
            splits = new ArrayList<>(1000);
            if (InputPathResolver.containsDateVariables(inputPathPatterns)) {
                List<DateRange> dateRanges = createDateRangeList(dateRangesString);
                for (DateRange dateRange : dateRanges) {
                    List<String> inputPatterns = getInputPatterns(inputPathPatterns, dateRange.getStartDate(),
                                                                  dateRange.getStopDate(), regionName);
                    RemoteIterator<LocatedFileStatus> fileStatusIt = getFileStatuses(hdfsFileSystemService,
                                                                                     inputPatterns, conf, pathInDB);
                    if (!productIdentifiers.isEmpty()) {
                        fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                    }
                    createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit);
                    if (requestSizeLimit > 0 && splits.size() >= requestSizeLimit) {
                        splits = splits.subList(0, requestSizeLimit);
                        break;
                    }
                }
            } else {
                List<String> inputPatterns = getInputPatterns(inputPathPatterns, null, null, regionName);
                RemoteIterator<LocatedFileStatus> fileStatusIt = getFileStatuses(hdfsFileSystemService, inputPatterns,
                                                                                 conf, pathInDB);
                if (!productIdentifiers.isEmpty()) {
                    fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                }
                createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit);
            }
        } else {
            throw new IOException(
                        String.format("Missing job parameter for inputFormat. Neither %s nor %s had been set.",
                                      JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS,
                                      JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY));
        }
        LOG.info("Total files to process : " + splits.size());
        return splits;
    }

    private RemoteIterator<LocatedFileStatus> filterUsingProductIdentifiers(
                RemoteIterator<LocatedFileStatus> fileStatusIt,
                Set<String> productIdentifiers) throws IOException {
        return new RemoteIterator<LocatedFileStatus>() {

            LocatedFileStatus next = getNext();

            @Override
            public boolean hasNext() throws IOException {
                return next != null;
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                LocatedFileStatus current = next;
                next = getNext();
                return current;
            }

            private LocatedFileStatus getNext() throws IOException {
                while (fileStatusIt.hasNext()) {
                    LocatedFileStatus fileStatus = fileStatusIt.next();
                    String filename = fileStatus.getPath().getName();
                    String filenameWithoutExtension = stripExtension(filename);
                    if (productIdentifiers.contains(filenameWithoutExtension)) {
                        return fileStatus;
                    }
                }
                return null;
            }
        };
    }

    private String stripExtension(String filename) {
        int index = filename.indexOf(".");
        if (index >= 0) {
            return filename.substring(0, index);
        }
        return filename;
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
                                RemoteIterator<LocatedFileStatus> fileStatusIt,
                                List<InputSplit> splits,
                                Configuration conf, int requestSizeLimit) throws IOException {
        while (fileStatusIt.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileStatusIt.next();
            InputSplit split = createSplit(productInventory, conf, locatedFileStatus);
            if (split != null) {
                splits.add(split);
                if (requestSizeLimit > 0 && splits.size() == requestSizeLimit) {
                    break;
                }
            }
        }
    }

    protected InputSplit createSplit(ProductInventory productInventory, Configuration conf, FileStatus file) throws
                                                                                                             IOException {
        long fileLength = file.getLen();

        BlockLocation[] blocks;
        if (file instanceof LocatedFileStatus) {
            blocks = ((LocatedFileStatus) file).getBlockLocations();
        } else {
            FileSystem fs = file.getPath().getFileSystem(conf);
            blocks = fs.getFileBlockLocations(file, 0, fileLength);
        }

        if (blocks != null && blocks.length > 0) {
            BlockLocation block = blocks[0];
            // create a split for the input
            if (productInventory == null) {
                // no inventory, process whole product
                return new ProductSplit(file.getPath(), fileLength, block.getHosts());
            } else {
                ProductInventoryEntry entry = productInventory.getEntry(file.getPath().getName());
                if (entry != null && entry.getProcessLength() > 0) {
                    // when listed process the given subset
                    int start = entry.getProcessStartLine();
                    int length = entry.getProcessLength();
                    return new ProductSplit(file.getPath(), fileLength, block.getHosts(), start, length);
                } else if (entry == null) {
                    // when not listed process whole product
                    return new ProductSplit(file.getPath(), fileLength, block.getHosts());
                }
            }
        } else {
            String msgFormat = "Failed to retrieve block location for file '%s'. Ignoring it.";
            throw new IOException(String.format(msgFormat, file.getPath()));
        }
        return null;
    }

    protected RemoteIterator<LocatedFileStatus> getFileStatuses(HdfsFileSystemService fileSystemService,
                                                                List<String> inputPatterns,
                                                                Configuration conf,
                                                                Set<String> existingPathes) throws IOException {
        FileSystemPathIterator.FileStatusFilter extraFilter = null;
        if (existingPathes != null && existingPathes.size() > 0) {
            extraFilter = fileStatus -> {
                String dbPath = GeodbScanMapper.getDBPath(fileStatus.getPath(), conf);
                return !existingPathes.contains(dbPath);
            };
        }
        return fileSystemService.globFileStatusIterator(inputPatterns, conf, extraFilter);
    }

    protected List<String> getInputPatterns(String inputPathPatterns, Date minDate, Date maxDate, String regionName) {
        return InputPathResolver.getInputPathPatterns(inputPathPatterns, minDate, maxDate, regionName);
    }

    /**
     * Creates a {@link NoRecordReader} because records are not used with this input format.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context)
                throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
