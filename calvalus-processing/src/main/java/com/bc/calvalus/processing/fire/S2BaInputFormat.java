package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S2BaInputFormat extends InputFormat {

    private static final int MAX_PRE_IMAGES_COUNT_SINGLE_ORBIT = 4;
    private static final int MAX_PRE_IMAGES_COUNT_MULTI_ORBIT = 8;
    private static final int DEFAULT_SEARCH_CHUNK_SIZE = 20;
    private final Logger logger = Logger.getLogger("com.bc.calvalus");
    private static NamespaceContext ATOM_NAMESPACE_CONTEXT = new AtomNamespaceContext();

    private String outputDir;
    private String sensor;
    private JobContext jobContext;

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        this.jobContext = jobContext;
        Configuration conf = this.jobContext.getConfiguration();
        String catalogueParam = conf.get("calvalus.input.geoInventory");
        String tile = conf.get("calvalus.tile");
        outputDir = conf.get("calvalus.output.dir");
        sensor = conf.get("calvalus.sensor");
        if (!sensor.equals("S2A") && !sensor.equals("S2B")) {
            throw new IllegalArgumentException("Wrong value for sensor: '" + sensor + "', must be one of 'S2A', 'S2B'");
        }

        Set<InputSplit> splits = new HashSet<>(1000);
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));

        /*

            1) get all input files for the current tile
            2) for each file f, produce a set of splits
                - Each split consists of two files: pre and post period
                - post date is date of f
                - create splits with f and latest 4 or 8 (depending on orbit) previous files
         */

        String[] productArchivePaths = getProductArchivePaths(catalogueParam, conf);
        for (String productArchivePath : productArchivePaths) {
            System.out.println(productArchivePath);
        }
        createSplits(productArchivePaths, tile, splits, conf);
        logger.info(String.format("Created %d split(s).", splits.size()));
        return Arrays.asList(splits.toArray(new InputSplit[0]));


        /**
         HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);
         InputPathResolver inputPathResolver = new InputPathResolver();
         List<String> inputPatterns = inputPathResolver.resolve(catalogueParam);

         // for each split, a single mapper is run
         // --> fileStatuses must contain filestatus for each input product at this stage
         createSplits(fileStatuses, tile, splits, hdfsInventoryService, conf);
         // here, each split must contain two files: pre and post period.
         Logger.getLogger("com.bc.calvalus").info(String.format("Created %d split(s).", splits.size()));
         return Arrays.asList(splits.toArray(new InputSplit[0]));
         */
    }

    private String[] getProductArchivePaths(String catalogueParam, Configuration conf) throws IOException {
        final Map<String, String> searchParameters = parseSearchParameters(catalogueParam);
        final String provider = searchParameters.get("catalogue");
        final String searchUrlTemplate = conf.get("calvalus." + provider + ".searchurl");
        final String searchXPath = conf.get("calvalus." + provider + ".searchxpath");
        final String searchCredentials = conf.get("calvalus." + provider + ".searchcredentials");
        final Pattern pathPattern = conf.get("calvalus." + provider + ".pathpattern") != null
                ? Pattern.compile(conf.get("calvalus." + provider + ".pathpattern")) : null;
        final String pathReplacement = conf.get("calvalus." + provider + ".pathreplacement");
        String dateRangesString = conf.get("calvalus.input.dateRanges");
        final List<DateRange> dateRanges = createDateRangeList(dateRangesString);

        final HttpClient httpClient = new HttpClient();
        final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);
        docFactory.setValidating(false);
        final XPathFactory xPathfactory = XPathFactory.newInstance();
        List<String> splits = new ArrayList<>(1000);
        int numQueries = 0;
        long t0 = System.currentTimeMillis();

        // date ranges loop
        for (DateRange dateRange : dateRanges) {
            searchParameters.put("start", DateUtils.formatDate(dateRange.getStartDate()));
            searchParameters.put("stop", DateUtils.formatDate(dateRange.getStopDate()));
            searchParameters.put("startmillis", String.valueOf(dateRange.getStartDate().getTime()));
            searchParameters.put("stopmillis", String.valueOf(dateRange.getStopDate().getTime() + 86399000));

            // incremental query loop
            int offset = 0;
            while (true) {
                searchParameters.put("offset", String.valueOf(offset));
                searchParameters.put("offset1", String.valueOf(offset + 1));
                searchParameters.put("count", String.valueOf(DEFAULT_SEARCH_CHUNK_SIZE));
                final String searchUrl = urlEncode(replaceSearchParameters(searchUrlTemplate, searchParameters));

                if (offset == 0) {
                    logger.info(searchUrl);
                }
                final GetMethod catalogueRequest = new GetMethod(searchUrl);
                if (searchCredentials != null) {
                    catalogueRequest.setRequestHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(searchCredentials.getBytes(StandardCharsets.UTF_8)));
                }
                final InputStream response = inquireCatalogue(httpClient, catalogueRequest);
                numQueries++;

                if (searchXPath != null) {
                    try {
                        NodeList pathNodes = parseCatalogueResponse(docFactory, xPathfactory, response, searchXPath);

                        // search results loop
                        int count = 0;
                        for (int i = 0; i < pathNodes.getLength(); ++i) {
                            String productArchivePath = pathNodes.item(i).getTextContent();
                            if (pathPattern != null && pathReplacement != null) {
                                productArchivePath = replacePathPattern(productArchivePath, pathPattern, pathReplacement);
                            }
                            logger.info(productArchivePath);
                            splits.add(productArchivePath);
                            count++;
                        }
                        if (count < DEFAULT_SEARCH_CHUNK_SIZE) {
                            break;
                        }
                        offset += count;
                    } catch (SAXException | XPathExpressionException | ParserConfigurationException e) {
                        throw new IOException(e);
                    }
                    catalogueRequest.releaseConnection();
                }
            }
        }
        logger.info(String.format("%d splits created.", splits.size()));
        logger.info("catalogue query " + numQueries + " cycles done in [ms]: " + (System.currentTimeMillis() - t0));

        return splits.toArray(new String[0]);
    }

    private String replacePathPattern(String productArchivePath, Pattern pathPattern, String pathReplacement) {
        Matcher matcher = pathPattern.matcher(productArchivePath);
        productArchivePath = matcher.replaceAll(pathReplacement);
        return productArchivePath;
    }

    private NodeList parseCatalogueResponse(DocumentBuilderFactory factory, XPathFactory xPathfactory, InputStream response, String searchXPath) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        response = new TokenReplacingStream(response,
                "xmlns:media=".getBytes(),
                "xmlns:resto=\"http://whereeverrestoresides\" xmlns:media=".getBytes());
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(response);
        XPath xpath = xPathfactory.newXPath();
        xpath.setNamespaceContext(ATOM_NAMESPACE_CONTEXT);
        XPathExpression expr = xpath.compile(searchXPath);
        return (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
    }

    private String urlEncode(String searchUrl) {
        return searchUrl.
                replaceAll(" ", "%20").
                replaceAll("\"", "%22").
                replaceAll("\\(", "%28").
                replaceAll("\\)", "%29").
                replaceAll("\\[", "%5B").
                replaceAll("\\]", "%5D").
                replaceAll("<", "%3C").
                replaceAll(">", "%3E");
    }

    private String replaceSearchParameters(String searchUrl,
                                           Map<String, String> searchParameters) {
        for (Map.Entry<String, String> param : searchParameters.entrySet()) {
            searchUrl = searchUrl.replaceAll("\\$\\{" + param.getKey() + "\\}", param.getValue());
        }
        return searchUrl;
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

    private InputStream inquireCatalogue(HttpClient httpClient, GetMethod getMethod) throws IOException {
        int statusCode = httpClient.executeMethod(getMethod);
        if (statusCode > 299) {
            String message = getMethod.getResponseBodyAsString();
            throw new IOException("search error: " + message + " query: " + getMethod.getQueryString());
        }
        return getMethod.getResponseBodyAsStream();
    }

    private void createSplits(String[] productArchivePaths, String tile, Set<InputSplit> splits, Configuration conf) throws IOException {
        /*
        for each file status r:
            take r and (up to) latest 4 or 8 matching files d, c, b, a (getPeriodStatuses)
                create r, d
                create r, c
                create r, b
                create r, a
         */
        for (String referencePath : productArchivePaths) {
            // if there already exists a file, like BA-T37NCF-20160514T075819.nc: continue
            Date referenceDate = getDate(referencePath);
            String path = "s3://calvalus/" + outputDir + "/" + tile + "/" + sensor + "-BA-" + tile + "-" + new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(referenceDate) + ".nc";
//            logger.info("Checking if BA output file '" + path + "' already exists...");
//            if (hdfsInventoryService.pathExists(path, "cvop")) {
//                logger.info("already exists, moving to next reference date.");
//                continue;
//            }

            // new ProductSplit(new Path(productArchivePath), -1, null)

//            logger.info("does not already exist, create splits accordingly.");
//            String[] periodPaths = getPeriodPaths(referencePath, conf);
//            for (String periodPath : periodPaths) {
//                splits.add(createSplit(referencePath, periodPath));
//                logger.info(String.format("Created split with postStatus %s and preStatus %s.", referenceDate, getDate(periodPath)));
//            }
        }
    }

    private void createSplits(FileStatus[] fileStatuses, String tile,
                              Set<InputSplit> splits, HdfsFileSystemService hdfsInventoryService, Configuration conf) throws IOException {
        /*
        for each file status r:
            take r and (up to) latest 4 or 8 matching files d, c, b, a (getPeriodStatuses)
                create r, d
                create r, c
                create r, b
                create r, a
         */
        for (FileStatus referenceFileStatus : fileStatuses) {
            // if there already exists a file, like BA-T37NCF-20160514T075819.nc: continue
            Date referenceDate = getDate(referenceFileStatus);
            String postDate = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(referenceDate);
            String path = "hdfs://calvalus/" + outputDir + "/" + tile + "/" + sensor + "-BA-" + tile + "-" + postDate + ".nc";
            logger.info("Checking if BA output file '" + path + "' already exists...");
            if (hdfsInventoryService.pathExists(path, "cvop")) {
                logger.info("already exists, moving to next reference date.");
                continue;
            }
            logger.info("does not already exist, create splits accordingly.");
            FileStatus[] periodStatuses = getPeriodStatuses(referenceFileStatus, hdfsInventoryService, conf);
            for (FileStatus preStatus : periodStatuses) {
                splits.add(createSplit(referenceFileStatus, preStatus));
                logger.info(String.format("Created split with postStatus %s and preStatus %s.", referenceDate, getDate(preStatus)));
            }
        }
    }

    private Map<String, String> parseSearchParameters(String catalogueParam) {
        Map<String, String> searchParameters = new HashMap<>();
        for (String param : catalogueParam.split("&")) {
            int p = param.indexOf('=');
            searchParameters.put(param.substring(0, p), param.substring(p + 1));
        }
        return searchParameters;
    }

    private static CombineFileSplit createSplit(FileStatus postFileStatus, FileStatus preStatus) {
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        filePaths.add(postFileStatus.getPath());
        fileLengths.add(postFileStatus.getLen());
        filePaths.add(preStatus.getPath());
        fileLengths.add(preStatus.getLen());
        return new ComparableCombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
    }

    private FileStatus[] getPeriodStatuses(FileStatus referenceFileStatus, HdfsFileSystemService hdfsInventoryService, Configuration conf) throws IOException {
        String referencePath = referenceFileStatus.getPath().toString();
        String tilePathPattern = getTilePathPattern(referencePath);

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(tilePathPattern);
        FileStatus[] periodStatuses = hdfsInventoryService.globFiles(jobContext.getUser(), inputPatterns);
        sort(periodStatuses);

        List<FileStatus> filteredList = new ArrayList<>();
        for (FileStatus periodStatus : periodStatuses) {
            if (getDate(periodStatus).getTime() < getDate(referenceFileStatus).getTime()) {
                filteredList.add(periodStatus);
            }
        }

        int maxPreImagesCount = getMaxPreImagesCount(filteredList);
        int resultCount = Math.min(maxPreImagesCount, filteredList.size());
        FileStatus[] result = new FileStatus[resultCount];
        System.arraycopy(filteredList.toArray(new FileStatus[0]), 0, result, 0, resultCount);
        return result;
    }

    private String[] getPeriodPaths(String referencePath) throws IOException {
        String tilePathPattern = getTilePathPattern(referencePath);

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(tilePathPattern);
//        FileStatus[] periodStatuses = hdfsInventoryService.globFiles(jobContext.getUser(), inputPatterns);
//        sort(periodStatuses);

        List<FileStatus> filteredList = new ArrayList<>();
//        for (FileStatus periodStatus : periodStatuses) {
//            if (getDate(periodStatus).getTime() < getDate(referenceFileStatus).getTime()) {
//                filteredList.add(periodStatus);
//            }
//        }

        int maxPreImagesCount = getMaxPreImagesCount(filteredList);
        int resultCount = Math.min(maxPreImagesCount, filteredList.size());
        String[] result = new String[resultCount];
        System.arraycopy(filteredList.toArray(new FileStatus[0]), 0, result, 0, resultCount);
        return result;
    }

    static int getMaxPreImagesCount(List<FileStatus> filteredList) {
        Logger.getLogger("com.bc.calvalus").info("Computing max pre images count...");
        String orbit = null;
        for (FileStatus fileStatus : filteredList) {
            if (orbit == null) {
                orbit = fileStatus.getPath().getName().substring(34, 39);
            } else {
                if (!orbit.equals(fileStatus.getPath().getName().substring(34, 39))) {
                    Logger.getLogger("com.bc.calvalus").info("...other file found with different orbit, so it is 8.");
                    return MAX_PRE_IMAGES_COUNT_MULTI_ORBIT;
                }
            }
        }
        Logger.getLogger("com.bc.calvalus").info("...no file found with different orbit, so it is 4.");
        return MAX_PRE_IMAGES_COUNT_SINGLE_ORBIT;
    }

    private static void sort(FileStatus[] periodStatuses) {
        Arrays.sort(periodStatuses, (fs1, fs2) -> getDate(fs1).getTime() > getDate(fs2).getTime() ? -1 : 1);
    }

    private static Date getDate(String path) {
        String datePart = path.split("_")[2];

        try {
            return new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(datePart);
        } catch (ParseException e) {
            throw new IllegalStateException("Programming error in input format, see wrapped exception", e);
        }
    }

    private static Date getDate(FileStatus fs) {
        String fsName = fs.getPath().getName();
        String datePart = fsName.split("_")[2];

        try {
            return new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(datePart);
        } catch (ParseException e) {
            throw new IllegalStateException("Programming error in input format, see wrapped exception", e);
        }
    }

    static String getTilePathPattern(String s2PrePath) {
        int startIndex;
        if (s2PrePath.contains("S2A_MSIL2A")) {
            startIndex = s2PrePath.indexOf("S2A_MSIL2A") - "/YYYY/MM/DD".length();
        } else {
            startIndex = s2PrePath.indexOf("S2B_MSIL2A") - "/YYYY/MM/DD".length();
        }
        if (startIndex == -1) {
            throw new IllegalArgumentException("Invalid path for S2 L2 product: " + s2PrePath);
        }
        String tile = s2PrePath.split("/")[s2PrePath.split("/").length - 1].split("_")[5];
        String basePath = s2PrePath.substring(0, startIndex);
        return String.format("%s.*/.*/.*/.*%s.*.zip", basePath, tile);
    }

    @SuppressWarnings("WeakerAccess")
    public static class ComparableCombineFileSplit extends CombineFileSplit {

        @SuppressWarnings("unused")
        public ComparableCombineFileSplit() {
        }

        public ComparableCombineFileSplit(Path[] files, long[] lengths) {
            super(files, lengths);
        }

        @Override
        public boolean equals(Object obj) {
            return this.toString().equals(obj.toString());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(toString().toCharArray());
        }
    }

    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new RecordReader() {
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
            }

            public boolean nextKeyValue() {
                return false;
            }

            public Object getCurrentKey() {
                return null;
            }

            public Object getCurrentValue() {
                return null;
            }

            public float getProgress() {
                return 0;
            }

            public void close() {
            }
        };
    }

    /**
     * from https://stackoverflow.com/questions/7743534/filter-search-and-replace-array-of-bytes-in-an-inputstream
     */

    public class TokenReplacingStream extends InputStream {

        private final InputStream source;
        private final byte[] oldBytes;
        private final byte[] newBytes;
        private int tokenMatchIndex = 0;
        private int bytesIndex = 0;
        private boolean unwinding;
        private int mismatch;

        public TokenReplacingStream(InputStream source, byte[] oldBytes, byte[] newBytes) {
            assert oldBytes.length > 0;
            this.source = source;
            this.oldBytes = oldBytes;
            this.newBytes = newBytes;
        }

        @Override
        public int read() throws IOException {
            if (unwinding) {
                if (bytesIndex < tokenMatchIndex) {
                    return oldBytes[bytesIndex++];
                } else {
                    bytesIndex = 0;
                    tokenMatchIndex = 0;
                    unwinding = false;
                    return mismatch;
                }
            } else if (tokenMatchIndex == oldBytes.length) {
                if (bytesIndex == newBytes.length) {
                    bytesIndex = 0;
                    tokenMatchIndex = 0;
                } else {
                    return newBytes[bytesIndex++];
                }
            }

            int b = source.read();
            if (b == oldBytes[tokenMatchIndex]) {
                tokenMatchIndex++;
            } else if (tokenMatchIndex > 0) {
                mismatch = b;
                unwinding = true;
            } else {
                return b;
            }

            return read();

        }

        @Override
        public void close() throws IOException {
            source.close();
        }
    }

    private static class AtomNamespaceContext implements NamespaceContext {
        @Override
        public String getNamespaceURI(String prefix) {
            if ("a".equals(prefix))
                return "http://www.w3.org/2005/Atom";
            if ("m".equals(prefix))
                return "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
            if ("d".equals(prefix))
                return "http://schemas.microsoft.com/ado/2007/08/dataservices";
            if ("odata".equals(prefix))
                return "https://scihub.copernicus.eu/dhus/odata/v1/";
            throw new IllegalArgumentException(prefix);
        }

        @Override
        public String getPrefix(String namespaceURI) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<String> getPrefixes(String namespaceURI) {
            throw new UnsupportedOperationException();
        }
    }
}
