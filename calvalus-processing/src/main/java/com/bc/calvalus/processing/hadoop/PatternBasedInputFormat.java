package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.inventory.hadoop.FileSystemPathIteratorFactory;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.geodb.GeodbInputFormat;
import com.bc.calvalus.processing.geodb.GeodbScanMapper;
import com.bc.calvalus.processing.productinventory.ProductInventory;
import com.bc.calvalus.processing.productinventory.ProductInventoryEntry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.esa.snap.core.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final int DEFAULT_SEARCH_CHUNK_SIZE = 20;
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF = new TypeReference<Map<String, Object>>() {};

    static class AtomNamespaceContext implements NamespaceContext {
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
    private static NamespaceContext ATOM_NAMESPACE_CONTEXT = new AtomNamespaceContext();

    /**
     * Maps each input file to a single (file) split.
     * <p/>
     * Input files are given by the configuration parameter
     * {@link com.bc.calvalus.processing.JobConfigNames#CALVALUS_INPUT_PATH_PATTERNS}. Its value is expected to
     * be a comma-separated list of file path patterns (HDFS URLs). These patterns can contain dates and region names.
     *
     * Alternatively, inputs are specified by geoInventory, dateRange, and regionGeoemtry.
     *
     * Alternatively, inputs are specified by catalogue, searchurl, searchxpath:
     *     "catalogue"        : "provider=mundi&collection=Sentinel2&type=MSIL1C&level=L1C",
     *    "catalogue.mundi.searchurl"     : "https://mundiwebservices.com/acdc/catalog/proxy/search/${collection}/opensearch?q=(sensingStartDate:[${start}T00:00:00Z%20TO%20${stop}T23:59:59Z]%20AND%20footprint:"Intersects(${polygon})")&processingLevel=${level}&startIndex=${offset}&maxRecords=${count}",
     *    "catalogue.mundi.searchxpath" : "concat('s3a:/',substring-after(/a:feed/a:entry/a:link[@rel="enclosure"]/@href,'https://obs.eu-de.otc.t-systems.com'))",
     *
     *    "dateRanges"       : "[2018-04-24:2018-04-24]",
     *    "regionGeometry"   : "POLYGON((10.624552319335976 45.58429059451441,10.719458959961003 45.58525169665108,10.71877231445319 45.55964882024322,10.625238964843788 45.5586872798687,10.624552319335976 45.58429059451441))",
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
        long t0 = System.currentTimeMillis();

        if (geoInventory != null && ! geoInventory.startsWith("catalogue") && inputPathPatterns == null) {

            // geo-inventory

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
            LOG.info("geo-inventory query done in [ms]: " + (System.currentTimeMillis() - t0));

        } else if (geoInventory == null && inputPathPatterns != null) {

            // input paths

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
                                                                                     inputPatterns, conf, null, true);
                    LOG.info("query for " + dateRange + " done");
                    if (!productIdentifiers.isEmpty()) {
                        fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                    }
                    createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit, true);
                    if (requestSizeLimit > 0 && splits.size() >= requestSizeLimit) {
                        splits = splits.subList(0, requestSizeLimit);
                        break;
                    }
                }
            } else {
                List<String> inputPatterns = getInputPatterns(inputPathPatterns, null, null, regionName);
                RemoteIterator<LocatedFileStatus> fileStatusIt = getFileStatuses(hdfsFileSystemService, inputPatterns,
                                                                                 conf, null, true);
                if (!productIdentifiers.isEmpty()) {
                    fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                }
                createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit, true);
            }
            LOG.info("file system query done in [ms]: " + (System.currentTimeMillis() - t0));

        } else if (geoInventory != null && ! geoInventory.startsWith("catalogue") && inputPathPatterns != null) {

            // update geo-index: splits for all products that are NOT in the geoDB

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
                                                                                     inputPatterns, conf, pathInDB, false);
                    if (!productIdentifiers.isEmpty()) {
                        fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                    }
                    createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit, false);
                    if (requestSizeLimit > 0 && splits.size() >= requestSizeLimit) {
                        splits = splits.subList(0, requestSizeLimit);
                        break;
                    }
                }
            } else {
                List<String> inputPatterns = getInputPatterns(inputPathPatterns, null, null, regionName);
                RemoteIterator<LocatedFileStatus> fileStatusIt = getFileStatuses(hdfsFileSystemService, inputPatterns,
                                                                                 conf, pathInDB, false);
                if (!productIdentifiers.isEmpty()) {
                    fileStatusIt = filterUsingProductIdentifiers(fileStatusIt, productIdentifiers);
                }
                createSplits(productInventory, fileStatusIt, splits, conf, requestSizeLimit, false);
            }
            LOG.info("geo-inventory query and complementary file system query done in [ms]: " + (System.currentTimeMillis() - t0));

        } else if (geoInventory != null && geoInventory.startsWith("catalogue")) {

            // catalogue query

            final Map<String, String> searchParameters = parseSearchParameters(geoInventory);
            final String provider = searchParameters.get("catalogue");
            final String searchUrlTemplate = conf.get("calvalus." + provider + ".searchurl");
            final String searchXPath = conf.get("calvalus." + provider + ".searchxpath");
            final String searchJPath = conf.get("calvalus." + provider + ".searchjpath");
            final String searchCredentials = conf.get("calvalus." + provider + ".searchcredentials");
            final Pattern pathPattern = conf.get("calvalus." + provider + ".pathpattern") != null
                    ? Pattern.compile(conf.get("calvalus." + provider + ".pathpattern")) : null;
            final String pathReplacement = conf.get("calvalus." + provider + ".pathreplacement");
            final String geometryWkt = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
            final List<DateRange> dateRanges = createDateRangeList(dateRangesString);

            if (geometryWkt != null) {
                searchParameters.put("polygon", geometryWkt);
            } else {
                searchParameters.put("polygon", "POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))");
            }

            final HttpClient httpClient = new HttpClient();
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            docFactory.setValidating(false);
            final XPathFactory xPathfactory = XPathFactory.newInstance();
            splits = new ArrayList<>(1000);
            int numQueries = 0;

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
                        LOG.info(searchUrl);
                    }
                    final GetMethod catalogueRequest = new GetMethod(searchUrl);
                    if (searchCredentials != null) {
                        catalogueRequest.setRequestHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(searchCredentials.getBytes(StandardCharsets.UTF_8)));
                    }
                    final InputStream response = inquireCatalogue(httpClient, catalogueRequest);
                    ++numQueries;

                    if (searchXPath != null) {
                        try {
                            NodeList pathNodes = parseCatalogueResponse(docFactory, xPathfactory, response, searchXPath);

                            // search results loop
                            int count = 0;
                            for (int i = 0; i < pathNodes.getLength() && (requestSizeLimit == 0 || splits.size() < requestSizeLimit); ++i) {
                                String productArchivePath = pathNodes.item(i).getTextContent();
                                if (pathPattern != null && pathReplacement != null) {
                                    productArchivePath = replacePathPattern(productArchivePath, pathPattern, pathReplacement);
                                }
                                System.out.println(productArchivePath);
                                splits.add(new ProductSplit(new Path(productArchivePath), -1, null));
                                ++count;
                            }
                            if (requestSizeLimit > 0 && splits.size() >= requestSizeLimit) {
                                LOG.info(String.format("query response truncated to request size limit %d", requestSizeLimit));
                                break;
                            }
                            if (count < DEFAULT_SEARCH_CHUNK_SIZE) {
                                break;
                            }
                            offset += count;
                        } catch (SAXException | XPathExpressionException | ParserConfigurationException e) {
                            throw new IOException(e);
                        }
                    } else if (searchJPath != null) {
                        List<String> paths = JsonPath.read(response, searchJPath);
                        int count = 0;
                        for (String path : paths) {
                            String productArchivePath = path;
                            if (pathPattern != null && pathReplacement != null) {
                                productArchivePath = replacePathPattern(productArchivePath, pathPattern, pathReplacement);
                            }
                            System.out.println(productArchivePath);
                            splits.add(new ProductSplit(new Path(productArchivePath), -1, null));
                            ++count;
                            if (requestSizeLimit > 0 && splits.size() >= requestSizeLimit) {
                                break;
                            }
                        }
                        if (requestSizeLimit > 0 && splits.size() >= requestSizeLimit) {
                            LOG.info(String.format("query response truncated to request size limit %d", requestSizeLimit));
                            break;
                        }
                        if (count < DEFAULT_SEARCH_CHUNK_SIZE) {
                            break;
                        }
                        offset += count;
                    } else {
                        throw new IllegalArgumentException("missing searchXPath or searchJPath configuration");
                    }
                    catalogueRequest.releaseConnection();
                }
            }
            LOG.info(String.format("%d splits created.", splits.size()));
            LOG.info("catalogue query " + numQueries + " cycles done in [ms]: " + (System.currentTimeMillis() - t0));

        } else if (! productIdentifiers.isEmpty()) {

            // TODO This is an abuse of calvalus.input.productIdentifiers for a path list that is not checked on the client side

            splits = new ArrayList<>();
            for (String identifier : productIdentifiers) {
                splits.add(new ProductSplit(new Path(identifier), -1, null));
            }

        } else {
            throw new IOException(
                        String.format("Missing job parameter for inputFormat. Neither %s nor %s nor %s had been set.",
                                      JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS,
                                      JobConfigNames.CALVALUS_INPUT_GEO_INVENTORY,
                                      JobConfigNames.CALVALUS_INPUT_PRODUCT_IDENTIFIERS));
        }
        LOG.info("Total files to process : " + splits.size());
        return splits;
    }

    private Map<String, String> parseSearchParameters(String catalogue) {
        Map<String,String> searchParameters = new HashMap<>();
        for (String param : catalogue.split("&")) {
            int p = param.indexOf('=');
            searchParameters.put(param.substring(0,p), param.substring(p+1));
        }
        return searchParameters;
    }


    private String replaceSearchParameters(String searchUrl,
                                           Map<String, String> searchParameters) {
        for (Map.Entry<String,String> param : searchParameters.entrySet()) {
            searchUrl = searchUrl.replaceAll("\\$\\{"+param.getKey()+"\\}", param.getValue());
        }
        return searchUrl;
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

    private InputStream inquireCatalogue(HttpClient httpClient, GetMethod getMethod) throws IOException {
        int statusCode = httpClient.executeMethod(getMethod);
        if (statusCode > 299) {
            String message = getMethod.getResponseBodyAsString();
            throw new IOException("search error: " + message + " query: " + getMethod.getQueryString());
        }
        return getMethod.getResponseBodyAsStream();
    }

    private NodeList parseCatalogueResponse(DocumentBuilderFactory factory, XPathFactory xPathfactory, InputStream response, String searchXPath) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        if (true) {  // TODO fix for CreoDias that returns XML with missing namespace declaration
            response = new TokenReplacingStream(response,
                                                "xmlns:media=".getBytes(),
                                                "xmlns:resto=\"http://whereeverrestoresides\" xmlns:media=".getBytes());
        }
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(response);
        XPath xpath = xPathfactory.newXPath();
        xpath.setNamespaceContext(ATOM_NAMESPACE_CONTEXT);
        XPathExpression expr = xpath.compile(searchXPath);
        return (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
    }

    private String replacePathPattern(String productArchivePath, Pattern pathPattern, String pathReplacement) {
        Matcher matcher = pathPattern.matcher(productArchivePath);
        productArchivePath = matcher.replaceAll(pathReplacement);
        return productArchivePath;
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

    protected RemoteIterator<LocatedFileStatus> mergedIterator(
                List<RemoteIterator<LocatedFileStatus>> iterators) throws IOException {
        return new RemoteIterator<LocatedFileStatus>() {
            int current = 0;

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
                while (current < iterators.size()) {
                    if (iterators.get(current).hasNext()) {
                        return iterators.get(current).next();
                    }
                    ++current;
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
                                Configuration conf, int requestSizeLimit, boolean withDirs) throws IOException {
        while (fileStatusIt.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileStatusIt.next();
            InputSplit split = createSplit(productInventory, conf, locatedFileStatus, withDirs);
            if (split != null) {
                splits.add(split);
                if (requestSizeLimit > 0 && splits.size() == requestSizeLimit) {
                    break;
                }
            }
        }
    }

    protected InputSplit createSplit(ProductInventory productInventory, Configuration conf, FileStatus file, boolean withDirs) throws
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
        } else if (withDirs) {
            return new ProductSplit(file.getPath(), 0, EMPTY_STRING_ARRAY);
        } else {
            String msgFormat = "Failed to retrieve block location for file '%s'. Ignoring it.";
            LOG.warning(String.format(msgFormat, file.getPath()));
            //throw new IOException(String.format(msgFormat, file.getPath()));
        }
        return null;
    }

    protected RemoteIterator<LocatedFileStatus> getFileStatuses(HdfsFileSystemService fileSystemService,
                                                                List<String> inputPatterns,
                                                                Configuration conf,
                                                                Set<String> existingPathes,
                                                                boolean withDirs) throws IOException {
        FileSystemPathIteratorFactory.FileStatusFilter extraFilter = null;
        if (existingPathes != null && existingPathes.size() > 0) {
            extraFilter = fileStatus -> {
                String dbPath = GeodbScanMapper.getDBPath(fileStatus.getPath(), conf);
                return !existingPathes.contains(dbPath);
            };
        }
        if (inputPatterns.size() <= 1 || startsWithWildcard(inputPatterns)) {
            return fileSystemService.globFileStatusIterator(inputPatterns, conf, extraFilter, withDirs, true);
        } else {
            // It was a bad idea to search for the common prefix. This may comprise much too many paths to descend
            List<RemoteIterator<LocatedFileStatus>> iters = new ArrayList<>();
            List<String> inputPattern = new ArrayList<>(1);
            for (String p : inputPatterns) {
                inputPattern.clear();
                inputPattern.add(p);
                iters.add(fileSystemService.globFileStatusIterator(inputPattern, conf, extraFilter, withDirs, true));
            }
            return mergedIterator(iters);
        }
    }

    protected boolean startsWithWildcard(List<String> inputPatterns) {
        int commonPrefixLength = AbstractFileSystemService.getCommonPathPrefix(inputPatterns).length();
        for (String pattern : inputPatterns) {
            if (pattern.length() < commonPrefixLength + 4 || ! "/.*/".equals(pattern.substring(commonPrefixLength, commonPrefixLength+4))) {
                return false;
            }
        }
        return true;
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

    /** from https://stackoverflow.com/questions/7743534/filter-search-and-replace-array-of-bytes-in-an-inputstream */

    public class TokenReplacingStream extends InputStream {

        private final InputStream source;
        private final byte[] oldBytes;
        private final byte[] newBytes;
        private int tokenMatchIndex = 0;
        private int bytesIndex = 0;
        private boolean unwinding;
        private int mismatch;
        private int numberOfTokensReplaced = 0;

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
                    numberOfTokensReplaced++;
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

        public int getNumberOfTokensReplaced() {
            return numberOfTokensReplaced;
        }

    }
}
