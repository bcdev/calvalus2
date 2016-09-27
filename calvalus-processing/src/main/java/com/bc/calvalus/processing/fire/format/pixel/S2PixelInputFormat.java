package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.PixelProductArea;
import com.bc.calvalus.processing.fire.format.SensorStrategy;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.Text;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.bc.calvalus.processing.fire.format.CommonUtils.getMerisTile;

/**
 * @author thomas
 */
public class S2PixelInputFormat extends InputFormat {

    private PixelProductArea area;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        SensorStrategy strategy = CommonUtils.getStrategy(conf.get("calvalus.sensor"));
        area = strategy.getArea(conf.get("area"));
        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        String inputPathPattern = getInputPathPattern(year, month, area);
        CalvalusLogger.getLogger().info("Input path pattern = " + inputPathPattern);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(jobClientsMap, "eodata");

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFileStatuses(hdfsInventoryService, inputPathPattern, conf);

        createSplits(fileStatuses, splits, conf, hdfsInventoryService);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    static String getInputPathPattern(String year, String month, PixelProductArea area) throws IOException {
        String granules = getGranules(area);
        return String.format("hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-.%s-%s%s.*nc", granules, year, month);
    }

    private static String getLcInputPathPattern(String year) {
        return String.format("hdfs://calvalus/calvalus/projects/fire/aux/s2-lc/%s.nc", lcYear(Integer.parseInt(year)));
    }

    private void createSplits(FileStatus[] fileStatuses, List<InputSplit> splits, Configuration conf, HdfsInventoryService hdfsInventoryService) throws IOException {
        List<String> usedGranules = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
            FileStatus lcPath = getLcFileStatus(path, path.getFileSystem(conf));
            filePaths.add(lcPath.getPath());
            fileLengths.add(lcPath.getLen());
            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
            usedGranules.add(getGranule(path.toString()));
        }

        String year = conf.get("calvalus.year");
        String lcInputPathPattern = getLcInputPathPattern(year);
        FileStatus lcFileStatus = getFileStatuses(hdfsInventoryService, lcInputPathPattern, conf)[0];
        for (String granule : getGranules(area).split("|")) {
            if (!usedGranules.contains(granule)) {
                List<Path> filePaths = new ArrayList<>();
                List<Long> fileLengths = new ArrayList<>();
                // dummy for BA input
                filePaths.add(new Path("dummy"));
                fileLengths.add(0L);
                filePaths.add(lcFileStatus.getPath());
                fileLengths.add(lcFileStatus.getLen());
                splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                        fileLengths.stream().mapToLong(Long::longValue).toArray()));
            }
        }
    }

    static String getGranule(String path) {
        // hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-T32NPN-20160210T095818.nc
        return path.substring(path.indexOf("BA-T") + 4, path.indexOf("BA-T") + 9);
    }

    private static String getGranules(PixelProductArea area) throws IOException {
        URL resource = S2PixelInputFormat.class.getResource("area-to-granules.xml");
        Document document;
        try {
            document = new SAXBuilder().build(resource);
        } catch (JDOMException e) {
            throw new IOException(e);
        }

        XPathFactory xpathFactory = XPathFactory.instance();
        String path = String.format("//POLYGON[@minLon=%s][@minLat=%s]/tiles/text()", area.left, area.top);
        XPathExpression<Object> xpath = xpathFactory.compile(path);

        List<Object> list = xpath.evaluate(document.getRootElement());
        if (list.size() > 1) {
            throw new IllegalStateException("path '" + path + "' matches more than one set of granules.");
        } else if (list.size() == 0) {
            throw new IllegalStateException("path '" + path + "' does not match any set of granules.");
        }

        return ((Text) list.get(0)).getValue().replace(",", "|");
    }

    private FileStatus getLcFileStatus(Path path, FileSystem fileSystem) throws IOException {
        String baInputPath = path.toString(); // hdfs://calvalus/calvalus/projects/fire/meris-ba/$year/BA_PIX_MER_$tile_$year$month_v4.0.tif
        String lcInputPath = getLcInputPath(baInputPath);
        return fileSystem.getFileStatus(new Path(lcInputPath));
    }

    private static String getLcInputPath(String baInputPath) {
        int yearIndex = baInputPath.indexOf("meris-ba/") + "meris-ba/".length();
        int year = Integer.parseInt(baInputPath.substring(yearIndex, yearIndex + 4));
        String lcYear = lcYear(year);
        String tile = getMerisTile(baInputPath);
        return baInputPath.substring(0, baInputPath.indexOf("meris-ba")) + "aux/lc/" + String.format("lc-%s-%s.nc", lcYear, tile);
    }

    private static String lcYear(int year) {
        // 2002 -> 2000
        // 2003 - 2007 -> 2005
        // 2008 - 2012 -> 2010
        switch (year) {
            case 2002:
            case 2003:
            case 2004:
            case 2005:
            case 2006:
            case 2007:
                return "2000";
            case 2008:
            case 2009:
            case 2010:
            case 2011:
            case 2012:
                return "2005";
            case 2013:
            case 2014:
            case 2015:
                return "2010";
        }
        throw new IllegalArgumentException("Illegal year: " + year);
    }

    private FileStatus[] getFileStatuses(HdfsInventoryService inventoryService,
                                         String inputPathPatterns,
                                         Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return inventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
