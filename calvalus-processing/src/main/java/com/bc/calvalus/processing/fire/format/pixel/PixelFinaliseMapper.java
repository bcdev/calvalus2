package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.LcRemappingS2;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public abstract class PixelFinaliseMapper extends Mapper {

    private static final Logger LOG = CalvalusLogger.getLogger();
    static final int TILE_SIZE = 256;

    public static final String KEY_LC_PATH = "LC_PATH";
    public static final String KEY_VERSION = "VERSION";
    public static final String KEY_AREA_STRING = "AREA_STRING";

    public static final int JD = 0;
    public static final int CL = 1;
    public static final int LC = 2;

    private static final int[] BAND_TYPES = new int[]{JD, CL, LC};
    private Configuration configuration;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        configuration = context.getConfiguration();
        String lcPath = configuration.get(KEY_LC_PATH);
        String version = configuration.get(KEY_VERSION);
        String areaString = configuration.get(KEY_AREA_STRING); // <nicename>;<left>;<top>;<right>;<bottom>

        ProductSplit inputSplit = (ProductSplit) context.getInputSplit();

        Path inputSplitLocation = inputSplit.getPath();
        LOG.info("Finalising file '" + inputSplitLocation + "'");

        String year = configuration.get("calvalus.year");
        String month = configuration.get("calvalus.month");
        String template;
        switch (configuration.get("calvalus.sensor")) {
            case "OLCI":
                template = S2_TEMPLATE;
                break;
            case "MODIS":
                template = MODIS_TEMPLATE;
                break;
            case "S2":
                template = S2_TEMPLATE;
                break;
            default:
                throw new IllegalArgumentException("Unknown sensor '" + configuration.get("calvalus.sensor") + "'");
        }

        String baseFilename = createBaseFilename(year, month, version, areaString);

        String outputDir = configuration.get("calvalus.output.dir");
        Path tifPath_JD = new Path(outputDir + "/" + baseFilename + "-JD.tif");
        Path tifPath_CL = new Path(outputDir + "/" + baseFilename + "-CL.tif");
        Path tifPath_LC = new Path(outputDir + "/" + baseFilename + "-LC.tif");
        FileSystem fileSystem = FileSystem.get(configuration);

        Path[] outputPaths = new Path[]{tifPath_JD, tifPath_CL, tifPath_LC};

        File localL3 = CalvalusProductIO.copyFileToLocal(inputSplitLocation, configuration);
        File localLC = CalvalusProductIO.copyFileToLocal(new Path(lcPath), configuration);

        Product source = ProductIO.readProduct(localL3);

        Product lcProduct = ProductIO.readProduct(localLC);
        lcProduct = collocateWithSource(lcProduct, source);

        Product resultJD = remap(source, baseFilename, lcProduct, JD, areaString.split(";")[1]);
        Product resultCL = remap(source, baseFilename, lcProduct, CL, null);
        Product resultLC = remap(source, baseFilename, lcProduct, LC, null);

        Product[] results = new Product[]{resultJD, resultCL, resultLC};

        FileSystem fs = outputPaths[0].getFileSystem(configuration);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < results.length; i++) {
            int finalI = i;
            Runnable worker = () -> {

                try {
                    LocalTime time = LocalTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                    CalvalusLogger.getLogger().info(time.format(formatter) + ": Writing final product " + (finalI + 1) + "/" + BAND_TYPES.length + "...");
                    Path tifPath = outputPaths[finalI];
                    Path alternativeTifPath = new Path(outputPaths[finalI].toString().replace("-Formatting", "-Formatting_format"));

                    if (fileSystem.exists(tifPath) || fileSystem.exists(alternativeTifPath)) {
                        LOG.info("File '" + tifPath + "' already exists, skipping.");
                        return;
                    }

                    Product result = results[finalI];
                    final ProductWriter geotiffWriter = ProductIO.getProductWriter(BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
                    String localFilename = baseFilename + "-" + BAND_TYPES[finalI] + ".tif";
                    geotiffWriter.writeProductNodes(result, localFilename);

                    geotiffWriter.writeBandRasterData(result.getBandAt(0), 0, 0, 0, 0, null, ProgressMonitor.NULL);
                    CalvalusLogger.getLogger().info(String.format("...done with " + BAND_TYPES[finalI] + ". Copying final product to %s...", tifPath.getParent().toString()));
                    FileUtil.copy(new File(localFilename), fs, tifPath, false, configuration);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            };
            executor.execute(worker);
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
        }

        LOG.info("...done. Creating metadata...");
        Path xmlPath = new Path(outputDir + "/" + baseFilename + ".xml");
        if (!fileSystem.exists(xmlPath)) {
            String metadata = createMetadata(template, year, month, version, areaString);
            try (FileWriter fw = new FileWriter(baseFilename + ".xml")) {
                fw.write(metadata);
            }
            FileUtil.copy(new File(baseFilename + ".xml"), fs, xmlPath, false, configuration);
        }
        CalvalusLogger.getLogger().info("...done");
    }

    protected abstract Product collocateWithSource(Product lcProduct, Product source);

    protected Product remap(Product source, String baseFilename, Product lcProduct, int band, String area) {
        source.setPreferredTileSize(TILE_SIZE, TILE_SIZE);

        Product target = new Product(baseFilename, "fire-cci-pixel-product", source.getSceneRasterWidth(), source.getSceneRasterHeight());
        target.setPreferredTileSize(TILE_SIZE, TILE_SIZE);

        ProductUtils.copyGeoCoding(source, target);

        Band sourceJdBand = source.getBand("JD");
        Band sourceClBand = source.getBand("CL");
        Band sourceLcBand = getLcBand(lcProduct);

        String sensor = getCalvalusSensor();

        switch (band) {
            case JD:
                Band jdBand = target.addBand("JD", ProductData.TYPE_INT16);
                jdBand.setSourceImage(new JdImage(sourceJdBand, sourceLcBand, sensor, area, configuration));
                break;
            case CL:
                Band clBand = target.addBand("CL", ProductData.TYPE_INT8);
                clBand.setSourceImage(new ClImage(sourceClBand, sourceJdBand, sourceLcBand, getClScaler(), sensor));
                break;
            case LC:
                Band lcBand = target.addBand("LC", ProductData.TYPE_UINT8);
                lcBand.setSourceImage(new LcImage(sourceLcBand, sourceJdBand, sourceClBand, sensor));
                break;
            default:
                throw new IllegalArgumentException("Programming error: invalid value '" + band + "' for band.");
        }


        return target;
    }

    protected abstract String getCalvalusSensor();

    protected abstract Band getLcBand(Product lcProduct);

    protected abstract ClScaler getClScaler();

    public abstract String createBaseFilename(String year, String month, String version, String areaString);

    static final String MODIS_TEMPLATE = "" +
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<gmi:MI_Metadata" +
            "        xsi:schemaLocation=\"http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd http://www.isotc211.org/2005/gmi http://www.isotc211.org/2005/gmi/gmi.xsd\"" +
            "        xmlns:gmd=\"http://www.isotc211.org/2005/gmd\"" +
            "        xmlns:gco=\"http://www.isotc211.org/2005/gco\"" +
            "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" +
            "        xmlns:gml=\"http://www.opengis.net/gml\"" +
            "        xmlns:gmi=\"http://www.isotc211.org/2005/gmi\"" +
            ">" +
            "" +
            "    <gmd:fileIdentifier>" +
            "        <gco:CharacterString>urn:uuid:esa-cci:fire:pixel:${UUID}</gco:CharacterString>" +
            "    </gmd:fileIdentifier>" +
            "" +
            "    <gmd:language>" +
            "        <gmd:LanguageCode codeList=\"http://www.loc.gov/standards/iso639-2/\" codeListValue=\"eng\">eng</gmd:LanguageCode>" +
            "    </gmd:language>" +
            "" +
            "    <gmd:characterSet>" +
            "        <gmd:MD_CharacterSetCode codeSpace=\"ISOTC211/19115\" codeListValue=\"MD_CharacterSetCode_utf8\"" +
            "                                 codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_CharacterSetCode\">" +
            "            MD_CharacterSetCode_utf8</gmd:MD_CharacterSetCode>" +
            "    </gmd:characterSet>" +
            "" +
            "    <gmd:hierarchyLevel>" +
            "        <gmd:MD_ScopeCode codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_ScopeCode\"" +
            "                          codeListValue=\"dataset\">dataset" +
            "        </gmd:MD_ScopeCode>" +
            "    </gmd:hierarchyLevel>" +
            "" +
            "    <gmd:contact>" +
            "        <gmd:CI_ResponsibleParty>" +
            "            <gmd:organisationName>" +
            "                <gco:CharacterString>University of Alcala</gco:CharacterString>" +
            "            </gmd:organisationName>" +
            "            <gmd:contactInfo>" +
            "                <gmd:CI_Contact>" +
            "                    <gmd:address>" +
            "                        <gmd:CI_Address>" +
            "                            <gmd:electronicMailAddress>" +
            "                                <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                            </gmd:electronicMailAddress>" +
            "                        </gmd:CI_Address>" +
            "                    </gmd:address>" +
            "                    <gmd:onlineResource>" +
            "                        <gmd:CI_OnlineResource>" +
            "                            <gmd:linkage>" +
            "                                <gmd:URL>http://www.esa-fire-cci.org/</gmd:URL>" +
            "                            </gmd:linkage>" +
            "                        </gmd:CI_OnlineResource>" +
            "                    </gmd:onlineResource>" +
            "                </gmd:CI_Contact>" +
            "            </gmd:contactInfo>" +
            "            <gmd:role>" +
            "                <gmd:CI_RoleCode" +
            "                        codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                        codeListValue=\"resourceProvider\">resourceProvider" +
            "                </gmd:CI_RoleCode>" +
            "            </gmd:role>" +
            "        </gmd:CI_ResponsibleParty>" +
            "    </gmd:contact>" +
            "" +
            "    <gmd:dateStamp>" +
            "        <gco:Date>${date}</gco:Date>" +
            "    </gmd:dateStamp>" +
            "" +
            "    <gmd:metadataStandardName>" +
            "        <gco:CharacterString>ISO 19115 - Geographic information - Metadata" +
            "        </gco:CharacterString>" +
            "    </gmd:metadataStandardName>" +
            "" +
            "    <gmd:metadataStandardVersion>" +
            "        <gco:CharacterString>ISO 19115:2003(E)</gco:CharacterString>" +
            "    </gmd:metadataStandardVersion>" +
            "" +
            "    <gmd:referenceSystemInfo>" +
            "        <gmd:MD_ReferenceSystem>" +
            "            <gmd:referenceSystemIdentifier>" +
            "                <gmd:RS_Identifier>" +
            "                    <gmd:authority>" +
            "                        <CI_Citation>" +
            "                            <gco:CharacterString>WGS84</gco:CharacterString>" +
            "                        </CI_Citation>" +
            "                    </gmd:authority>" +
            "                    <gmd:code>" +
            "                        <gco:CharacterString>WGS84</gco:CharacterString>" +
            "                    </gmd:code>" +
            "                </gmd:RS_Identifier>" +
            "            </gmd:referenceSystemIdentifier>" +
            "        </gmd:MD_ReferenceSystem>" +
            "    </gmd:referenceSystemInfo>" +
            "" +
            "    <gmd:identificationInfo>" +
            "        <gmd:MD_DataIdentification>" +
            "" +
            "            <gmd:citation>" +
            "                <gmd:CI_Citation>" +
            "                    <gmd:title>" +
            "                        <gco:CharacterString>Fire_cci Pixel MODIS Burned Area product ${REPLACE_WITH_VERSION} – Zone ${zoneId}" +
            "                        </gco:CharacterString>" +
            "                    </gmd:title>" +
            "                    <gmd:date>" +
            "                        <!-- creation date-->" +
            "                        <gmd:CI_Date>" +
            "                            <gmd:date>" +
            "                                <gco:Date>${creationDate}</gco:Date>" +
            "                            </gmd:date>" +
            "                            <gmd:dateType>" +
            "                                <gmd:CI_DateTypeCode" +
            "                                        codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode\"" +
            "                                        codeListValue=\"creation\">creation" +
            "                                </gmd:CI_DateTypeCode>" +
            "                            </gmd:dateType>" +
            "                        </gmd:CI_Date>" +
            "                    </gmd:date>" +
            "                    <gmd:date>" +
            "                        <!-- publication date-->" +
            "                        <gmd:CI_Date>" +
            "                            <gmd:date>" +
            "                                <gco:Date>2019-06-15</gco:Date>" +
            "                            </gmd:date>" +
            "                            <gmd:dateType>" +
            "                                <gmd:CI_DateTypeCode" +
            "                                        codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode\"" +
            "                                        codeListValue=\"publication\">publication" +
            "                                </gmd:CI_DateTypeCode>" +
            "                            </gmd:dateType>" +
            "                        </gmd:CI_Date>" +
            "                    </gmd:date>" +
            "" +
            "                    <gmd:identifier>" +
            "                        <gmd:MD_Identifier>" +
            "                            <gmd:code>" +
            "                                <gco:CharacterString>doi:10.5285/58f00d8814064b79a0c49662ad3af537</gco:CharacterString>" +
            "                            </gmd:code>" +
            "                        </gmd:MD_Identifier>" +
            "                    </gmd:identifier>" +
            "                </gmd:CI_Citation>" +
            "            </gmd:citation>" +
            "" +
            "<gmd:abstract>" +
            "<gco:CharacterString>In support of the IPCC, the ESA Climate Change Initiative (CCI) programme comprises " +
            "the generation of different projects, each focusing on the production of global coverage of an " +
            "Essential Climate Variable (ECV). The ECV Fire Disturbance (Fire_cci) provides validated, " +
            "error-characterised, global data sets of burned areas (BA) derived from existing satellite " +
            "observations. The Fire_cci BA products consist of a Pixel " +
            "and Grid product addressing the needs and requirements of climate, atmospheric and ecosystem " +
            "scientists and researchers supporting their modelling efforts. Further information on the ESA CCI " +
            "Programme and a comprehensive documentation on the underlying algorithms, work flow, production " +
            "system and product validation is publicly accessible on https://www.esa-fire-cci.org/." +
            "</gco:CharacterString>" +
            "<gco:CharacterString>" +
            "#[[" +
            "The product is a set of single-layer GeoTIFF files with the following naming convention: " +
            "${Indicative Date}-ESACCI-L3S_FIRE-BA-${Indicative sensor}[-${Additional Segregator}]-fv${xx.x}[-${layer}].tif. " +
            "${Indicative Date} is the identifying date for this data set. Format is YYYYMMDD, where YYYY is the four " +
            "digit year, MM is the two digit month from 01 to 12 and DD is the two digit day of the month from 01 to 31. " +
            "For monthly products the date will be set to 01. " +
            "${Indicative sensor} is MODIS. ${Additional Segregator} is the AREA_${TILE_CODE} being the tile code " +
            "described in the Product User Guide. ${File Version} is the File version number in the form n{1,}[.n{1,}] " +
            "(That is 1 or more digits followed by optional . and another 1 or more digits.). ${layer} is the code for " +
            "the layer represented in each file, being: JD: layer 1, CL: layer 2, and LC: layer 3. " +
            "An example is: 20050301-ESACCI-L3S_FIRE-BA-MODIS-AREA_5-${REPLACE_WITH_VERSION}-JD.tif.]]#" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>For further information on the product, please consult the Product User Guide: Fire_cci_D3.3.3_PUG-MODIS_v1.4 available at: www.esa-fire-cci.org/documents" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 1: Date of the first detection; Pixel Spacing = 0.0022457331 deg  (approx. 250m); " +
            "Pixel value = Day of the year, from 1 to 365 (or 366). A value of 0 is included when the pixel is not burned " +
            "in the month; a value of -1 is allocated to pixels that are not observed in the month; a value of -2 is " +
            "allocated to pixels that are not burnable (urban areas, bare areas, water bodies and permanent snow and " +
            "ice). Data type = Integer; Number of layers = 1; Data depth = 16" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 2: Confidence Level; Pixel Spacing = 0.0022457331 deg  (approx. 250m); Pixel " +
            "value = 0 to 100, where the value is the probability in percentage that the pixel is actually burned, as a " +
            "result of the different steps of the burned area classification. The higher the value, the higher the " +
            "confidence that the pixel is actually burned. A value of 0 is allocated to pixels that are not observed in " +
            "the month, or not taken into account in the burned area processing (non-burnable). Data type = Byte; Number " +
            "of layers = 1; Data depth = 8" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 3: Land cover of that pixel, extracted from the CCI LandCover v2.0.7 (LC). N is the " +
            "number of the land cover category in the reference " +
            "map. It is only valid when layer 1 &gt; 0. Pixel value is 0 to N under the following codes: " +
            "10 = Cropland, rainfed; " +
            "20 = Cropland, irrigated or post-flooding; " +
            "30 = Mosaic cropland (&gt;50%) / natural vegetation (tree, shrub, herbaceous cover) (&lt;50%); " +
            "40 = Mosaic natural vegetation (tree, shrub, herbaceous cover) (&gt;50%) / cropland (&lt;50%); " +
            "50 = Tree cover, broadleaved, evergreen, closed to open (&gt;15%); " +
            "60 = Tree cover, broadleaved, deciduous, closed to open (&gt;15%); " +
            "70 = Tree cover, needleleaved, evergreen, closed to open (&gt;15%); " +
            "80 = Tree cover, needleleaved, deciduous, closed to open (&gt;15%); " +
            "90 = Tree cover, mixed leaf type (broadleaved and needleleaved); " +
            "100 = Mosaic tree and shrub (&gt;50%) / herbaceous cover (&lt;50%); " +
            "110 = Mosaic herbaceous cover (&gt;50%) / tree and shrub (&lt;50%); " +
            "120 = Shrubland; " +
            "130 = Grassland; " +
            "140 = Lichens and mosses; " +
            "150 = Sparse vegetation (tree, shrub, herbaceous cover) (&lt;15%); " +
            "160 = Tree cover, flooded, fresh or brackish water; " +
            "170 = Tree cover, flooded, saline water; " +
            "180 = Shrub or herbaceous cover, flooded, fresh/saline/brackish water; " +
            "Pixel Spacing = 0.0022457331 deg  (approx. 250m); Data type = Byte; Number of layers = 1; Data depth = 8" +
            "</gco:CharacterString>" +
            "</gmd:abstract>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- ResourceProvider-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>University of Alcala</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>https://www.esa-fire-cci.org/</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"resourceProvider \">resourceProvider" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- Distributor-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>ESA Fire_cci</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>https://www.esa-fire-cci.org/</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"distributor \">distributor" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- principalInvestigator-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>University of Alcala</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>https://www.esa-fire-cci.org/</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"principalInvestigator\">principalInvestigator" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- Processor-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>Brockmann Consult GmbH</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>thomas.storm@brockmann-consult.de</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.brockmann-consult.de</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"Processor\">Processor" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:descriptiveKeywords>" +
            "                <gmd:MD_Keywords>" +
            "                    <gmd:keyword>" +
            "                        <gco:CharacterString>Burned Area; Fire Disturbance; Climate Change; ESA; GCOS</gco:CharacterString>" +
            "                    </gmd:keyword>" +
            "                    <gmd:type>" +
            "                        <gmd:MD_KeywordTypeCode codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml\"" +
            "                                                codeListValue=\"theme\">theme" +
            "                        </gmd:MD_KeywordTypeCode>" +
            "                    </gmd:type>" +
            "                </gmd:MD_Keywords>" +
            "            </gmd:descriptiveKeywords>" +
            "" +
            "            <gmd:resourceConstraints>" +
            "                <gmd:MD_Constraints>" +
            "                    <gmd:useLimitation>" +
            "                        <gco:CharacterString>ESA CCI Data Policy: free and open access</gco:CharacterString>" +
            "                    </gmd:useLimitation>" +
            "                </gmd:MD_Constraints>" +
            "            </gmd:resourceConstraints>" +
            "            <gmd:resourceConstraints>" +
            "                <gmd:MD_LegalConstraints>" +
            "                    <gmd:accessConstraints>" +
            "                        <gmd:MD_RestrictionCode" +
            "                                codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_RestrictionCode\"" +
            "                                codeListValue=\"otherRestrictions\">otherRestrictions" +
            "                        </gmd:MD_RestrictionCode>" +
            "                    </gmd:accessConstraints>" +
            "                    <gmd:otherConstraints>" +
            "                        <gco:CharacterString>no limitations</gco:CharacterString>" +
            "                    </gmd:otherConstraints>" +
            "                </gmd:MD_LegalConstraints>" +
            "            </gmd:resourceConstraints>" +
            "" +
            "            <gmd:spatialResolution>" +
            "                <gmd:MD_Resolution>" +
            "                    <gmd:distance>" +
            "                        <gco:Distance uom=\"degrees\">0.0022457331</gco:Distance>" +
            "                    </gmd:distance>" +
            "                </gmd:MD_Resolution>" +
            "            </gmd:spatialResolution>" +
            "" +
            "            <gmd:language>" +
            "                <gmd:LanguageCode codeList=\"http://www.loc.gov/standards/iso639-2/\" codeListValue=\"eng\">eng" +
            "                </gmd:LanguageCode>" +
            "            </gmd:language>" +
            "" +
            "            <gmd:topicCategory>" +
            "                <gmd:MD_TopicCategoryCode>biota</gmd:MD_TopicCategoryCode>" +
            "            </gmd:topicCategory>" +
            "            <gmd:topicCategory>" +
            "                <gmd:MD_TopicCategoryCode>environment</gmd:MD_TopicCategoryCode>" +
            "            </gmd:topicCategory>" +
            "            <gmd:topicCategory>" +
            "                <gmd:MD_TopicCategoryCode>imageryBaseMapsEarthCover</gmd:MD_TopicCategoryCode>" +
            "            </gmd:topicCategory>" +
            "" +
            "            <gmd:extent>" +
            "                <gmd:EX_Extent>" +
            "                    <gmd:geographicElement>" +
            "                        <gmd:EX_GeographicBoundingBox>" +
            "                            <gmd:westBoundLongitude>" +
            "                                <gco:Decimal>${westLon}</gco:Decimal>" +
            "                            </gmd:westBoundLongitude>" +
            "                            <gmd:eastBoundLongitude>" +
            "                                <gco:Decimal>${eastLon}</gco:Decimal>" +
            "                            </gmd:eastBoundLongitude>" +
            "                            <gmd:southBoundLatitude>" +
            "                                <gco:Decimal>${southLat}</gco:Decimal>" +
            "                            </gmd:southBoundLatitude>" +
            "                            <gmd:northBoundLatitude>" +
            "                                <gco:Decimal>${northLat}</gco:Decimal>" +
            "                            </gmd:northBoundLatitude>" +
            "                        </gmd:EX_GeographicBoundingBox>" +
            "                    </gmd:geographicElement>" +
            "" +
            "                    <gmd:temporalElement>" +
            "                        <gmd:EX_TemporalExtent>" +
            "                            <gmd:extent>\n" +
            "                                <gml:TimePeriod>" +
            "                                    <gml:beginPosition>${begin}</gml:beginPosition>" +
            "                                    <gml:endPosition>${end}</gml:endPosition>" +
            "                                </gml:TimePeriod>" +
            "                            </gmd:extent>" +
            "                        </gmd:EX_TemporalExtent>" +
            "                    </gmd:temporalElement>" +
            "" +
            "                </gmd:EX_Extent>" +
            "            </gmd:extent>" +
            "" +
            "        </gmd:MD_DataIdentification>" +
            "    </gmd:identificationInfo>" +
            "" +
            "</gmi:MI_Metadata>";

    static PositionAndValue findNeighbourValue(float[] jdData, int[] lcArray, int pixelIndex, int width, boolean isJD, String sensor) {
        int[] xDirections = new int[]{-1, 0, 1};
        int[] yDirections = new int[]{-1, 0, 1};

        SortedMap<Integer, Integer[]> values = new TreeMap<>();

        for (int yDirection : yDirections) {
            for (int xDirection : xDirections) {
                if (pixelIndex % width == 0 && xDirection == -1
                        || (pixelIndex + 1) % width == 0 && xDirection == 1) {
                    continue;
                }

                int newPixelIndex = pixelIndex + yDirection * width + xDirection;
                if (newPixelIndex < jdData.length) {
                    if (newPixelIndex < 0 || newPixelIndex >= jdData.length) {
                        continue;
                    }
                    float neighbourValue = jdData[newPixelIndex];

                    boolean inBurnableLcClass = isInBurnableLcClass(lcArray[newPixelIndex], sensor);

                    if (!Float.isNaN(neighbourValue) && neighbourValue != 999 && inBurnableLcClass) {
                        PositionAndValue positionAndValue = new PositionAndValue(newPixelIndex, neighbourValue);
                        if (values.containsKey((int) positionAndValue.value)) {
                            Integer[] v = new Integer[]{values.get((int) positionAndValue.value)[0] + 1, positionAndValue.newPixelIndex};
                            values.put((int) positionAndValue.value, v);
                        } else {
                            Integer[] v = new Integer[]{1, positionAndValue.newPixelIndex};
                            values.put((int) positionAndValue.value, v);
                        }
                    }
                }
            }
        }

        PositionAndValue result = null;
        int maxCount = 0;
        for (int value : values.keySet()) {
            Integer count = values.get(value)[0];
            if (count > maxCount) {
                maxCount = count;
                result = new PositionAndValue(values.get(value)[1], value);
            }
        }

        if (result == null) {
            // all neighbours are NaN or not burnable

            if (isJD) {
                return new PositionAndValue(pixelIndex, -1);
            } else {
                return new PositionAndValue(pixelIndex, 0);
            }
        }
        return result;
    }

    private static boolean isInBurnableLcClass(int sourceLcClass, String sensor) {
        switch (sensor) {
            case "S2":
                return LcRemappingS2.isInBurnableLcClass(sourceLcClass);
            case "MODIS":
            case "OLCI":
                return LcRemapping.isInBurnableLcClass(sourceLcClass);
            default:
                throw new IllegalStateException("Unknown sensor '" + sensor + "'");
        }
    }

    static class PositionAndValue {

        PositionAndValue(int newPixelIndex, float value) {
            this.newPixelIndex = newPixelIndex;
            this.value = value;
        }

        int newPixelIndex;
        public float value;

    }

    public interface ClScaler {

        float scaleCl(float cl);

    }
    static final String S2_TEMPLATE = "" +
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<gmi:MI_Metadata" +
            "        xsi:schemaLocation=\"http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd http://www.isotc211.org/2005/gmi http://www.isotc211.org/2005/gmi/gmi.xsd\"" +
            "        xmlns:gmd=\"http://www.isotc211.org/2005/gmd\"" +
            "        xmlns:gco=\"http://www.isotc211.org/2005/gco\"" +
            "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" +
            "        xmlns:gml=\"http://www.opengis.net/gml\"" +
            "        xmlns:gmi=\"http://www.isotc211.org/2005/gmi\"" +
            ">" +
            "" +
            "    <gmd:fileIdentifier>" +
            "        <gco:CharacterString>urn:uuid:esa-cci:fire:pixel:${UUID}</gco:CharacterString>" +
            "    </gmd:fileIdentifier>" +
            "" +
            "    <gmd:language>" +
            "        <gmd:LanguageCode codeList=\"http://www.loc.gov/standards/iso639-2/\" codeListValue=\"eng\">eng</gmd:LanguageCode>" +
            "    </gmd:language>" +
            "" +
            "    <gmd:characterSet>" +
            "        <gmd:MD_CharacterSetCode codeSpace=\"ISOTC211/19115\" codeListValue=\"MD_CharacterSetCode_utf8\"" +
            "                                 codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_CharacterSetCode\">" +
            "            MD_CharacterSetCode_utf8</gmd:MD_CharacterSetCode>" +
            "    </gmd:characterSet>" +
            "" +
            "    <gmd:hierarchyLevel>" +
            "        <gmd:MD_ScopeCode codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_ScopeCode\"" +
            "                          codeListValue=\"dataset\">dataset" +
            "        </gmd:MD_ScopeCode>" +
            "    </gmd:hierarchyLevel>" +
            "" +
            "    <gmd:contact>" +
            "        <gmd:CI_ResponsibleParty>" +
            "            <gmd:organisationName>" +
            "                <gco:CharacterString>University of Alcala</gco:CharacterString>" +
            "            </gmd:organisationName>" +
            "            <gmd:contactInfo>" +
            "                <gmd:CI_Contact>" +
            "                    <gmd:address>" +
            "                        <gmd:CI_Address>" +
            "                            <gmd:electronicMailAddress>" +
            "                                <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                            </gmd:electronicMailAddress>" +
            "                        </gmd:CI_Address>" +
            "                    </gmd:address>" +
            "                    <gmd:onlineResource>" +
            "                        <gmd:CI_OnlineResource>" +
            "                            <gmd:linkage>" +
            "                                <gmd:URL>http://www.esa-fire-cci.org/</gmd:URL>" +
            "                            </gmd:linkage>" +
            "                        </gmd:CI_OnlineResource>" +
            "                    </gmd:onlineResource>" +
            "                </gmd:CI_Contact>" +
            "            </gmd:contactInfo>" +
            "            <gmd:role>" +
            "                <gmd:CI_RoleCode" +
            "                        codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                        codeListValue=\"resourceProvider\">resourceProvider" +
            "                </gmd:CI_RoleCode>" +
            "            </gmd:role>" +
            "        </gmd:CI_ResponsibleParty>" +
            "    </gmd:contact>" +
            "" +
            "    <gmd:dateStamp>" +
            "        <gco:Date>${date}</gco:Date>" +
            "    </gmd:dateStamp>" +
            "" +
            "    <gmd:metadataStandardName>" +
            "        <gco:CharacterString>ISO 19115 - Geographic information - Metadata" +
            "        </gco:CharacterString>" +
            "    </gmd:metadataStandardName>" +
            "" +
            "    <gmd:metadataStandardVersion>" +
            "        <gco:CharacterString>ISO 19115:2003(E)</gco:CharacterString>" +
            "    </gmd:metadataStandardVersion>" +
            "" +
            "    <gmd:referenceSystemInfo>" +
            "        <gmd:MD_ReferenceSystem>" +
            "            <gmd:referenceSystemIdentifier>" +
            "                <gmd:RS_Identifier>" +
            "                    <gmd:authority>" +
            "                        <CI_Citation>" +
            "                            <gco:CharacterString>WGS84</gco:CharacterString>" +
            "                        </CI_Citation>" +
            "                    </gmd:authority>" +
            "                    <gmd:code>" +
            "                        <gco:CharacterString>WGS84</gco:CharacterString>" +
            "                    </gmd:code>" +
            "                </gmd:RS_Identifier>" +
            "            </gmd:referenceSystemIdentifier>" +
            "        </gmd:MD_ReferenceSystem>" +
            "    </gmd:referenceSystemInfo>" +
            "" +
            "    <gmd:identificationInfo>" +
            "        <gmd:MD_DataIdentification>" +
            "" +
            "            <gmd:citation>" +
            "                <gmd:CI_Citation>" +
            "                    <gmd:title>" +
            "                        <gco:CharacterString>Fire_cci Pixel MSI Burned Area product fv1.1 – Area ${zoneId}" +
            "                        </gco:CharacterString>" +
            "                    </gmd:title>" +
            "                    <gmd:date>" +
            "                        <!-- creation date-->" +
            "                        <gmd:CI_Date>" +
            "                            <gmd:date>" +
            "                                <gco:Date>${creationDate}</gco:Date>" +
            "                            </gmd:date>" +
            "                            <gmd:dateType>" +
            "                                <gmd:CI_DateTypeCode" +
            "                                        codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode\"" +
            "                                        codeListValue=\"creation\">creation" +
            "                                </gmd:CI_DateTypeCode>" +
            "                            </gmd:dateType>" +
            "                        </gmd:CI_Date>" +
            "                    </gmd:date>" +
            "                    <gmd:date>" +
            "                        <!-- publication date-->" +
            "                        <gmd:CI_Date>" +
            "                            <gmd:date>" +
            "                                <gco:Date>2019-06-15</gco:Date>" +
            "                            </gmd:date>" +
            "                            <gmd:dateType>" +
            "                                <gmd:CI_DateTypeCode" +
            "                                        codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode\"" +
            "                                        codeListValue=\"publication\">publication" +
            "                                </gmd:CI_DateTypeCode>" +
            "                            </gmd:dateType>" +
            "                        </gmd:CI_Date>" +
            "                    </gmd:date>" +
            "" +
            "                    <gmd:identifier>" +
            "                        <gmd:MD_Identifier>" +
            "                            <gmd:code>" +
            "                                <gco:CharacterString>doi: 10.5285/065f6040ef08485db989cbd89d536167</gco:CharacterString>" +
            "                            </gmd:code>" +
            "                        </gmd:MD_Identifier>" +
            "                    </gmd:identifier>" +
            "                </gmd:CI_Citation>" +
            "            </gmd:citation>" +
            "" +
            "<gmd:abstract>" +
            "<gco:CharacterString>In support of the IPCC, the ESA Climate Change Initiative (CCI) programme comprises " +
            "the generation of different projects, each focusing on the production of global coverage of an " +
            "Essential Climate Variable (ECV). The ECV Fire Disturbance (Fire_cci) provides validated, " +
            "error-characterised, global data sets of burned areas (BA) derived from existing satellite " +
            "observations. The Fire_cci BA products consist of a Pixel " +
            "and Grid product addressing the needs and requirements of climate, atmospheric and ecosystem " +
            "scientists and researchers supporting their modelling efforts. Further information on the ESA CCI " +
            "Programme and a comprehensive documentation on the underlying algorithms, work flow, production " +
            "system and product validation is publicly accessible on https://www.esa-fire-cci.org/." +
            "</gco:CharacterString>" +
            "<gco:CharacterString>" +
            "#[[" +
            "The product is a set of single-layer GeoTIFF files with the following naming convention: " +
            "${Indicative Date}-ESACCI-L3S_FIRE-BA-${Indicative sensor}[-${Additional Segregator}]-${xx.x}[-${layer}].tif. " +
            "${Indicative Date} is the identifying date for this data set. Format is YYYYMMDD, where YYYY is the four " +
            "digit year, MM is the two digit month from 01 to 12 and DD is the two digit day of the month from 01 to 31. " +
            "For monthly products the date is set to 01. " +
            "${Indicative sensor} is MSI. ${Additional Segregator} is the AREA_${TILE_CODE} being the tile code " +
            "described in the Product User Guide. ${File Version} is the File version number in the form n{1,}[.n{1,}] " +
            "(That is 1 or more digits followed by optional . and another 1 or more digits.). ${layer} is the code for " +
            "the layer represented in each file, being: JD: layer 1, CL: layer 2, and LC: layer 3. " +
            "An example is: 20050301-ESACCI-L3S_FIRE-BA-MSI-AREA_h44v16-fv1.1-JD.tif.]]#" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>For further information on the product, please consult the Product User Guide: Fire_cci_D3.3.2_PUG-MSI_v1.1" +
            " available at: www.esa-fire-cci.org/documents" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 1: Date of the first detection; Pixel Spacing = 0.00017966259deg (approx. 20m); " +
            "Pixel value = Day of the year, from 1 to 365 (or 366). A value of 0 is included when the pixel is not burned " +
            "in the month; a value of -1 is allocated to pixels that are not observed in the month; a value of -2 is " +
            "allocated to pixels that are not burnable (build up areas, bare areas, , snow and/or ice, open water). " +
            "Data type = Integer; Number of layers = 1; Data depth = 16" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 2: Confidence Level; Pixel Spacing = 0.00017966259deg (approx. 20m); Pixel " +
            "value = 0 to 100, where the value is the probability in percentage that the pixel is actually burned, as a " +
            "result of the different steps of the burned area classification. The higher the value, the higher the " +
            "confidence that the pixel is actually burned. A value of 0 is allocated to pixels that are not observed in " +
            "the month, or not taken into account in the burned area processing (non burnable). Data type = Byte; Number " +
            "of layers = 1; Data depth = 8" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 3: Land cover of that pixel, extracted from the CCI S2 prototype Land Cover map " +
            "at 20m of Africa 2016. N is the number of the land cover categories in the reference map. It is only valid " +
            "when layer 1 &gt; 0. Pixel value is 0 to N under the following codes: " +
            "1 – Trees cover area; 2 – Shrubs cover area; 3 – Grassland; 4 – Cropland; 5 – Vegetation aquatic or regularly flooded; 6 – Lichen Mosses / Sparse vegetation; " +
            "Pixel Spacing = 0.00017966259deg  (approx. 20m); Data type = Byte; Number of layers = 1; Data depth = 8" +
            "</gco:CharacterString>" +
            "</gmd:abstract>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- ResourceProvider-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>University of Alcala</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.esa-fire-cci.org/</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"resourceProvider \">resourceProvider" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- Distributor-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>ESA Fire_cci</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.esa-fire-cci.org/</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"distributor \">distributor" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- principalInvestigator-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>University of Alcala</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.esa-fire-cci.org/</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"principalInvestigator\">principalInvestigator" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- Processor-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>Brockmann Consult GmbH</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>thomas.storm@brockmann-consult.de</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.brockmann-consult.de</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"Processor\">Processor" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:descriptiveKeywords>" +
            "                <gmd:MD_Keywords>" +
            "                    <gmd:keyword>" +
            "                        <gco:CharacterString>Burned Area; Fire Disturbance; Climate Change; ESA; GCOS</gco:CharacterString>" +
            "                    </gmd:keyword>" +
            "                    <gmd:type>" +
            "                        <gmd:MD_KeywordTypeCode codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml\"" +
            "                                                codeListValue=\"theme\">theme" +
            "                        </gmd:MD_KeywordTypeCode>" +
            "                    </gmd:type>" +
            "                </gmd:MD_Keywords>" +
            "            </gmd:descriptiveKeywords>" +
            "" +
            "            <gmd:resourceConstraints>" +
            "                <gmd:MD_Constraints>" +
            "                    <gmd:useLimitation>" +
            "                        <gco:CharacterString>ESA CCI Data Policy: free and open access</gco:CharacterString>" +
            "                    </gmd:useLimitation>" +
            "                </gmd:MD_Constraints>" +
            "            </gmd:resourceConstraints>" +
            "            <gmd:resourceConstraints>" +
            "                <gmd:MD_LegalConstraints>" +
            "                    <gmd:accessConstraints>" +
            "                        <gmd:MD_RestrictionCode" +
            "                                codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_RestrictionCode\"" +
            "                                codeListValue=\"otherRestrictions\">otherRestrictions" +
            "                        </gmd:MD_RestrictionCode>" +
            "                    </gmd:accessConstraints>" +
            "                    <gmd:otherConstraints>" +
            "                        <gco:CharacterString>no limitations</gco:CharacterString>" +
            "                    </gmd:otherConstraints>" +
            "                </gmd:MD_LegalConstraints>" +
            "            </gmd:resourceConstraints>" +
            "" +
            "            <gmd:spatialResolution>" +
            "                <gmd:MD_Resolution>" +
            "                    <gmd:distance>" +
            "                        <gco:Distance uom=\"degrees\">0.00017966259</gco:Distance>" +
            "                    </gmd:distance>" +
            "                </gmd:MD_Resolution>" +
            "            </gmd:spatialResolution>" +
            "" +
            "            <gmd:language>" +
            "                <gmd:LanguageCode codeList=\"http://www.loc.gov/standards/iso639-2/\" codeListValue=\"eng\">eng" +
            "                </gmd:LanguageCode>" +
            "            </gmd:language>" +
            "" +
            "            <gmd:topicCategory>" +
            "                <gmd:MD_TopicCategoryCode>biota</gmd:MD_TopicCategoryCode>" +
            "            </gmd:topicCategory>" +
            "            <gmd:topicCategory>" +
            "                <gmd:MD_TopicCategoryCode>environment</gmd:MD_TopicCategoryCode>" +
            "            </gmd:topicCategory>" +
            "            <gmd:topicCategory>" +
            "                <gmd:MD_TopicCategoryCode>imageryBaseMapsEarthCover</gmd:MD_TopicCategoryCode>" +
            "            </gmd:topicCategory>" +
            "" +
            "            <gmd:extent>" +
            "                <gmd:EX_Extent>" +
            "                    <gmd:geographicElement>" +
            "                        <gmd:EX_GeographicBoundingBox>" +
            "                            <gmd:westBoundLongitude>" +
            "                                <gco:Decimal>${westLon}</gco:Decimal>" +
            "                            </gmd:westBoundLongitude>" +
            "                            <gmd:eastBoundLongitude>" +
            "                                <gco:Decimal>${eastLon}</gco:Decimal>" +
            "                            </gmd:eastBoundLongitude>" +
            "                            <gmd:southBoundLatitude>" +
            "                                <gco:Decimal>${southLat}</gco:Decimal>" +
            "                            </gmd:southBoundLatitude>" +
            "                            <gmd:northBoundLatitude>" +
            "                                <gco:Decimal>${northLat}</gco:Decimal>" +
            "                            </gmd:northBoundLatitude>" +
            "                        </gmd:EX_GeographicBoundingBox>" +
            "                    </gmd:geographicElement>" +
            "" +
            "                    <gmd:temporalElement>" +
            "                        <gmd:EX_TemporalExtent>" +
            "                            <gmd:extent>\n" +
            "                                <gml:TimePeriod>" +
            "                                    <gml:beginPosition>${begin}</gml:beginPosition>" +
            "                                    <gml:endPosition>${end}</gml:endPosition>" +
            "                                </gml:TimePeriod>" +
            "                            </gmd:extent>" +
            "                        </gmd:EX_TemporalExtent>" +
            "                    </gmd:temporalElement>" +
            "" +
            "                </gmd:EX_Extent>" +
            "            </gmd:extent>" +
            "" +
            "        </gmd:MD_DataIdentification>" +
            "    </gmd:identificationInfo>" +
            "" +
            "</gmi:MI_Metadata>";

    static String createMetadata(String template, String year, String month, String version, String areaString) throws IOException {
        String area = areaString.split(";")[0];
        String nicename = areaString.split(";")[1];
        String left = areaString.split(";")[2];
        String top = areaString.split(";")[3];
        String right = areaString.split(";")[4];
        String bottom = areaString.split(";")[5];

        VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.init();
        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("UUID", UUID.randomUUID().toString());
        velocityContext.put("date", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(Instant.now()));
        velocityContext.put("zoneId", area);
        velocityContext.put("zoneName", nicename);
        velocityContext.put("creationDate", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(LocalDate.now()));
        velocityContext.put("westLon", Integer.parseInt(left) - 180);
        velocityContext.put("eastLon", Integer.parseInt(right) - 180);
        velocityContext.put("northLat", 90 - Integer.parseInt(top));
        velocityContext.put("southLat", 90 - Integer.parseInt(bottom));
        velocityContext.put("begin", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.systemDefault()).format(Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atDay(1).atTime(0, 0, 0)));
        velocityContext.put("end", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.systemDefault()).format(Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atDay(Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth()).atTime(23, 59, 59)));

        StringWriter stringWriter = new StringWriter();
        velocityEngine.evaluate(velocityContext, stringWriter, "pixelFormatting", template.replace("${REPLACE_WITH_VERSION}", version));
        String intermediateResult = stringWriter.toString();

        try (InputStream stream = new ByteArrayInputStream(intermediateResult.getBytes("UTF-8"))) {
            SAXBuilder builder = new SAXBuilder();
            Document anotherDocument = builder.build(stream);
            StringWriter out = new StringWriter();
            new XMLOutputter(Format.getPrettyFormat()).output(anotherDocument, out);
            return out.toString();
        } catch (JDOMException | NullPointerException e) {
            throw new IOException(e);
        }
    }

}
