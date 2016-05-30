/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * The fire formatting pixel mapper.
 *
 * @author thomas
 * @author marcop
 */
public class PixelMergeMapper extends Mapper<Text, FileSplit, Text, PixelCell> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "256");
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "256");
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        PixelProductArea area = PixelProductArea.valueOf(context.getConfiguration().get("area"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        LOG.info("paths=" + Arrays.toString(paths));

        File[] variableFiles = new File[3];

        Archiver archiver = ArchiverFactory.createArchiver("tar", "gz");
        for (int i = 0; i < paths.length; i++) {
            variableFiles[i] = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            CalvalusLogger.getLogger().info("Extracting...");
            archiver.extract(variableFiles[i], new File("."));
            CalvalusLogger.getLogger().info("...done.");
        }

        CalvalusLogger.getLogger().info("Reading products...");
        Product inputVar1 = ProductIO.readProduct(variableFiles[0].getName().substring(0, variableFiles[0].getName().indexOf(".")) + ".dim");
        Product inputVar2 = ProductIO.readProduct(variableFiles[1].getName().substring(0, variableFiles[0].getName().indexOf(".")) + ".dim");
        Product inputVar3 = ProductIO.readProduct(variableFiles[2].getName().substring(0, variableFiles[0].getName().indexOf(".")) + ".dim");
        CalvalusLogger.getLogger().info("done.");

        Band JD = findBand(PixelVariableType.DAY_OF_YEAR.bandName, inputVar1, inputVar2, inputVar3);
        Band CL = findBand(PixelVariableType.CONFIDENCE_LEVEL.bandName, inputVar1, inputVar2, inputVar3);
        Band LC = findBand(PixelVariableType.LC_CLASS.bandName, inputVar1, inputVar2, inputVar3);

        context.progress();

        String baseFilename = createBaseFilename(year, month, area);
        Product result = new Product(baseFilename, "fire-cci-pixel-product", inputVar1.getSceneRasterWidth(), inputVar1.getSceneRasterHeight());
        ProductUtils.copyGeoCoding(inputVar1, result);
        ProductUtils.copyBand(JD.getName(), JD.getProduct(), result, true);
        ProductUtils.copyBand(CL.getName(), CL.getProduct(), result, true);
        ProductUtils.copyBand(LC.getName(), LC.getProduct(), result, true);

        CalvalusLogger.getLogger().info("Creating metadata...");
        String metadata = createMetadata(year, month, area);
        try (FileWriter fw = new FileWriter(baseFilename + ".xml")) {
            fw.write(metadata);
        }
        CalvalusLogger.getLogger().info("...done. Writing final product...");

        final ProductWriter geotiffWriter = ProductIO.getProductWriter(BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
        geotiffWriter.writeProductNodes(result, baseFilename + ".tif");
        geotiffWriter.writeBandRasterData(result.getBandAt(0), 0, 0, 0, 0, null, null);

        CalvalusLogger.getLogger().info("...done. Creating zip of final product...");
        String zipFilename = baseFilename + ".tar.gz";
        createTarGZ(baseFilename + ".tif", baseFilename + ".xml", zipFilename);

        result.dispose();
        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        Path path = new Path(outputDir + "/" + zipFilename);
        CalvalusLogger.getLogger().info(String.format("...done. Copying final product to %s...", path.toString()));
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileUtil.copy(new File(zipFilename), fs, path, false, context.getConfiguration());
        CalvalusLogger.getLogger().info("...done.");
    }

    private Band findBand(String bandName, Product... products) {
        for (Product product : products) {
            Band band = product.getBand(bandName);
            if (band != null) {
                return band;
            }
        }
        throw new IllegalArgumentException(String.format("Cannot find band %s in any of the given products", bandName));
    }

    static String createMetadata(String year, String month, PixelProductArea area) throws IOException {

        VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.init();
        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("UUID", UUID.randomUUID().toString());
        velocityContext.put("date", String.format("%s-%s-01", year, month));
        velocityContext.put("zoneId", area.index);
        velocityContext.put("zoneName", area.nicename);
        velocityContext.put("creationDate", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(Instant.now()));
        velocityContext.put("westLon", area.left - 180);
        velocityContext.put("eastLon", area.right - 180);
        velocityContext.put("southLat", area.bottom - 90);
        velocityContext.put("northLat", area.top - 90);

        StringWriter stringWriter = new StringWriter();
        velocityEngine.evaluate(velocityContext, stringWriter, "pixelFormatting", TEMPLATE);
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

    static String createBaseFilename(String year, String month, PixelProductArea area) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MERIS-AREA_%d-v02.0-fv04.0", year, month, area.index);
    }

    private static void createTarGZ(String filePath, String xmlPath, String outputPath) throws IOException {
        try (OutputStream fOut = new FileOutputStream(new File(outputPath));
             OutputStream bOut = new BufferedOutputStream(fOut);
             OutputStream gzOut = new GzipCompressorOutputStream(bOut);
             TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {
            tOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
            addFileToTarGz(tOut, filePath, "");
            addFileToTarGz(tOut, xmlPath, "");
        }
    }

    private static void addFileToTarGz(TarArchiveOutputStream tOut, String path, String base)
            throws IOException {
        File f = new File(path);
        String entryName = base + f.getName();
        TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);
        tOut.putArchiveEntry(tarEntry);

        if (f.isFile()) {
            IOUtils.copy(new FileInputStream(f), tOut);
            tOut.closeArchiveEntry();
        } else {
            tOut.closeArchiveEntry();
            File[] children = f.listFiles();
            if (children != null) {
                for (File child : children) {
                    addFileToTarGz(tOut, child.getAbsolutePath(), entryName + "/");
                }
            }
        }
    }

    private static final String TEMPLATE = "" +
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<gmi:MI_Metadata" +
            "        xsi:schemaLocation=\"http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd http://www.isotc211.org/2005/gmi http://www.isotc211.org/2005/gmi/gmi.xsd\"" +
            "        xmlns:gmd=\"http://www.isotc211.org/2005/gmd\"" +
            "        xmlns:gco=\"http://www.isotc211.org/2005/gco\"" +
            "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" +
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
            "                <gco:CharacterString>University of Alcala, Spain</gco:CharacterString>" +
            "            </gmd:organisationName>" +
            "            <gmd:contactInfo>" +
            "                <gmd:CI_Contact>" +
            "                    <gmd:address>" +
            "                        <gmd:CI_Address>" +
            "                            <gmd:electronicMailAddress>" +
            "                                <gco:CharacterString>emilio.chuvieco@uah.es></gco:CharacterString>" +
            "                            </gmd:electronicMailAddress>" +
            "                        </gmd:CI_Address>" +
            "                    </gmd:address>" +
            "                    <gmd:onlineResource>" +
            "                        <gmd:CI_OnlineResource>" +
            "                            <gmd:linkage>" +
            "                                <gmd:URL>http://www.geogra.uah.es/emilio/</gmd:URL>" +
            "                            </gmd:linkage>" +
            "                        </gmd:CI_OnlineResource>" +
            "                    </gmd:onlineResource>" +
            "                </gmd:CI_Contact>" +
            "            </gmd:contactInfo>" +
            "            <gmd:role>" +
            "                <gmd:CI_RoleCode" +
            "                        codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"" +
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
            "        <gco:CharacterString>ISO 19115-1 Geographic Information - Metadata Part 2 Extensions for imagery and gridded data" +
            "        </gco:CharacterString>" +
            "    </gmd:metadataStandardName>" +
            "" +
            "    <gmd:metadataStandardVersion>" +
            "        <gco:CharacterString>ISO 19115-1:2012</gco:CharacterString>" +
            "    </gmd:metadataStandardVersion>" +
            "" +
            "    <gmd:referenceSystemInfo>" +
            "        <gmd:MD_ReferenceSystem>" +
            "            <gmd:referenceSystemIdentifier>" +
            "                <gmd:RS_Identifier>" +
            "                    <gmd:authority>" +
            "                        <CI_citation>" +
            "                            <gco:CharacterString>WGS84</gco:CharacterString>" +
            "                        </CI_citation>" +
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
            "                        <gco:CharacterString>Fire Disturbance ECV (Fire_cci) Pixel Product – Zone ${zoneId} – ${zoneName}" +
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
            "                                <gco:Date>2016-06-08</gco:Date>" +
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
            "                        <gmd:RS_Identifier>" +
            "                            <gmd:code>" +
            "                                <gco:CharacterString>doi:***********</gco:CharacterString>" +
            "                            </gmd:code>" +
            "                        </gmd:RS_Identifier>" +
            "                    </gmd:identifier>" +
            "                </gmd:CI_Citation>" +
            "            </gmd:citation>" +
            "" +
            "<gmd:abstract>" +
            "<gco:CharacterString>In support of the IPCC, the ESA Climate Change Initiative (CCI) programme comprises " +
            "the generation of thirteen projects, each focusing on the production of global coverage of an " +
            "Essential Climate Variable (ECV). The ECV Fire Disturbance (Fire_cci) provides validated, " +
            "error-characterised, global data sets of burnt areas (BA) derived from existing satellite " +
            "observations (SPOT-VGT, ENVISAT (A)ATSR, ENVISAT MERIS). The Fire_cci BA products consist of a Pixel " +
            "and Grid product addressing the needs and requirements of climate, atmospheric and ecosystem " +
            "scientists and researchers supporting their modelling efforts. Further information to the ESA CCI " +
            "Programme and a comprehensive documentation on the underlying algorithms, work flow, production " +
            "system and product validation is publicly accessible on https://www.esa-fire-cci.org/." +
            "</gco:CharacterString>" +
            "<gco:CharacterString>" +
            "#[[" +
            "The product is a multi-layer TIFF with the following naming convention: ${IndicativeDate}-ESACCI-L3S_FIRE-BA-" +
            "${Indicative sensor}[-${Additional Segregator}]-[-v${GDS version}]-fv${xx.x}.tif. " +
            "${Indicative Date} is the identifying date for this data set. Format is YYYY[MM[DD]], where YYYY is the " +
            "four digit year, MM is the two digit month from 01 to 12 and DD is the two digit day of the month from 01 to " +
            "31. For monthly products the date will be set to 01. " +
            "${Indicative sensor} is MERIS, when data coming from MERIS sensor; MODISVNIR when outputs come from MODIS 250m channels; " +
            "MSI, when it comes from Sentinel-2 MSI; OLCI, Sentinel-3; SLSTR, Sentinel-3; PROBA, PROBA-V. ${Additional Segregator} " +
            "is the AREA_${TILE_NUMBER} being the tile number the subset index described in Extent. v${GDS version} " +
            "is the version number of the GHRSST Data Specification is optional for the CCI file naming convention. fv" +
            "${File Version} is the File version number in the form n{1,}[.n{1,}] (That is 1 or more digits followed by optional " +
            ". and another 1 or more digits.). An example is: " +
            "20050301-ESACCI-L3S_FIRE-BA-MERIS-AREA_1-fv04.0.tif.]]#" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Product Specification at: Chuvieco, E. (2013). ESA CCI ECV Fire Disturbance - " +
            "Product Specification Document, Fire_cci_Ph2_UAH_D1_2_PSD_v3_0.pdf" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Algorithm Theoretical Basis Document – Pre-Processing: " +
            "https://www.esa-fire-cci.org/" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Algorithm Theoretical Basis Document – BA detection:" +
            "https://www.esa-fire-cci.org/" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Algorithm Theoretical Basis Document – BA merging: https://www.esa-fire-cci.org/" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Product Validation and Algorithm Selection Report: https://www.esa-fire-cci.org/" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer JD: Date of the first detection; Pixel Spacing = 300m; TIFF; A zero (0) will " +
            "be included in this field when the pixel is not burned in the month or it is not observed; Pixel " +
            "value = Day of the year, from 1 to 365; Data type = REAL; Number of layers = 1; Data depth = 16" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer CL: Uncertainty Confidence Layer; Pixel Spacing = 300m; Pixel value = 0 to" +
            "100, where the value is the probability in percentage that the pixel is actually burned, as a result" +
            "of both the pre-processing and the actual burned area classification. The higher the value, the" +
            "higher the confidence that the pixel is actually burned. Data type = REAL; Number of layers = 1;" +
            "Data depth = 16" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer LC: Land cover of that pixel, extracted from the CCI LandCover (LC). The 2000 " +
            "LC data is used for 2002 data, the 2005 LC data is used for data from 2003 to 2007, and the 2010 LC " +
            "data is used for data from 2008 to 2012. N is the number of land cover categories in the reference " +
            "map. It is only valid when layer 1 &gt; 0. Pixel value is 0 to N where under the following codes: 11 = " +
            "Post-flooding or irrigated croplands (or aquatic), 14 = Rainfed croplands, 20 = Mosaic cropland " +
            "(50-70%) / vegetation (grassland/shrubland/forest) (20-50%), 30 = Mosaic vegetation " +
            "(grassland/shrubland/forest) (50-70%) / cropland (20-50%), 40 = Closed to open (&gt;15%) broadleaved " +
            "evergreen or semi-deciduous forest (&gt;5m), 50 = Closed (&gt;40%) broadleaved deciduous forest (&gt;5m), 60 " +
            "= Open (15-40%) broadleaved deciduous forest/woodland (&gt;5m), 70 = Closed (&gt;40%) needleleaved " +
            "evergreen forest (&gt;5m), 90 = Open (15-40%) needleleaved deciduous or evergreen forest (&gt;5m), 100 = " +
            "Closed to open (&gt;15%) mixed broadleaved and needleleaved forest (&gt;5m), 110 = Mosaic forest or " +
            "shrubland (50-70%) / grassland (20-50%), 120 = Mosaic grassland (50-70%) / forest or shrubland " +
            "(20-50%) , 130 = Closed to open (&gt;15%) (broadleaved or needleleaved, evergreen or deciduous) " +
            "shrubland (&lt;5m), 140 = Closed to open (&gt;15%) herbaceous vegetation (grassland, savannas or " +
            "lichens/mosses), 150 = Sparse (&lt;15%) vegetation, 160 = Closed to open (&gt;15%) broadleaved forest " +
            "regularly flooded (semi-permanently or temporarily) - Fresh or brackish water, 170 = Closed (&gt;40%) " +
            "broadleaved forest or shrubland permanently flooded - Saline or brackish water, 180 = Closed to open " +
            "(&gt;15%) grassland or woody vegetation on regularly flooded or waterlogged soil - Fresh, brackish or " +
            "saline water, 190 = Others (Artificial surfaces and associated areas (Urban areas &gt;50%), 200 = Bare " +
            "areas, 210 = Water bodies, 220 = Permanent snow and ice, 230 = No data (burnt areas, clouds etc.,); " +
            "Pixel Spacing = 300m; Data type = REAL; Number of layers = 1; Data depth = 16" +
            "</gco:CharacterString>" +
            "</gmd:abstract>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- resourceProvider-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>European Space Agency</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>Stephen.plummer@esa.int></gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.esa.int</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"resourceProvider \">resourceProvider" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- Custodian-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>University of Alcala</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es></gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.geogra.uah.es/emilio/</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"Custodian \">Custodian" +
            "                        </gmd:CI_RoleCode>" +
            "                    </gmd:role>" +
            "                </gmd:CI_ResponsibleParty>" +
            "            </gmd:pointOfContact>" +
            "" +
            "            <gmd:pointOfContact>" +
            "                <gmd:CI_ResponsibleParty>" +
            "                    <!-- Owner-->" +
            "                    <gmd:organisationName>" +
            "                        <gco:CharacterString>European Space Agency</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>Stephen.plummer@esa.int></gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://www.esa.int</gmd:URL>" +
            "                                    </gmd:linkage>" +
            "                                </gmd:CI_OnlineResource>" +
            "                            </gmd:onlineResource>" +
            "                        </gmd:CI_Contact>" +
            "                    </gmd:contactInfo>" +
            "                    <gmd:role>" +
            "                        <gmd:CI_RoleCode" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"" +
            "                                codeListValue=\"Owner \">Owner" +
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
            "                                        <gmd:URL>http://www.geogra.uah.es/emilio/</gmd:URL>" +
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
            "            <gmd:descriptiveKeywords>" +
            "                <gmd:MD_Keywords>" +
            "                    <gmd:keyword>" +
            "                        <gco:CharacterString>Burned Area:Fire:Climate:Change:ESA:GCOS</gco:CharacterString>" +
            "                    </gmd:keyword>" +
            "                    <gmd:type>" +
            "                        <gmd:MD_KeywordTypeCode codeList=\"./resources/codeList.xml#MD_KeywordTypeCode\"" +
            "                                                codeListValue=\"theme\">theme" +
            "                        </gmd:MD_KeywordTypeCode>" +
            "                    </gmd:type>" +
            "                </gmd:MD_Keywords>" +
            "            </gmd:descriptiveKeywords>" +
            "" +
            "            <gmd:resourceConstraints>" +
            "                <gmd:MD_Constraints>" +
            "                    <gmd:useLimitation>" +
            "                        <gco:CharacterString>esaNoMonetaryCharge</gco:CharacterString>" +
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
            "                        <gco:CharacterString>no limitation</gco:CharacterString>" +
            "                    </gmd:otherConstraints>" +
            "                </gmd:MD_LegalConstraints>" +
            "            </gmd:resourceConstraints>" +
            "" +
            "            <gmd:spatialResolution>" +
            "                <gmd:MD_Resolution>" +
            "                    <gmd:distance>" +
            "                        <gco:Distance uom=\"metres\">300</gco:Distance>" +
            "                    </gmd:distance>" +
            "                </gmd:MD_Resolution>" +
            "            </gmd:spatialResolution>" +
            "" +
            "            <gmd:language>" +
            "                <gmd:LanguageCode codeList=\"http://www.loc.gov/standards/iso639-2/\" codeListValue=\"eng\">eng" +
            "                </gmd:LanguageCode>" +
            "            </gmd:language>" +
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
            "                </gmd:EX_Extent>" +
            "            </gmd:extent>" +
            "" +
            "        </gmd:MD_DataIdentification>" +
            "    </gmd:identificationInfo>" +
            "" +
            "</gmi:MI_Metadata>";

}
