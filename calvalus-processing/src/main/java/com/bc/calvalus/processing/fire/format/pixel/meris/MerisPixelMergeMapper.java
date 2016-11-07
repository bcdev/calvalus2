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

package com.bc.calvalus.processing.fire.format.pixel.meris;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.MerisStrategy;
import com.bc.calvalus.processing.fire.format.PixelProductArea;
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.time.Instant;
import java.time.Year;
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
public class MerisPixelMergeMapper extends Mapper<Text, FileSplit, Text, MerisPixelCell> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "256");
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "256");
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        String version = context.getConfiguration().get("calvalus.baversion", "v04.1");

        PixelProductArea area = new MerisStrategy().getArea(context.getConfiguration().get("area"));

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

        String baseFilename = createBaseFilename(year, month, version, area);
        Product result = new Product(baseFilename, "fire-cci-pixel-product", inputVar1.getSceneRasterWidth(), inputVar1.getSceneRasterHeight());
        ProductUtils.copyGeoCoding(inputVar1, result);
        ProductUtils.copyBand(JD.getName(), JD.getProduct(), result, true);
        ProductUtils.copyBand(CL.getName(), CL.getProduct(), result, true);
        ProductUtils.copyBand(LC.getName(), LC.getProduct(), result, true);

        CalvalusLogger.getLogger().info("Creating metadata...");
        String metadata = createMetadata(year, month, version, area);
        try (FileWriter fw = new FileWriter(baseFilename + ".xml")) {
            fw.write(metadata);
        }
        CalvalusLogger.getLogger().info("...done. Writing final product...");

        final ProductWriter geotiffWriter = ProductIO.getProductWriter(BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
        geotiffWriter.writeProductNodes(result, baseFilename + ".tif");
        geotiffWriter.writeBandRasterData(result.getBandAt(0), 0, 0, 0, 0, null, null);

        result.dispose();
        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        Path tifPath = new Path(outputDir + "/" + baseFilename + ".tif");
        Path xmlPath = new Path(outputDir + "/" + baseFilename + ".xml");
        CalvalusLogger.getLogger().info(String.format("...done. Copying final product to %s...", tifPath.getParent().toString()));
        FileSystem fs = tifPath.getFileSystem(context.getConfiguration());
        FileUtil.copy(new File(baseFilename + ".tif"), fs, tifPath, false, context.getConfiguration());
        FileUtil.copy(new File(baseFilename + ".xml"), fs, xmlPath, false, context.getConfiguration());
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

    static String createMetadata(String year, String month, String version, PixelProductArea area) throws IOException {

        VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.init();
        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("UUID", UUID.randomUUID().toString());
        velocityContext.put("date", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(Instant.now()));
        velocityContext.put("zoneId", area.index);
        velocityContext.put("zoneName", area.nicename);
        velocityContext.put("creationDate", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(Year.of(2016).atMonth(6).atDay(29)));
        velocityContext.put("westLon", area.left - 180);
        velocityContext.put("eastLon", area.right - 180);
        velocityContext.put("northLat", 90 - area.top);
        velocityContext.put("southLat", 90 - area.bottom);
        velocityContext.put("begin", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.systemDefault()).format(Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atDay(1).atTime(0, 0, 0)));
        velocityContext.put("end", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.systemDefault()).format(Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atDay(Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth()).atTime(23, 59, 59)));

        StringWriter stringWriter = new StringWriter();
        velocityEngine.evaluate(velocityContext, stringWriter, "pixelFormatting", TEMPLATE.replace("${REPLACE_WITH_VERSION}", version));
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

    static String createBaseFilename(String year, String month, String version, PixelProductArea area) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MERIS-AREA_%s-f%s", year, month, area.index, version);
    }

    private static final String TEMPLATE = "" +
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
            "                        <gco:CharacterString>Fire_cci Pixel MERIS Burned Area product v4.1 - Zone ${zoneId}: ${zoneName}" +
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
            "                                <gco:Date>2016-07-12</gco:Date>" +
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
            "                                <gco:CharacterString>doi:10.5285/8723B4DD-49A4-4C37-9A3C-BA4392EEE63B</gco:CharacterString>" +
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
            "scientists and researchers supporting their modelling efforts. Further information to the ESA CCI " +
            "Programme and a comprehensive documentation on the underlying algorithms, work flow, production " +
            "system and product validation is publicly accessible on https://www.esa-fire-cci.org/." +
            "</gco:CharacterString>" +
            "<gco:CharacterString>" +
            "#[[" +
            "The product is a multi-layer TIFF with the following naming convention: ${IndicativeDate}-ESACCI-L3S_FIRE-BA-" +
            "${Indicative sensor}[-${Additional Segregator}]-fv${xx.x}.tif. " +
            "${Indicative Date} is the identifying date for this data set. Format is YYYY[MM[DD]], where YYYY is the " +
            "four digit year, MM is the two digit month from 01 to 12 and DD is the two digit day of the month from 01 to " +
            "31. For monthly products the date will be set to 01. " +
            "${Indicative sensor} is MERIS. ${Additional Segregator} " +
            "is the AREA_${TILE_NUMBER} being the tile number the subset index described in Extent. fv" +
            "${File Version} is the File version number in the form n{1,}[.n{1,}] (That is 1 or more digits followed by optional " +
            ". and another 1 or more digits.). An example is: " +
            "20050301-ESACCI-L3S_FIRE-BA-MERIS-AREA_1-f${REPLACE_WITH_VERSION}.tif.]]#" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>For further information on the product, please consult the Product User Guide: Fire_cci_D3.3_PUG_v2 available at: www.esa-fire.cci.org/documents" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 1: Date of the first detection; Pixel Spacing = 0.00277778 deg.; Pixel " +
            "value = Day of the year, from 1 to 365 (or 366) A value of 0 is included when the pixel is not burned in " +
            "the month or it is not observed; a value of 999 is allocated to pixels that are not taken into account in " +
            "the burned area processing (continuous water, ocean).; Data type = Integer; Number of layers = 1; Data depth = 16" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 2: Confidence Level; Pixel Spacing = 0.00277778 deg.; Pixel value = 0 to " +
            "100, where the value is the probability in percentage that the pixel is actually burned, as a result " +
            "of both the pre-processing and the actual burned area classification. The higher the value, the " +
            "higher the confidence that the pixel is actually burned. A value of 999 is allocated to pixels that are " +
            "not taken into account in the burned area processing (continuous water, ocean). Data type = Integer; " +
            "Number of layers = 1;" +
            "Data depth = 16" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 3: Land cover of that pixel, extracted from the CCI LandCover (LC). N is the " +
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
            "Pixel Spacing = 0.00277778 deg.; Data type = Integer; Number of layers = 1; Data depth = 16" +
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
            "                                        <gmd:URL>http://esa-fire-cci.org/</gmd:URL>" +
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
            "                        <gco:CharacterString>ESA CCI</gco:CharacterString>" +
            "                    </gmd:organisationName>" +
            "                    <gmd:contactInfo>" +
            "                        <gmd:CI_Contact>" +
            "                            <gmd:address>" +
            "                                <gmd:CI_Address>" +
            "                                    <gmd:electronicMailAddress>" +
            "                                        <gco:CharacterString>help@esa-portal-cci.org</gco:CharacterString>" +
            "                                    </gmd:electronicMailAddress>" +
            "                                </gmd:CI_Address>" +
            "                            </gmd:address>" +
            "                            <gmd:onlineResource>" +
            "                                <gmd:CI_OnlineResource>" +
            "                                    <gmd:linkage>" +
            "                                        <gmd:URL>http://cci.esa.int/data/</gmd:URL>" +
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
            "                        <gco:Distance uom=\"degrees\">0.00277778</gco:Distance>" +
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

}
