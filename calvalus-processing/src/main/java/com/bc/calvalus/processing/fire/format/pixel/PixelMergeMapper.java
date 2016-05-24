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
import com.bc.calvalus.processing.beam.GpfUtils;
import com.bc.ceres.core.NullProgressMonitor;
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
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
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
        GpfUtils.init(context.getConfiguration());  // set system properties from request
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

        String baseFilename = createBaseFilename(year, month, area);
        Product result = new Product(baseFilename, "fire-cci-pixel-product", inputVar1.getSceneRasterWidth(), inputVar1.getSceneRasterHeight());
        ProductUtils.copyGeoCoding(inputVar1, result);
        ProductUtils.copyBand(inputVar1.getBandAt(0).getName(), inputVar1, result, true);
        ProductUtils.copyBand(inputVar2.getBandAt(0).getName(), inputVar2, result, true);
        ProductUtils.copyBand(inputVar3.getBandAt(0).getName(), inputVar3, result, true);

        CalvalusLogger.getLogger().info("Creating metadata...");
        String metadata = createMetadata(year, month, area);
        try (FileWriter fw = new FileWriter(baseFilename + ".xml")) {
            fw.write(metadata);
        }
        CalvalusLogger.getLogger().info("...done. Writing final product...");
        ProductIO.writeProduct(result, baseFilename + ".tif", BigGeoTiffProductWriterPlugIn.FORMAT_NAME, new NullProgressMonitor() {
            @Override
            public void internalWorked(double work) {
                CalvalusLogger.getLogger().info("Tile written.");
                context.progress();
            }
        });
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

    static String createMetadata(String year, String month, PixelProductArea area) {

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

        return stringWriter.toString();
    }

    static String createBaseFilename(String year, String month, PixelProductArea area) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MERIS-AREA_%d-v02.0-fv04.0", year, month, area.index);
    }

    private static void createTarGZ(String filePath, String xmlPath, String outputPath) throws IOException {
        try (OutputStream fOut = new FileOutputStream(new File(outputPath));
             OutputStream bOut = new BufferedOutputStream(fOut);
             OutputStream gzOut = new GzipCompressorOutputStream(bOut);
             TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {
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
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<gmi:MI_Metadata\n" +
            "        xsi:schemaLocation=\"http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd   http://www.isotc211.org/2005/gmi http://www.isotc211.org/2005/gmi/gmi.xsd\"\n" +
            "        xmlns:gmd=\"http://www.isotc211.org/2005/gmd\"\n" +
            "        xmlns:gco=\"http://www.isotc211.org/2005/gco\"\n" +
            "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "        xmlns:gmi=\"http://www.isotc211.org/2005/gmi\"\n" +
            ">\n" +
            "\n" +
            "    <gmd:fileIdentifier>\n" +
            "        <gco:CharacterString>urn:uuid:esa-cci:fire:pixel:${UUID}</gco:CharacterString>\n" +
            "    </gmd:fileIdentifier>\n" +
            "\n" +
            "    <gmd:language>\n" +
            "        <gmd:LanguageCode codeList=\"http://www.loc.gov/standards/iso639-2/\" codeListValue=\"eng\">eng</gmd:LanguageCode>\n" +
            "    </gmd:language>\n" +
            "\n" +
            "    <gmd:characterSet>\n" +
            "        <gmd:MD_CharacterSetCode codeSpace=\"ISOTC211/19115\" codeListValue=\"MD_CharacterSetCode_utf8\"\n" +
            "                                 codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_CharacterSetCode\">\n" +
            "            MD_CharacterSetCode_utf8</gmd:MD_CharacterSetCode>\n" +
            "    </gmd:characterSet>\n" +
            "\n" +
            "    <gmd:hierarchyLevel>\n" +
            "        <gmd:MD_ScopeCode codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_ScopeCode\"\n" +
            "                          codeListValue=\"dataset\">dataset\n" +
            "        </gmd:MD_ScopeCode>\n" +
            "    </gmd:hierarchyLevel>\n" +
            "\n" +
            "    <gmd:contact>\n" +
            "        <gmd:CI_ResponsibleParty>\n" +
            "            <gmd:organisationName>\n" +
            "                <gco:CharacterString>University of Alcala, Spain</gco:CharacterString>\n" +
            "            </gmd:organisationName>\n" +
            "            <gmd:contactInfo>\n" +
            "                <gmd:CI_Contact>\n" +
            "                    <gmd:address>\n" +
            "                        <gmd:CI_Address>\n" +
            "                            <gmd:electronicMailAddress>\n" +
            "                                <gco:CharacterString>emilio.chuvieco@uah.es></gco:CharacterString>\n" +
            "                            </gmd:electronicMailAddress>\n" +
            "                        </gmd:CI_Address>\n" +
            "                    </gmd:address>\n" +
            "                    <gmd:onlineResource>\n" +
            "                        <gmd:CI_OnlineResource>\n" +
            "                            <gmd:linkage>\n" +
            "                                <gmd:URL>http://www.geogra.uah.es/emilio/</gmd:URL>\n" +
            "                            </gmd:linkage>\n" +
            "                        </gmd:CI_OnlineResource>\n" +
            "                    </gmd:onlineResource>\n" +
            "                </gmd:CI_Contact>\n" +
            "            </gmd:contactInfo>\n" +
            "            <gmd:role>\n" +
            "                <gmd:CI_RoleCode\n" +
            "                        codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"\n" +
            "                        codeListValue=\"resourceProvider\">resourceProvider\n" +
            "                </gmd:CI_RoleCode>\n" +
            "            </gmd:role>\n" +
            "        </gmd:CI_ResponsibleParty>\n" +
            "    </gmd:contact>\n" +
            "\n" +
            "    <gmd:dateStamp>\n" +
            "        <gco:Date>${date}</gco:Date>\n" +
            "    </gmd:dateStamp>\n" +
            "\n" +
            "    <gmd:metadataStandardName>\n" +
            "        <gco:CharacterString>ISO 19115-1 Geographic Information - Metadata Part 2 Extensions for imagery and gridded\n" +
            "            data\n" +
            "        </gco:CharacterString>\n" +
            "    </gmd:metadataStandardName>\n" +
            "\n" +
            "    <gmd:metadataStandardVersion>\n" +
            "        <gco:CharacterString>ISO 19115-1:2012</gco:CharacterString>\n" +
            "    </gmd:metadataStandardVersion>\n" +
            "\n" +
            "    <gmd:referenceSystemInfo>\n" +
            "        <gmd:MD_ReferenceSystem>\n" +
            "            <gmd:referenceSystemIdentifier>\n" +
            "                <gmd:RS_Identifier>\n" +
            "                    <gmd:authority>\n" +
            "                        <CI_citation>\n" +
            "                            <gco:CharacterString>WGS84</gco:CharacterString>\n" +
            "                        </CI_citation>\n" +
            "                    </gmd:authority>\n" +
            "                    <gmd:code>\n" +
            "                        <gco:CharacterString>WGS84</gco:CharacterString>\n" +
            "                    </gmd:code>\n" +
            "                </gmd:RS_Identifier>\n" +
            "            </gmd:referenceSystemIdentifier>\n" +
            "        </gmd:MD_ReferenceSystem>\n" +
            "    </gmd:referenceSystemInfo>\n" +
            "\n" +
            "    <gmd:identificationInfo>\n" +
            "        <gmd:MD_DataIdentification>\n" +
            "\n" +
            "            <gmd:citation>\n" +
            "                <gmd:CI_Citation>\n" +
            "                    <gmd:title>\n" +
            "                        <gco:CharacterString>Fire Disturbance ECV (Fire_cci) Pixel Product – Zone ${zoneId} –\n" +
            "                            ${zoneName}\n" +
            "                        </gco:CharacterString>\n" +
            "                    </gmd:title>\n" +
            "                    <gmd:date>\n" +
            "                        <!-- creation date-->\n" +
            "                        <gmd:CI_Date>\n" +
            "                            <gmd:date>\n" +
            "                                <gco:Date>${creationDate}</gco:Date>\n" +
            "                            </gmd:date>\n" +
            "                            <gmd:dateType>\n" +
            "                                <gmd:CI_DateTypeCode\n" +
            "                                        codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode\"\n" +
            "                                        codeListValue=\"creation\">creation\n" +
            "                                </gmd:CI_DateTypeCode>\n" +
            "                            </gmd:dateType>\n" +
            "                        </gmd:CI_Date>\n" +
            "                    </gmd:date>\n" +
            "                    <gmd:date>\n" +
            "                        <!-- publication date-->\n" +
            "                        <gmd:CI_Date>\n" +
            "                            <gmd:date>\n" +
            "                                <gco:Date>2016-06-08</gco:Date>\n" +
            "                            </gmd:date>\n" +
            "                            <gmd:dateType>\n" +
            "                                <gmd:CI_DateTypeCode\n" +
            "                                        codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode\"\n" +
            "                                        codeListValue=\"publication\">publication\n" +
            "                                </gmd:CI_DateTypeCode>\n" +
            "                            </gmd:dateType>\n" +
            "                        </gmd:CI_Date>\n" +
            "                    </gmd:date>\n" +
            "\n" +
            "                    <gmd:identifier>\n" +
            "                        <gmd:RS_Identifier>\n" +
            "                            <gmd:code>\n" +
            "                                <gco:CharacterString>doi:***********</gco:CharacterString>\n" +
            "                            </gmd:code>\n" +
            "                        </gmd:RS_Identifier>\n" +
            "                    </gmd:identifier>\n" +
            "                </gmd:CI_Citation>\n" +
            "            </gmd:citation>\n" +
            "\n" +
            "            <gmd:abstract>\n" +
            "                <gco:CharacterString>In support of the IPCC, the ESA Climate Change Initiative (CCI) programme comprises\n" +
            "                    the generation of thirteen projects, each focusing on the production of global coverage of an\n" +
            "                    Essential Climate Variable (ECV). The ECV Fire Disturbance (Fire_cci) provides validated,\n" +
            "                    error-characterised, global data sets of burnt areas (BA) derived from existing satellite\n" +
            "                    observations (SPOT-VGT, ENVISAT (A)ATSR, ENVISAT MERIS). The Fire_cci BA products consist of a Pixel\n" +
            "                    and Grid product addressing the needs and requirements of climate, atmospheric and ecosystem\n" +
            "                    scientists and researchers supporting their modelling efforts. Further information to the ESA CCI\n" +
            "                    Programme and a comprehensive documentation on the underlying algorithms, work flow, production\n" +
            "                    system and product validation is publicly accessible on https://www.esa-fire-cci.org/ .\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>The product is a multi-layer TIFF with the following naming convention:<Indicative\n" +
            "                        Date>-ESACCI-L3S_FIRE-BA-<Indicative sensor>[-<Additional Segregator>]-[-v<GDS version>]-fv\n" +
            "                    <xx.x>.tif.\n" +
            "                    <Indicative Date>\n" +
            "                    is the identifying date for this data set. Format is YYYY[MM[DD]], where YYYY is the four digit\n" +
            "                    year, MM is the two digit month from 01 to 12 and DD is the two digit day of the month from 01 to\n" +
            "                    31. For monthly products the date will be set to 01.\n" +
            "                    <Indicative sensor>\n" +
            "                    is MERIS, when data coming from MERIS sensor; MODISVNIR when outputs come from MODIS 250m channels;\n" +
            "                    MSI, when it comes from Sentinel-2 MSI; OLCI, Sentinel-3; SLSTR, Sentinel-3; PROBA, PROBA-V.\n" +
            "                    <Additional Segregator>\n" +
            "                    is the AREA_\n" +
            "                    <TILE_NUMBER>\n" +
            "                    being the tile number the subset index described in Extent. v\n" +
            "                    <GDS version>\n" +
            "                    is the version number of the GHRSST Data Specification is optional for the CCI file naming\n" +
            "                    convention. fv\n" +
            "                    <File Version>\n" +
            "                    is the File version number in the form n{1,}[.n{1,}] (That is 1 or more digits followed by optional\n" +
            "                    . and another 1 or more digits.). An example is:\n" +
            "                    20050301-ESACCI-L3S_FIRE-BA-MERIS-AREA_1-fv04.0.tif.\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Product Specification at: Chuvieco, E. (2013). ESA CCI ECV Fire Disturbance -\n" +
            "                    Product Specification Document, Fire_cci_Ph2_UAH_D1_2_PSD_v3_0.pdf\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Algorithm Theoretical Basis Document – Pre-Processing:\n" +
            "                    https://www.esa-fire-cci.org/\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Algorithm Theoretical Basis Document – BA detection:\n" +
            "                    https://www.esa-fire-cci.org/\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Algorithm Theoretical Basis Document – BA merging: https://www.esa-fire-cci.org/\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Product Validation and Algorithm Selection Report: https://www.esa-fire-cci.org/\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Layer JD: Date of the first detection; Pixel Spacing = 300m; TIFF; A zero (0) will\n" +
            "                    be included in this field when the pixel is not burned in the month or it is not observed; Pixel\n" +
            "                    value = Day of the year, from 1 to 365; Data type = REAL; Number of layers = 1; Data depth = 16\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Layer CL: Uncertainty Confidence Layer; Pixel Spacing = 300m; Pixel value = 0 to\n" +
            "                    100, where the value is the probability in percentage that the pixel is actually burned, as a result\n" +
            "                    of both the pre-processing and the actual burned area classification. The higher the value, the\n" +
            "                    higher the confidence that the pixel is actually burned. Data type = REAL; Number of layers = 1;\n" +
            "                    Data depth = 16\n" +
            "                </gco:CharacterString>\n" +
            "                <gco:CharacterString>Layer LC: Land cover of that pixel, extracted from the CCI LandCover (LC). The 2000\n" +
            "                    LC data is used for 2002 data, the 2005 LC data is used for data from 2003 to 2007, and the 2010 LC\n" +
            "                    data is used for data from 2008 to 2012. N is the number of land cover categories in the reference\n" +
            "                    map. It is only valid when layer 1 > 0. Pixel value is 0 to N where under the following codes: 11 =\n" +
            "                    Post-flooding or irrigated croplands (or aquatic), 14 = Rainfed croplands, 20 = Mosaic cropland\n" +
            "                    (50-70%) / vegetation (grassland/shrubland/forest) (20-50%), 30 = Mosaic vegetation\n" +
            "                    (grassland/shrubland/forest) (50-70%) / cropland (20-50%), 40 = Closed to open (>15%) broadleaved\n" +
            "                    evergreen or semi-deciduous forest (>5m), 50 = Closed (>40%) broadleaved deciduous forest (>5m), 60\n" +
            "                    = Open (15-40%) broadleaved deciduous forest/woodland (>5m), 70 = Closed (>40%) needleleaved\n" +
            "                    evergreen forest (>5m), 90 = Open (15-40%) needleleaved deciduous or evergreen forest (>5m), 100 =\n" +
            "                    Closed to open (>15%) mixed broadleaved and needleleaved forest (>5m), 110 = Mosaic forest or\n" +
            "                    shrubland (50-70%) / grassland (20-50%), 120 = Mosaic grassland (50-70%) / forest or shrubland\n" +
            "                    (20-50%) , 130 = Closed to open (>15%) (broadleaved or needleleaved, evergreen or deciduous)\n" +
            "                    shrubland (<5m), 140 = Closed to open (>15%) herbaceous vegetation (grassland, savannas or\n" +
            "                    lichens/mosses), 150 = Sparse (<15%) vegetation, 160 = Closed to open (>15%) broadleaved forest\n" +
            "                    regularly flooded (semi-permanently or temporarily) - Fresh or brackish water, 170 = Closed (>40%)\n" +
            "                    broadleaved forest or shrubland permanently flooded - Saline or brackish water, 180 = Closed to open\n" +
            "                    (>15%) grassland or woody vegetation on regularly flooded or waterlogged soil - Fresh, brackish or\n" +
            "                    saline water, 190 = Others (Artificial surfaces and associated areas (Urban areas >50%), 200 = Bare\n" +
            "                    areas, 210 = Water bodies, 220 = Permanent snow and ice, 230 = No data (burnt areas, clouds etc.,);\n" +
            "                    Pixel Spacing = 300m; Data type = REAL; Number of layers = 1; Data depth = 16\n" +
            "                </gco:CharacterString>\n" +
            "            </gmd:abstract>\n" +
            "\n" +
            "            <gmd:pointOfContact>\n" +
            "                <gmd:CI_ResponsibleParty>\n" +
            "                    <!-- resourceProvider-->\n" +
            "                    <gmd:organisationName>\n" +
            "                        <gco:CharacterString>European Space Agency</gco:CharacterString>\n" +
            "                    </gmd:organisationName>\n" +
            "                    <gmd:contactInfo>\n" +
            "                        <gmd:CI_Contact>\n" +
            "                            <gmd:address>\n" +
            "                                <gmd:CI_Address>\n" +
            "                                    <gmd:electronicMailAddress>\n" +
            "                                        <gco:CharacterString>Stephen.plummer@esa.int></gco:CharacterString>\n" +
            "                                    </gmd:electronicMailAddress>\n" +
            "                                </gmd:CI_Address>\n" +
            "                            </gmd:address>\n" +
            "                            <gmd:onlineResource>\n" +
            "                                <gmd:CI_OnlineResource>\n" +
            "                                    <gmd:linkage>\n" +
            "                                        <gmd:URL>http://www.esa.int</gmd:URL>\n" +
            "                                    </gmd:linkage>\n" +
            "                                </gmd:CI_OnlineResource>\n" +
            "                            </gmd:onlineResource>\n" +
            "                        </gmd:CI_Contact>\n" +
            "                    </gmd:contactInfo>\n" +
            "                    <gmd:role>\n" +
            "                        <gmd:CI_RoleCode\n" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"\n" +
            "                                codeListValue=\"resourceProvider \">resourceProvider\n" +
            "                        </gmd:CI_RoleCode>\n" +
            "                    </gmd:role>\n" +
            "                </gmd:CI_ResponsibleParty>\n" +
            "            </gmd:pointOfContact>\n" +
            "\n" +
            "            <gmd:pointOfContact>\n" +
            "                <gmd:CI_ResponsibleParty>\n" +
            "                    <!-- Custodian-->\n" +
            "                    <gmd:organisationName>\n" +
            "                        <gco:CharacterString>University of Alcala</gco:CharacterString>\n" +
            "                    </gmd:organisationName>\n" +
            "                    <gmd:contactInfo>\n" +
            "                        <gmd:CI_Contact>\n" +
            "                            <gmd:address>\n" +
            "                                <gmd:CI_Address>\n" +
            "                                    <gmd:electronicMailAddress>\n" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es></gco:CharacterString>\n" +
            "                                    </gmd:electronicMailAddress>\n" +
            "                                </gmd:CI_Address>\n" +
            "                            </gmd:address>\n" +
            "                            <gmd:onlineResource>\n" +
            "                                <gmd:CI_OnlineResource>\n" +
            "                                    <gmd:linkage>\n" +
            "                                        <gmd:URL>http://www.geogra.uah.es/emilio/</gmd:URL>\n" +
            "                                    </gmd:linkage>\n" +
            "                                </gmd:CI_OnlineResource>\n" +
            "                            </gmd:onlineResource>\n" +
            "                        </gmd:CI_Contact>\n" +
            "                    </gmd:contactInfo>\n" +
            "                    <gmd:role>\n" +
            "                        <gmd:CI_RoleCode\n" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"\n" +
            "                                codeListValue=\"Custodian \">Custodian\n" +
            "                        </gmd:CI_RoleCode>\n" +
            "                    </gmd:role>\n" +
            "                </gmd:CI_ResponsibleParty>\n" +
            "            </gmd:pointOfContact>\n" +
            "\n" +
            "            <gmd:pointOfContact>\n" +
            "                <gmd:CI_ResponsibleParty>\n" +
            "                    <!-- Owner-->\n" +
            "                    <gmd:organisationName>\n" +
            "                        <gco:CharacterString>European Space Agency</gco:CharacterString>\n" +
            "                    </gmd:organisationName>\n" +
            "                    <gmd:contactInfo>\n" +
            "                        <gmd:CI_Contact>\n" +
            "                            <gmd:address>\n" +
            "                                <gmd:CI_Address>\n" +
            "                                    <gmd:electronicMailAddress>\n" +
            "                                        <gco:CharacterString>Stephen.plummer@esa.int></gco:CharacterString>\n" +
            "                                    </gmd:electronicMailAddress>\n" +
            "                                </gmd:CI_Address>\n" +
            "                            </gmd:address>\n" +
            "                            <gmd:onlineResource>\n" +
            "                                <gmd:CI_OnlineResource>\n" +
            "                                    <gmd:linkage>\n" +
            "                                        <gmd:URL>http://www.esa.int</gmd:URL>\n" +
            "                                    </gmd:linkage>\n" +
            "                                </gmd:CI_OnlineResource>\n" +
            "                            </gmd:onlineResource>\n" +
            "                        </gmd:CI_Contact>\n" +
            "                    </gmd:contactInfo>\n" +
            "                    <gmd:role>\n" +
            "                        <gmd:CI_RoleCode\n" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"\n" +
            "                                codeListValue=\"Owner \">Owner\n" +
            "                        </gmd:CI_RoleCode>\n" +
            "                    </gmd:role>\n" +
            "                </gmd:CI_ResponsibleParty>\n" +
            "            </gmd:pointOfContact>\n" +
            "\n" +
            "            <gmd:pointOfContact>\n" +
            "                <gmd:CI_ResponsibleParty>\n" +
            "                    <!-- principalInvestigator-->\n" +
            "                    <gmd:organisationName>\n" +
            "                        <gco:CharacterString>University of Alcala</gco:CharacterString>\n" +
            "                    </gmd:organisationName>\n" +
            "                    <gmd:contactInfo>\n" +
            "                        <gmd:CI_Contact>\n" +
            "                            <gmd:address>\n" +
            "                                <gmd:CI_Address>\n" +
            "                                    <gmd:electronicMailAddress>\n" +
            "                                        <gco:CharacterString>emilio.chuvieco@uah.es</gco:CharacterString>\n" +
            "                                    </gmd:electronicMailAddress>\n" +
            "                                </gmd:CI_Address>\n" +
            "                            </gmd:address>\n" +
            "                            <gmd:onlineResource>\n" +
            "                                <gmd:CI_OnlineResource>\n" +
            "                                    <gmd:linkage>\n" +
            "                                        <gmd:URL>http://www.geogra.uah.es/emilio/</gmd:URL>\n" +
            "                                    </gmd:linkage>\n" +
            "                                </gmd:CI_OnlineResource>\n" +
            "                            </gmd:onlineResource>\n" +
            "                        </gmd:CI_Contact>\n" +
            "                    </gmd:contactInfo>\n" +
            "                    <gmd:role>\n" +
            "                        <gmd:CI_RoleCode\n" +
            "                                codeList=\"http://standards.iso.org/ittf/PubliclyAvailableStandards/ISO_19139_Schemas/resources/Codelist/gmxCodelists.xml#CI_RoleCode\"\n" +
            "                                codeListValue=\"principalInvestigator\">principalInvestigator\n" +
            "                        </gmd:CI_RoleCode>\n" +
            "                    </gmd:role>\n" +
            "                </gmd:CI_ResponsibleParty>\n" +
            "            </gmd:pointOfContact>\n" +
            "\n" +
            "            <gmd:descriptiveKeywords>\n" +
            "                <gmd:MD_Keywords>\n" +
            "                    <gmd:keyword>\n" +
            "                        <gco:CharacterString>Burned Area:Fire:Climate:Change:ESA:GCOS</gco:CharacterString>\n" +
            "                    </gmd:keyword>\n" +
            "                    <gmd:type>\n" +
            "                        <gmd:MD_KeywordTypeCode codeList=\"./resources/codeList.xml#MD_KeywordTypeCode\"\n" +
            "                                                codeListValue=\"theme\">theme\n" +
            "                        </gmd:MD_KeywordTypeCode>\n" +
            "                    </gmd:type>\n" +
            "                </gmd:MD_Keywords>\n" +
            "            </gmd:descriptiveKeywords>\n" +
            "\n" +
            "            <gmd:resourceConstraints>\n" +
            "                <gmd:MD_Constraints>\n" +
            "                    <gmd:useLimitation>\n" +
            "                        <gco:CharacterString>esaNoMonetaryCharge</gco:CharacterString>\n" +
            "                    </gmd:useLimitation>\n" +
            "                </gmd:MD_Constraints>\n" +
            "            </gmd:resourceConstraints>\n" +
            "            <gmd:resourceConstraints>\n" +
            "                <gmd:MD_LegalConstraints>\n" +
            "                    <gmd:accessConstraints>\n" +
            "                        <gmd:MD_RestrictionCode\n" +
            "                                codeList=\"http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_RestrictionCode\"\n" +
            "                                codeListValue=\"otherRestrictions\">otherRestrictions\n" +
            "                        </gmd:MD_RestrictionCode>\n" +
            "                    </gmd:accessConstraints>\n" +
            "                    <gmd:otherConstraints>\n" +
            "                        <gco:CharacterString>no limitation</gco:CharacterString>\n" +
            "                    </gmd:otherConstraints>\n" +
            "                </gmd:MD_LegalConstraints>\n" +
            "            </gmd:resourceConstraints>\n" +
            "\n" +
            "            <gmd:spatialResolution>\n" +
            "                <gmd:MD_Resolution>\n" +
            "                    <gmd:distance>\n" +
            "                        <gco:Distance uom=\"metres\">300</gco:Distance>\n" +
            "                    </gmd:distance>\n" +
            "                </gmd:MD_Resolution>\n" +
            "            </gmd:spatialResolution>\n" +
            "\n" +
            "            <gmd:language>\n" +
            "                <gmd:LanguageCode codeList=\"http://www.loc.gov/standards/iso639-2/\" codeListValue=\"eng\">eng\n" +
            "                </gmd:LanguageCode>\n" +
            "            </gmd:language>\n" +
            "\n" +
            "            <gmd:extent>\n" +
            "                <gmd:EX_Extent>\n" +
            "                    <gmd:geographicElement>\n" +
            "                        <gmd:EX_GeographicBoundingBox>\n" +
            "                            <gmd:westBoundLongitude>\n" +
            "                                <gco:Decimal>${westLon}</gco:Decimal>\n" +
            "                            </gmd:westBoundLongitude>\n" +
            "                            <gmd:eastBoundLongitude>\n" +
            "                                <gco:Decimal>${eastLon}</gco:Decimal>\n" +
            "                            </gmd:eastBoundLongitude>\n" +
            "                            <gmd:southBoundLatitude>\n" +
            "                                <gco:Decimal>${southLat}</gco:Decimal>\n" +
            "                            </gmd:southBoundLatitude>\n" +
            "                            <gmd:northBoundLatitude>\n" +
            "                                <gco:Decimal>${northLat}</gco:Decimal>\n" +
            "                            </gmd:northBoundLatitude>\n" +
            "                        </gmd:EX_GeographicBoundingBox>\n" +
            "                    </gmd:geographicElement>\n" +
            "                </gmd:EX_Extent>\n" +
            "            </gmd:extent>\n" +
            "\n" +
            "        </gmd:MD_DataIdentification>\n" +
            "    </gmd:identificationInfo>\n" +
            "\n" +
            "</gmi:MI_Metadata>";

}
