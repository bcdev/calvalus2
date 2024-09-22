package com.bc.calvalus.processing.fire.format.pixel.syn;

import com.bc.calvalus.processing.fire.ContinentalArea;
import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SynMetadataGenerator extends PixelFinaliseMapper {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Arguments: outputDir year doi publicationDate");
            System.out.println("e.g.     : . 2022  doi publicationDate");
            System.exit(-1);
        }
        String outputDir = args[0];
        String year = args[1];
        String doi = args[2];
        String publicationDate = args[3];

        for (int a = 1; a <= 6; a++) {
            String areaKey = "AREA_" + a;
            String area = getArea(areaKey);
            for (int i = 1; i <= 12; i++) {
                String month = String.format("%02d", i);
                String filename = String.format("%s%s01-ESACCI-L3S_FIRE-BA-SYN-%s-fv1.1.xml", year, month, areaKey);
                Path xmlPath = Paths.get(outputDir, filename);
                if (!Files.exists(xmlPath)) {
                    String metadata = createMetadata(template, year, month, "1.1", area, doi, publicationDate);
                    try (FileWriter fw = new FileWriter(new File(outputDir, filename))) {
                        fw.write(metadata);
                    }
                }
            }
        }
    }

    private static String getArea(String areaKey) {
        switch (areaKey) {
            case "AREA_1":
                return ContinentalArea.northamerica.toString();
            case "AREA_2":
                return ContinentalArea.southamerica.toString();
            case "AREA_3":
                return ContinentalArea.europe.toString();
            case "AREA_4":
                return ContinentalArea.asia.toString();
            case "AREA_5":
                return ContinentalArea.africa.toString();
            case "AREA_6":
                return ContinentalArea.australia.toString();
            case "AREA_7":
                return ContinentalArea.greenland.toString();
            default:
                throw new IllegalArgumentException("Area key must be one of AREA_1 .. AREA_6");
        }
    }

    private static String template =
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
                    "                                <gmd:URL>https://climate.esa.int/en/projects/fire/</gmd:URL>" +
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
                    "                        <gco:CharacterString>Fire_cci Sentinel-3 SYN Burned Area Pixel product, version ${REPLACE_WITH_VERSION} - Area ${zoneId}" +
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
                    "                                <gco:Date>${publicationDate}</gco:Date>" +
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
                    "                                <gco:CharacterString>${doi}</gco:CharacterString>" +
                    "                            </gmd:code>" +
                    "                        </gmd:MD_Identifier>" +
                    "                    </gmd:identifier>" +
                    "                </gmd:CI_Citation>" +
                    "            </gmd:citation>" +
                    "" +
                    "<gmd:abstract>" +
                    "<gco:CharacterString>In support of the IPCC, the ESA Climate Change Initiative (CCI) programme" +
                    " comprises different projects, each focusing on the production of global coverage of an Essential" +
                    " Climate Variable (ECV). The ECV Fire Disturbance (Fire_cci) provides validated," +
                    " error-characterised, global data sets of burned area (BA) derived from existing satellite" +
                    " observations. The Fire_cci BA products consist of a Pixel and Grid product addressing the needs" +
                    " and requirements of climate, atmospheric and ecosystem scientists and researchers supporting" +
                    " their modelling efforts. Further information on the ESA CCI Programme and a comprehensive" +
                    " documentation on the underlying algorithms, workflow, production system and product validation" +
                    " is publicly accessible on https://climate.esa.int/en/projects/fire/." +
                    "</gco:CharacterString>" +
                    "<gco:CharacterString>" +
                    "#[[" +
                    "The product is a set of single-layer GeoTIFF files with the following naming convention: " +
                    "${Indicative Date}-ESACCI-L3S_FIRE-BA-${Indicative sensor}[-${Additional Segregator}]-fv${xx.x}" +
                    "[-${layer}].tif. ${Indicative Date} is the identifying date for this data set. Format is YYYYMMDD," +
                    " where YYYY is the four-digit year, MM is the two-digit month from 01 to 12 and DD is the" +
                    " two-digit day of the month from 01 to 31. For monthly products, the date is set to 01." +
                    " ${Indicative sensor} is SYN, corresponding to the Synergy product derived from the Sentinel-3" +
                    " OLCI and SLSTR sensors. ${Additional Segregator} is the AREA_${TILE_CODE} being the tile code" +
                    " described in the Product User Guide. ${File Version} is the File version number in the form" +
                    " n{1,}[.n{1,}] (That is 1 or more digits followed by optional . and another 1 or more digits.)." +
                    " ${layer} is the code for the layer represented in each file, being: JD: layer 1, CL: layer 2," +
                    " and LC: layer 3. An example is: 20190301-ESACCI-L3S_FIRE-BA-SYN-AREA_5-fv1.1-JD.tif.]]#" +
                    "</gco:CharacterString>" +
                    "<gco:CharacterString>For further information on the product, please consult the Product User Guide." +
                    "</gco:CharacterString>" +
                    "<gco:CharacterString>Layer 1: Date of the first detection; Pixel Spacing = 0.00277777778 deg  (approx. 300m); " +
                    "Pixel value = Day of the year, from 1 to 365 (or 366). A value of 0 is included when the pixel is not burned " +
                    "in the month; a value of -1 is allocated to pixels that are not observed in the month; a value of -2 is " +
                    "allocated to pixels that are not burnable (urban areas, bare areas, water bodies and permanent snow and " +
                    "ice). Data type = Integer; Number of layers = 1; Data depth = 16" +
                    "</gco:CharacterString>" +
                    "<gco:CharacterString>Layer 2: Confidence Level; Pixel Spacing = 0.00277777778 deg  (approx. 300m); Pixel " +
                    "value = 0 to 100, where the value is the probability in percentage that the pixel is actually burned, as a " +
                    "result of the different steps of the burned area classification. The higher the value, the higher the " +
                    "confidence that the pixel is actually burned. A value of 0 is allocated to pixels that are not observed in " +
                    "the month, or not taken into account in the burned area processing (non-burnable). Data type = Byte; Number " +
                    "of layers = 1; Data depth = 8" +
                    "</gco:CharacterString>" +
                    "<gco:CharacterString>Layer 3: Land cover of that pixel, extracted from the C3S LandCover v2.1.1 (LC). N is the " +
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
                    "Pixel Spacing = 0.00277777778 deg  (approx. 300m); Data type = Byte; Number of layers = 1; Data depth = 8" +
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
                    "                                        <gmd:URL>https://climate.esa.int/en/projects/fire/</gmd:URL>" +
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
                    "                                        <gmd:URL>https://climate.esa.int/en/projects/fire/</gmd:URL>" +
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
                    "                                        <gmd:URL>https://climate.esa.int/en/projects/fire/</gmd:URL>" +
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
                    "                        <gco:Distance uom=\"degrees\">0.00277777778</gco:Distance>" +
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


    @Override
    protected Product collocateWithSource(Product lcProduct, Product source) {
        return null;
    }

    @Override
    protected String getCalvalusSensor() {
        return null;
    }

    @Override
    protected Band getLcBand(Product lcProduct) {
        return null;
    }

    @Override
    protected ClScaler getClScaler() {
        return null;
    }

    @Override
    public String createBaseFilename(String year, String month, String version, String areaString) {
        return null;
    }
}
