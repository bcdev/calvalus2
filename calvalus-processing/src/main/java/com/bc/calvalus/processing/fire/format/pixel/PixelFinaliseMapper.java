package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.analysis.QuicklookGenerator;
import com.bc.calvalus.processing.analysis.Quicklooks;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.ceres.core.PrintWriterProgressMonitor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Progressable;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.image.SingleBandedOpImage;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.esa.snap.watermask.operator.WatermaskOp;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import javax.imageio.ImageIO;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
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
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Logger;

public class PixelFinaliseMapper extends Mapper {

    private static final Logger LOG = CalvalusLogger.getLogger();
    static final int TILE_SIZE = 256;

    public static final String KEY_LC_PATH = "LC_PATH";
    public static final String KEY_VERSION = "VERSION";
    public static final String KEY_AREA_STRING = "AREA_STRING";
    public static final String KEY_SENSOR_ID = "SENSOR_ID";


    @Override
    public void run(Context context) throws IOException, InterruptedException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        String lcPath = context.getConfiguration().get(KEY_LC_PATH);
        String version = context.getConfiguration().get(KEY_VERSION);
        String areaString = context.getConfiguration().get(KEY_AREA_STRING); // <nicename>;<left>;<top>;<right>;<bottom>
        String sensorId = context.getConfiguration().get(KEY_SENSOR_ID);

        ProductSplit inputSplit = (ProductSplit) context.getInputSplit();

        Path inputSplitLocation = inputSplit.getPath();
        LOG.info("Finalising file '" + inputSplitLocation + "'");

        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");

        String baseFilename = createBaseFilename(year, month, version, areaString);

        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        Path tifPath = new Path(outputDir + "/" + baseFilename + ".tif");
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());

        if (fileSystem.exists(tifPath)) {
            LOG.info("File '" + inputSplitLocation + "' already exists, done.");
            return;
        }

        File localL3 = CalvalusProductIO.copyFileToLocal(inputSplitLocation, context.getConfiguration());
        File localLC = CalvalusProductIO.copyFileToLocal(new Path(lcPath), context.getConfiguration());

        Product lcProduct = ProductIO.readProduct(localLC);
        Product result = remap(localL3, baseFilename, sensorId, lcProduct, context);

        LOG.info("Creating metadata...");
        String metadata = createMetadata(year, month, version, areaString);
        try (FileWriter fw = new FileWriter(baseFilename + ".xml")) {
            fw.write(metadata);
        }
        CalvalusLogger.getLogger().info("...done. Writing final product...");

        final ProductWriter geotiffWriter = ProductIO.getProductWriter(BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
        geotiffWriter.writeProductNodes(result, baseFilename + ".tif");
        geotiffWriter.writeBandRasterData(result.getBandAt(0), 0, 0, 0, 0, null, new PrintWriterProgressMonitor(System.out));

        Path xmlPath = new Path(outputDir + "/" + baseFilename + ".xml");
        Path pngPath = new Path(outputDir + "/" + baseFilename + ".png");
        CalvalusLogger.getLogger().info(String.format("...done. Copying final product to %s...", tifPath.getParent().toString()));
        FileSystem fs = tifPath.getFileSystem(context.getConfiguration());
        FileUtil.copy(new File(baseFilename + ".tif"), fs, tifPath, false, context.getConfiguration());
        FileUtil.copy(new File(baseFilename + ".xml"), fs, xmlPath, false, context.getConfiguration());
        CalvalusLogger.getLogger().info("...done");
        CalvalusLogger.getLogger().info("...done. Creating quicklook...");
        Quicklooks.QLConfig qlConfig = new Quicklooks.QLConfig();
        qlConfig.setImageType("png");
        qlConfig.setBandName("JD");
        qlConfig.setSubSamplingX(125);
        qlConfig.setSubSamplingY(125);
        File localCpd = new File("fire-modis-pixel.cpd");
        CalvalusProductIO.copyFileToLocal(new Path("/calvalus/projects/fire/aux/fire-modis-pixel.cpd"), localCpd, context.getConfiguration());
        qlConfig.setCpdURL(localCpd.toURI().toURL().toExternalForm());
        RenderedImage image = QuicklookGenerator.createImage(context, result, qlConfig);
        if (image != null) {
            ImageIO.write(image, "png", new File(baseFilename + ".png"));
            FileUtil.copy(new File(baseFilename + ".png"), fs, pngPath, false, context.getConfiguration());
        }
    }

    static Product remap(File localL3, String baseFilename, String sensorId, Product lcProduct, Progressable context) throws IOException {
        Product source = ProductIO.readProduct(localL3);
        source.setPreferredTileSize(TILE_SIZE, TILE_SIZE);

        Product target = new Product(baseFilename, "fire-cci-pixel-product", source.getSceneRasterWidth(), source.getSceneRasterHeight());
        target.setPreferredTileSize(TILE_SIZE, TILE_SIZE);

        ProductUtils.copyGeoCoding(source, target);

        GPF.getDefaultInstance().getOperatorSpiRegistry().addOperatorSpi(new WatermaskOp.Spi());
        Band landWaterMask = GPF.createProduct("LandWaterMask", new HashMap<>(), source).getBandAt(0);

        Band jdBand = target.addBand("JD", ProductData.TYPE_INT32);
        Band clBand = target.addBand("CL", ProductData.TYPE_INT8);
        Band lcBand = target.addBand("LC", ProductData.TYPE_UINT8);
        Band sensorBand = target.addBand("sensor", ProductData.TYPE_INT8);

        jdBand.setSourceImage(new JdImage(source.getBand("JD"), landWaterMask, lcProduct.getBand("lccs_class")));
        clBand.setSourceImage(new ClImage(source.getBand("CL"), jdBand));
        lcBand.setSourceImage(new LcImage(target, lcProduct, jdBand, context));
        sensorBand.setSourceImage(new SensorImage(source.getBand("JD"), sensorId));

        return target;
    }

    static String createBaseFilename(String year, String month, String version, String areaString) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MODIS-AREA_%s-%s", year, month, areaString.split(";")[0].replace(" ", "_"), version);
    }

    static String createMetadata(String year, String month, String version, String areaString) throws IOException {
        String nicename = areaString.split(";")[0];
        String left = areaString.split(";")[1];
        String top = areaString.split(";")[2];
        String right = areaString.split(";")[3];
        String bottom = areaString.split(";")[4];

        VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.init();
        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("UUID", UUID.randomUUID().toString());
        velocityContext.put("date", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(Instant.now()));
        velocityContext.put("zoneId", nicename);
        velocityContext.put("zoneName", nicename);
        velocityContext.put("creationDate", DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(Year.of(2017).atMonth(2).atDay(8)));
        velocityContext.put("westLon", Integer.parseInt(left) - 180);
        velocityContext.put("eastLon", Integer.parseInt(right) - 180);
        velocityContext.put("northLat", 90 - Integer.parseInt(top));
        velocityContext.put("southLat", 90 - Integer.parseInt(bottom));
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

    private static class JdImage extends SingleBandedOpImage {

        private final Band sourceJdBand;
        private final Band watermask;
        private final Band lcBand;

        JdImage(Band sourceJdBand, Band watermask, Band lcBand) {
            super(DataBuffer.TYPE_INT, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
            this.sourceJdBand = sourceJdBand;
            this.watermask = watermask;
            this.lcBand = lcBand;
        }

        @Override
        protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
            float[] sourceJdArray = new float[destRect.width * destRect.height];
            byte[] lcArray = new byte[destRect.width * destRect.height];
            byte[] watermaskArray = new byte[destRect.width * destRect.height];
            try {
                lcBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Byte(lcArray));
                sourceJdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(sourceJdArray));
                watermask.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Byte(watermaskArray));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            int pixelIndex = 0;
            PixelPos pixelPos = new PixelPos();
            for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
                for (int x = destRect.x; x < destRect.x + destRect.width; x++) {
                    pixelPos.x = x;
                    pixelPos.y = y;
                    byte watermask = watermaskArray[pixelIndex];

                    if (watermask > 0) {
                        dest.setSample(x, y, 0, -2);
                        pixelIndex++;
                        continue;
                    }

                    float sourceJd = sourceJdArray[pixelIndex];
                    if (Float.isNaN(sourceJd)) {
                        boolean originalIsBurnable = LcRemapping.isInBurnableLcClass(lcArray[pixelIndex]);
                        sourceJd = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, true, originalIsBurnable, pixelIndex, destRect.width).neighbourValue;
                    }

                    int targetJd;
                    if (sourceJd < 900) {
                        targetJd = (int) sourceJd;
                    } else {
                        targetJd = -1;
                    }

                    dest.setSample(x, y, 0, targetJd);

                    pixelIndex++;
                }
            }
        }
    }


    private static class ClImage extends SingleBandedOpImage {

        private final Band sourceClBand;
        private final Band sourceJdBand;

        private ClImage(Band sourceClBand, Band sourceJdBand) {
            super(DataBuffer.TYPE_BYTE, sourceClBand.getRasterWidth(), sourceClBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
            this.sourceClBand = sourceClBand;
            this.sourceJdBand = sourceJdBand;
        }

        @Override
        protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
            float[] sourceClArray = new float[destRect.width * destRect.height];
            int[] sourceJdArray = new int[destRect.width * destRect.height];
            try {
                sourceClBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(sourceClArray));
                sourceJdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Int(sourceJdArray));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            int pixelIndex = 0;
            for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
                for (int x = destRect.x; x < destRect.x + destRect.width; x++) {

                    int targetCl;
                    float sourceCl = sourceClArray[pixelIndex];
                    if (Float.isNaN(sourceCl)) {
                        NeighbourResult neighbourResult = findNeighbourValue(sourceClArray, false, false, pixelIndex, destRect.width);
                        if (neighbourResult.newPixelIndex != -1 && sourceJdArray[neighbourResult.newPixelIndex] >= 0 && sourceJdArray[neighbourResult.newPixelIndex] < 900) {
                            targetCl = (int) (neighbourResult.neighbourValue);
                        } else {
                            targetCl = 0;
                        }
                    } else {
                        int jdValue = sourceJdArray[pixelIndex];
                        if (jdValue >= 0 && jdValue < 900) {
                            targetCl = (int) (sourceCl);
                        } else {
                            targetCl = 0;
                        }
                    }

                    dest.setSample(x, y, 0, targetCl);
                    pixelIndex++;
                }
            }
        }
    }

    private static class LcImage extends SingleBandedOpImage {

        private final Product lcProduct;
        private final Band jdBand;
        private final Progressable context;

        private LcImage(Product target, Product lcProduct, Band jdBand, Progressable context) {
            super(DataBuffer.TYPE_BYTE, target.getSceneRasterWidth(), target.getSceneRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
            this.lcProduct = lcProduct;
            this.jdBand = jdBand;
            this.context = context;
        }

        @Override
        protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
            context.progress();
            int[] jdArray = new int[destRect.width * destRect.height];
            try {
                jdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Int(jdArray));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            int[] lcData = new int[destRect.width * destRect.height];
            try {
                lcProduct.getBand("lccs_class").readPixels(destRect.x, destRect.y, destRect.width, destRect.height, lcData);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            int pixelIndex = 0;
            for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
                for (int x = destRect.x; x < destRect.x + destRect.width; x++) {
                    lcData[pixelIndex] = LcRemapping.remap(lcData[pixelIndex]);
                    if (lcData[pixelIndex] == LcRemapping.INVALID_LC_CLASS) {
                        lcData[pixelIndex] = 0;
                    }
                    int jdValue = jdArray[pixelIndex];
                    if (jdValue <= 0 || jdValue >= 997) {
                        lcData[pixelIndex] = 0;
                    }
                    dest.setSample(x, y, 0, lcData[pixelIndex]);
                    pixelIndex++;
                }
            }
        }
    }

    private static class SensorImage extends SingleBandedOpImage implements RenderedImage {
        private final Band jd;
        private final int sensorId;

        SensorImage(Band jd, String sensorId) {
            super(DataBuffer.TYPE_BYTE, jd.getRasterWidth(), jd.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
            this.jd = jd;
            this.sensorId = Integer.parseInt(sensorId);
        }

        @Override
        protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
            ProductData.Float jdData = new ProductData.Float(destRect.width * destRect.height);
            try {
                jd.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, jdData);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            for (int i = 0; i < jdData.getNumElems(); i++) {
                int jdValue = jdData.getElemIntAt(i);
                if (jdValue > 0 && jdValue < 900) {
                    dest.setSample(destRect.x + i % destRect.width, destRect.y + i / destRect.width, 0, sensorId);
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
            "                        <gco:CharacterString>Fire_cci SFD Burned Area product v1.0 – Zone ${zoneId}" +
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
            "                                <gco:Date>2017-02-25</gco:Date>" +
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
            "                                <gco:CharacterString></gco:CharacterString>" +
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
            "${Indicative sensor}[-${Additional Segregator}]-fv${xx.x}.tif." +
            "${Indicative Date} is the identifying date for this data set. Format is YYYY[MM[DD]], where YYYY is the " +
            "four digit year, MM is the two digit month from 01 to 12 and DD is the two digit day of the month from 01 to " +
            "31. For monthly products the date will be set to 01. " +
            "${Indicative sensor} is MSI. ${Additional Segregator} " +
            "is the AREA_${TILE_CODE} being the tile code described in the Product User Guide. " +
            "${File Version} is the File version number in the form n{1,}[.n{1,}] (That is 1 or more digits followed by optional " +
            ". and another 1 or more digits.). An example is: " +
            "20050301-ESACCI-L3S_FIRE-BA-MSI-AREA_h38v16-${REPLACE_WITH_VERSION}.tif.]]#" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>For further information on the product, please consult the Product User Guide: Fire_cci_D3.3_PUG_SFD available at: www.esa-fire-cci.org/documents" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 1: Date of the first detection; Pixel Spacing = 0.00017966 deg (approx. 20m); Pixel " +
            "value = Day of the year, from 1 to 365 (or 366) A value of 0 is included when the pixel is not burned in " +
            "the month; a value of -1 is allocated to pixels that are not observed in the month; a value of -2 is allocated to pixels " +
            "that are not taken into account in " +
            "the burned area processing (continuous water, ocean). Data type = Integer; Number of layers = 1; Data depth = 16" +
            "</gco:CharacterString>" +
            "<gco:CharacterString>Layer 2: Confidence Level; Pixel Spacing = 0.00017966 deg (approx. 20m); Pixel value = 0 to " +
            "100, where the value is the probability in percentage that the pixel is actually burned, as a result " +
            "of both the pre-processing and the actual burned area classification. The higher the value, the " +
            "higher the confidence that the pixel is actually burned. A value of 0 is allocated to pixels that are " +
            "not burned, or not observed in the month, or not taken into account in the burned area processing (continuous water, ocean). Data type = Byte; " +
            "Number of layers = 1;" +
            "Data depth = 8" +
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
            "Pixel Spacing = 0.00017966 deg (approx. 20m); Data type = Byte; Number of layers = 1; Data depth = 8" +
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
            "                                        <gmd:URL>http://esa-fire-cci.org/</gmd:URL>" +
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
            "                        <gco:Distance uom=\"degrees\">0.00017966</gco:Distance>" +
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

    static NeighbourResult findNeighbourValue(float[] sourceData, boolean checkForBurnable, boolean originalIsBurnable, int pixelIndex, int width) {
        int[] xDirections = new int[]{-1, 0, 1};
        int[] yDirections = new int[]{-1, 0, 1};

        for (int yDirection : yDirections) {
            for (int xDirection : xDirections) {
                int newPixelIndex = pixelIndex + yDirection * width + xDirection;
                if (newPixelIndex < sourceData.length) {
                    if (newPixelIndex < 0 || newPixelIndex >= sourceData.length) {
                        continue;
                    }
                    float neighbourValue = sourceData[newPixelIndex];
                    if (!Float.isNaN(neighbourValue)) {
                        if (checkForBurnable) {
                            // JD case: if neighbour > 0 but original is unburnable: 0
                            if (neighbourValue > 0 && !originalIsBurnable) {
                                return new NeighbourResult(newPixelIndex, 0);
                            } else {
                                return new NeighbourResult(newPixelIndex, neighbourValue);
                            }
                        } else {
                            // CL case: never mind
                            return new NeighbourResult(newPixelIndex, neighbourValue);
                        }
                    }
                }
            }
        }

        return new NeighbourResult(-1, Float.NaN);
    }

    static class NeighbourResult {

        public NeighbourResult(int newPixelIndex, float neighbourValue) {
            this.newPixelIndex = newPixelIndex;
            this.neighbourValue = neighbourValue;
        }

        int newPixelIndex;
        float neighbourValue;

    }
}
