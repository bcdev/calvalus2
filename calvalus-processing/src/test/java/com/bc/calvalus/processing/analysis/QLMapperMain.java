package com.bc.calvalus.processing.analysis;


import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.beam.StreamingProductPlugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.io.FileUtils;

import javax.imageio.ImageIO;
import javax.media.jai.PlanarImage;
import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class QLMapperMain {

    private static final String TEST_DATA = "/eodata/MER_FSG_1PNUPA20080601_for_QL.dim";
    private static final String CHL_CPD = "/eodata/Freshmon_CHL_BC_40.cpd";
    private static final String FRESHMON_LOGO = "/eodata/Freshmon_logo.png";

    public static void main(String[] args) throws Exception {
        System.setProperty("com.sun.media.jai.disableMediaLib", "true");  // disable native libraries for JAI
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product product;
        final Quicklooks.QLConfig qlConfig;
        Configuration configuration;
        if (args.length == 2) {
            Quicklooks quicklooks = Quicklooks.fromXml(FileUtils.readText(new File(args[0])));
            qlConfig = quicklooks.getConfigs()[0];
            configuration = new Configuration();
            product = openProduct(args[1], configuration);
            final String[] bandNames = product.getBandNames();
            System.out.println("bandNames = " + Arrays.toString(bandNames));
        } else {
            qlConfig = new Quicklooks.QLConfig();
            qlConfig.setBandName("CHL");
            qlConfig.setImageType("png");
            qlConfig.setBackgroundColor(new Color(255, 255, 255, 255));
            qlConfig.setCpdURL(QLMapper.class.getResource(CHL_CPD).toExternalForm());
            qlConfig.setMaskOverlays(new String[]{"l1p_cc_land", "l1p_cc_cloud", "l1p_cc_coastline"});
            qlConfig.setLegendEnabled(true);
            qlConfig.setOverlayURL(QLMapper.class.getResource(FRESHMON_LOGO).toExternalForm());

            configuration = new Configuration();
            configuration.set(JobConfigNames.CALVALUS_PROJECT_NAME, "FRESHMON");

            Product inputProduct;
            if (args.length == 1) {
                inputProduct = openProduct(args[0], configuration);
            } else {
                URI productUri = QLMapper.class.getResource(TEST_DATA).toURI();
                inputProduct = ProductIO.readProduct(new File(productUri));
            }
            product = doReprojection(inputProduct);
            Map<String, Object> spatialSubsetParameter = createSpatialSubsetParameter();
            if (!spatialSubsetParameter.isEmpty()) {
                product = GPF.createProduct("Subset", spatialSubsetParameter, product);
            }
            ProcessorAdapter.copySceneRasterStartAndStopTime(inputProduct, product, null);
        }

        TaskAttemptContext context = createrContext(configuration);
        RenderedImage image = QuicklookGenerator.createImage(context, product, qlConfig);
        if (image == null) {
            throw new IllegalArgumentException("Failed to generate image from config");
        }

        try (OutputStream outputStream = new FileOutputStream(qlConfig.getBandName() + "." + qlConfig.getImageType())) {
            ImageIO.write(image, qlConfig.getImageType(), outputStream);
        }

        JFrame jFrame = new JFrame();
        final JLabel positionLabel = new JLabel();
        jFrame.addMouseMotionListener(new MouseMotionListener() {
            @Override
            public void mouseDragged(MouseEvent e) {
            }

            @Override
            public void mouseMoved(MouseEvent e) {
                positionLabel.setText(String.format("[%d, %d]", e.getX(), e.getY()));
            }
        });
        JPanel contentPane = new JPanel(new BorderLayout());
        contentPane.add(new JLabel(new ImageIcon(PlanarImage.wrapRenderedImage(image).getAsBufferedImage())),
                        BorderLayout.CENTER);
        contentPane.add(positionLabel, BorderLayout.SOUTH);
        jFrame.setContentPane(contentPane);
        jFrame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        jFrame.setExtendedState(Frame.MAXIMIZED_BOTH);
        jFrame.setVisible(true);
    }

    private static Product openProduct(String pathString, Configuration configuration) throws IOException {
        if (pathString.endsWith("seq")) {
            return CalvalusProductIO.readProduct(new Path(pathString), configuration, StreamingProductPlugin.FORMAT_NAME);
        } else {
            return ProductIO.readProduct(pathString);
        }
    }

    private static Map<String, Object> createSpatialSubsetParameter() {
        Map<String, Object> subsetParams = new HashMap<>();
        subsetParams.put("geoRegion", "POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))");
        return subsetParams;
    }

    private static Product doReprojection(Product product) {
        Map<String, Object> reprojParams = new HashMap<>();
        reprojParams.put("crs", "EPSG:32633");
        reprojParams.put("noDataValue", 0.0);
        return GPF.createProduct("Reproject", reprojParams, product);
    }


    private static TaskAttemptContext createrContext(Configuration configuration) throws IOException, InterruptedException {
        return new TaskAttemptContextImpl(configuration, new TaskAttemptID());
    }
}
