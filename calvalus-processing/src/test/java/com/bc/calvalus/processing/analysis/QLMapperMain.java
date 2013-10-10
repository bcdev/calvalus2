package com.bc.calvalus.processing.analysis;


import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;

import javax.media.jai.PlanarImage;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.WindowConstants;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Frame;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class QLMapperMain {

    private static final String TEST_DATA = "/eodata/MER_FSG_1PNUPA20080601_for_QL.dim";
    private static final String CHL_CPD = "/eodata/Freshmon_CHL_BC_40.cpd";
    private static final String FRESHMON_LOGO = "/eodata/Freshmon_logo.png";

    public static void main(String[] args) throws Exception {
        System.setProperty("com.sun.media.jai.disableMediaLib", "true");  // disable native libraries for JAI
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Quicklooks.QLConfig qlConfig = new Quicklooks.QLConfig();
        qlConfig.setBandName("CHL");
        qlConfig.setImageType("png");
        qlConfig.setBackgroundColor(new Color(255, 255, 255, 255));
        qlConfig.setCpdURL(QLMapper.class.getResource(CHL_CPD).toExternalForm());
        qlConfig.setMaskOverlays(new String[]{"l1p_cc_land", "l1p_cc_cloud", "l1p_cc_coastline"});
        qlConfig.setLegendEnabled(true);
        qlConfig.setOverlayURL(QLMapper.class.getResource(FRESHMON_LOGO).toExternalForm());

        Configuration configuration = new Configuration();
        configuration.set(JobConfigNames.CALVALUS_PROJECT_NAME, "FRESHMON");
        final Mapper.Context context = createMapperContext(configuration);

        Product inputProduct;
        final String pathString;
        if (args.length >= 1) {
            pathString = args[0];
            if (pathString.endsWith("seq")) {
                final StreamingProductReader reader = new StreamingProductReader(new Path(pathString), configuration);
                inputProduct = reader.readProductNodes(null, null);
            } else {
                inputProduct = ProductIO.readProduct(pathString);
            }
        } else {
            URI productUri = QLMapper.class.getResource(TEST_DATA).toURI();
            inputProduct = ProductIO.readProduct(new File(productUri));
        }

        Product product = doReprojection(inputProduct);
        Map<String, Object> spatialSubsetParameter = createSpatialSubsetParameter();
        if (!spatialSubsetParameter.isEmpty()) {
            product = GPF.createProduct("Subset", spatialSubsetParameter, product);
        }
        ProcessorAdapter.copySceneRasterStartAndStopTime(inputProduct, product, null);

        RenderedImage image = QLMapper.createImage(context, product, qlConfig);


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

    private static Map<String, Object> createSpatialSubsetParameter() {
        Map<String, Object> subsetParams = new HashMap<String, Object>();
        subsetParams.put("geoRegion", "POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))");
        return subsetParams;
    }

    private static Product doReprojection(Product product) {
        Map<String, Object> reprojParams = new HashMap<String, Object>();
        reprojParams.put("crs", "EPSG:32633");
        reprojParams.put("noDataValue", 0.0);
        return GPF.createProduct("Reproject", reprojParams, product);
    }


    private static Mapper.Context createMapperContext(Configuration configuration) throws IOException, InterruptedException {
        final RecordWriter recordWriter = new RecordWriter() {
            @Override
            public void write(Object o, Object o2) throws IOException, InterruptedException {
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            }
        };
        final OutputCommitter outputCommitter = new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {
            }

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
            }

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
            }
        };
        final StatusReporter statusReporter = new StatusReporter() {
            @Override
            public Counter getCounter(Enum<?> anEnum) {
                return null;
            }

            @Override
            public Counter getCounter(String s, String s2) {
                return null;
            }

            @Override
            public void progress() {
            }

            @Override
            public void setStatus(String s) {
            }
        };
        final InputSplit inputSplit = new InputSplit() {
            @Override
            public long getLength() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public String[] getLocations() throws IOException, InterruptedException {
                return new String[0];
            }
        };
        final WrappedMapper mapper = new WrappedMapper();
        return mapper.new Context(new MapContextImpl(configuration, new TaskAttemptID(), new NoRecordReader(), recordWriter, outputCommitter,
                                                     statusReporter, inputSplit));
    }


}
