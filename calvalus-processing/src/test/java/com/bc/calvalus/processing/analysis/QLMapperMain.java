package com.bc.calvalus.processing.analysis;


import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import javax.media.jai.PlanarImage;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.WindowConstants;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Frame;
import java.awt.image.RenderedImage;
import java.io.File;
import java.net.URI;

public class QLMapperMain {

    private static final String TEST_DATA = "/eodata/MER_FSG_1PNUPA20080601_for_QL.dim";
    private static final String CHL_CPD = "/eodata/Freshmon_CHL_BC_40.cpd";
    private static final String FRESHMON_LOGO = "/eodata/Freshmon_logo.png";

    public static void main(String[] args) throws Exception {
        URI productUri = QLMapper.class.getResource(TEST_DATA).toURI();
        Product product = ProductIO.readProduct(new File(productUri));

        Quicklooks.QLConfig qlConfig = new Quicklooks.QLConfig();
        qlConfig.setBandName("CHL");
        qlConfig.setImageType("png");
        qlConfig.setBackgroundColor(new Color(16, 16, 16, 255));
        qlConfig.setCpdURL(QLMapper.class.getResource(CHL_CPD).toExternalForm());
        qlConfig.setMaskOverlays(new String[]{"l1p_cc_land", "l1p_cc_coastline", "l1p_cc_cloud"});
        qlConfig.setLegendEnabled(true);
        qlConfig.setOverlayURL(QLMapper.class.getResource(FRESHMON_LOGO).toExternalForm());
        RenderedImage image = QLMapper.createImage(product, qlConfig);


        JFrame jFrame = new JFrame();
        JPanel contentPane = new JPanel(new BorderLayout());
        contentPane.add(new JLabel(new ImageIcon(PlanarImage.wrapRenderedImage(image).getAsBufferedImage())));
        jFrame.setContentPane(contentPane);
        jFrame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        jFrame.setExtendedState(Frame.MAXIMIZED_BOTH);
        jFrame.setVisible(true);


    }


}
