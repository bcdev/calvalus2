/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.analysis;

import com.bc.ceres.glayer.support.ImageLayer;
import com.bc.ceres.glayer.support.ShapeLayer;
import com.bc.ceres.grender.Rendering;
import com.bc.ceres.grender.Viewport;
import com.bc.ceres.grender.support.BufferedImageRendering;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.esa.snap.core.util.io.FileUtils;
import org.geotools.geometry.jts.LiteShape2;

import javax.imageio.ImageIO;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WorldQuickLookGenerator {

    private final Color lineColor;
    private final Color fillColor;
    private final List<Shape> shapeList;

    public WorldQuickLookGenerator() {
        this(Color.BLACK, Color.WHITE);
    }

    public WorldQuickLookGenerator(Color lineColor, Color fillColor) {
        this.lineColor = lineColor;
        this.fillColor = fillColor;
        this.shapeList = new ArrayList<Shape>();
    }

    public void addGeometry(Geometry geometry) {
        try {
            shapeList.add(new LiteShape2(geometry, null, null, true));
        } catch (Exception ignore) {
        }
    }

    public BufferedImage createQuickLookImage(BufferedImage worldImage) {
        final int w = worldImage.getWidth();
        final int h = worldImage.getHeight();
        final AffineTransform s2i = new AffineTransform();
        s2i.translate(0.0, h);
        s2i.scale(1.0, -1.0);
        s2i.scale(w / 360.0, h / 180.0);
        s2i.translate(180.0, 90.0);
        final ShapeLayer shapeLayer = new ShapeLayer(shapeList.toArray(new Shape[shapeList.size()]), s2i) {
            @Override
            protected void renderLayer(Rendering rendering) {
                final Graphics2D g = rendering.getGraphics();
                final Viewport vp = rendering.getViewport();
                final AffineTransform transformSave = g.getTransform();
                try {
                    final AffineTransform transform = new AffineTransform();
                    transform.concatenate(vp.getModelToViewTransform());
                    transform.concatenate(getShapeToModelTransform());
                    g.setTransform(transform);
                    for (Shape shape : getShapeList()) {
                        g.setPaint(fillColor);
                        g.fill(shape);
                        g.setPaint(lineColor);
                        g.setStroke(new BasicStroke(0.5f));
                        g.draw(shape);
                    }
                } finally {
                    g.setTransform(transformSave);
                }
            }
        };
        shapeLayer.setTransparency(0.5);
        final ImageLayer imageLayer = new ImageLayer(worldImage);
        imageLayer.getChildren().add(shapeLayer);

        final BufferedImage quickLookImage = new BufferedImage(w, h, worldImage.getType());
        final Rendering rendering = new BufferedImageRendering(quickLookImage);
        imageLayer.render(rendering);

        return quickLookImage;
    }

    public static void main(String[] args) throws IOException {
        BufferedImage sourceWorldMap = ImageIO.read(GeometryReducer.class.getResourceAsStream("worldMap.png"));

        Color lineColor = Color.WHITE;
        Color fillColor = new Color(255, 255, 255, 150);
        WorldQuickLookGenerator worldGenerator = new WorldQuickLookGenerator(lineColor, fillColor);
        WKTReader wktReader = new WKTReader();
        File inputFile = new File(args[0]);
        String text = FileUtils.readText(inputFile);
        String[] lines = text.split("\n");
        for (String wkt : lines) {
            try {
                worldGenerator.addGeometry(wktReader.read(wkt));
            } catch (ParseException ignore) {
            }
        }
        BufferedImage worldMap = worldGenerator.createQuickLookImage(sourceWorldMap);
        ImageIO.write(worldMap, "png", new File("geometry-worldmap.png"));


    }
}
