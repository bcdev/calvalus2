package com.bc.calvalus.client.map;

import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.overlay.Marker;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;

import java.util.ArrayList;
import java.util.List;

public class WKTParser {
    private final String wkt;
    private int pos;

    public WKTParser(String wkt) {
        this.wkt = wkt;
    }

    public static Overlay parse(String wkt) {
        WKTParser parser = new WKTParser(wkt);
        return parser.parseOverlay();
    }

    private Overlay parseOverlay() {
        int pos0 = pos;
        if (eat("POLYGON")) {
            ArrayList<LatLng> points = new ArrayList<LatLng>();
            force('(');
            force(points);
            force(')');
            return new Polygon(points.toArray(new LatLng[points.size()]));
        } else if (eat("POINT")) {
            double[] point = new double[2];
            force('(');
            force(point, 0);
            force(point, 1);
            force(')');
            return new Marker(LatLng.newInstance(point[1], point[0]));
        } else {
            pos = pos0;
            fail("'POINT' or 'POLYGON' expected");
            return null;
        }
    }

    private void force(String token) {
        if (!eat(token)) {
            fail("'" + token + "' expected");
        }
    }

    private void force(char token) {
        if (!eat(token)) {
            fail("'" + token + "' expected");
        }
    }

    private void force(double[] token, int i) {
        if (!eat(token, i)) {
            fail("Number expected");
        }
    }

    private void fail(String msg) {
        throw new IllegalArgumentException("Malformed WKT: " + msg);
    }

    private boolean eat(String token) {
        int pos0 = pos;
        eatWhite();
        if (canEatMore()
                && wkt.substring(pos, pos + token.length()).equalsIgnoreCase(token)) {
            pos += token.length();
            return true;
        }
        pos = pos0;
        return false;
    }

    private boolean eat(char token) {
        int pos0 = pos;
        eatWhite();
        if (canEatMore() && wkt.charAt(pos) == token) {
            pos++;
            return true;
        }
        pos = pos0;
        return false;
    }

    private boolean eat(double[] token, int i) {
        int pos0 = pos;
        eatWhite();
        int pos1 = pos;
        int pos2 = pos;
        while (canEatMore(pos2)
                && "-+.0123456789eE".indexOf(wkt.charAt(pos2)) >= 0) {
            pos2++;
        }
        try {
            token[i] = Double.parseDouble(wkt.substring(pos1, pos2));
            pos = pos2;
            return true;
        } catch (NumberFormatException e) {
            pos = pos0;
            return false;
        }
    }

    private void force(List<LatLng> points) {
        if (!eat(points)) {
            fail("Coordinate list expected");
        }
    }

    private boolean eat(List<LatLng> points) {
        int pos0 = pos;
        if (eat('(')) {
            double[] coord = new double[2];
            while (canEatMore()) {
                force(coord, 1);
                force(coord, 0);
                points.add(LatLng.newInstance(coord[0], coord[1]));
                if (eat(')')) {
                    return true;
                } else if (!eat(',')) {
                    fail("',' or ')' expected");
                    return false;
                }
            }
        }
        pos = pos0;
        points.clear();
        return false;
    }

    private void eatWhite() {

        while (canEatMore()
                && isWhitespace(wkt.charAt(pos))) {
            pos++;
        }
    }

    private boolean isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\b' || c == '\n';
    }

    private boolean canEatMore() {
        return canEatMore(pos);
    }

    private boolean canEatMore(int pos) {
        return pos < wkt.length();
    }
}
