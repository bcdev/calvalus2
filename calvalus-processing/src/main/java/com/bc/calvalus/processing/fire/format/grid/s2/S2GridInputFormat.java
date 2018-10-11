package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.PixelProductArea;
import com.bc.calvalus.processing.fire.format.S2Strategy;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author thomas
 */
public class S2GridInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Properties tiles = new Properties();
        tiles.load(getClass().getResourceAsStream("areas-tiles-2deg.properties"));
        List<InputSplit> splits = new ArrayList<>(1000);

        for (Object twoDegTile : tiles.keySet()) {

            String tile = twoDegTile.toString();
            int tileX = Integer.parseInt(tile.split("y")[0].substring(1));
            int tileY = Integer.parseInt(tile.split("y")[1]);
            for (int k = 0; k < 4; k++) {
                String oneDegTile;
                switch (k) {
                    case 0:
                        oneDegTile = twoDegTile.toString();
                        break;
                    case 1:
                        oneDegTile = "x" + (tileX + 1) + "y" + tileY;
                        break;
                    case 2:
                        oneDegTile = "x" + (tileX + 1) + "y" + (tileY - 1);
                        break;
                    case 3:
                        oneDegTile = "x" + tileX + "y" + (tileY - 1);
                        break;
                    default:
                        throw new IllegalStateException("Will never come here.");
                }

                S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea[] areas = getAreas(oneDegTile);
                addSplit(areas, splits, oneDegTile);
            }
        }
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    static S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea[] getAreas(Object oneDegTile) {
        String tile = oneDegTile.toString();
        int tileX = Integer.parseInt(tile.split("y")[0].substring(1));
        int tileY = Integer.parseInt(tile.split("y")[1]);

        PixelProductArea[] allAreas = new S2Strategy().getAllAreas();
        List<PixelProductArea> areas = new ArrayList<>();

        for (PixelProductArea area : allAreas) {
            Rectangle refRectangle = new Rectangle(area.left, area.bottom, 5, 5);
            Rectangle rectangle = new Rectangle(tileX, tileY, 1, 1);
            if (rectangle.intersects(refRectangle)) {
                areas.add(area);
            }
        }

        S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea[] result = new S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea[areas.size()];
        for (int i = 0; i < areas.size(); i++) {
            PixelProductArea area = areas.get(i);
            result[i] = S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea.valueOf(area.nicename);
        }
        return result;
    }

    private void addSplit(S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea[] areas, List<InputSplit> splits, String oneDegreeTile) {
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        for (S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea area : areas) {
            filePaths.add(new Path(area.name()));
            fileLengths.add(0L);
        }
        filePaths.add(new Path(oneDegreeTile));
        fileLengths.add(0L);
        CombineFileSplit combineFileSplit = new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
        splits.add(combineFileSplit);
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NoRecordReader();
    }

}
