package com.bc.calvalus.processing.beam;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Map;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ZarrZmetadataReaderPluginTest extends TestCase {
    private static final TypeReference<Map<String, Object>> VALUE_TYPE_REF =
                new TypeReference<Map<String, Object>>() {};
    @Test
    public void testFindShape() throws Exception {
        final ZarrZmetadataReaderPlugin.ZarrZmetadataReader zarrZmetadataReader =
                new ZarrZmetadataReaderPlugin.ZarrZmetadataReader(null);
        final String zmetadatastring =
                "{" +
                "  \"metadata\" : {" +
                "    \".zattrs\" : {" +
                "      \"Conventions\" : \"CF-1.4\"," +
                "      \"coordinates\" : \"lat_bnds lon_bnds time_bnds\"," +
                "      \"TileSize\" : \"1024:1024\"" +
                "    }," +
                "    \".zgroup\" : {" +
                "      \"zarr_format\" : 2" +
                "    }," +
                "    \"chl_q1_mean/.zarray\" : {" +
                "      \"shape\" : [ 3, 24977, 19748 ]," +
                "      \"chunks\" : [ 1, 1024, 1024 ]," +
                "      \"fill_value\" : \"NaN\"," +
                "      \"dtype\" : \">f4\"," +
                "      \"filters\" : null," +
                "      \"dimension_separator\" : \".\"," +
                "      \"compressor\" : {" +
                "        \"level\" : 1," +
                "        \"id\" : \"zlib\"" +
                "      }," +
                "      \"order\" : \"C\"," +
                "      \"zarr_format\" : 2" +
                "    }" +
                "  }" +
                "}";
        final ObjectMapper jsonParser = new ObjectMapper();
        jsonParser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        Map<String, Object> zmetadata = jsonParser.readValue(zmetadatastring, VALUE_TYPE_REF);

        final int[] shape = zarrZmetadataReader.findShape(zmetadata, null);

        assertEquals(3, shape[0]);
    }

}