package com.bc.calvalus.api.ogcapiprocesses;

import com.bc.calvalus.ogcapiprocesses.CalvalusToOGCProcessTranslator;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CalvalusToOGCProcessTranslatorTest {
    private static final String COLLECTIONS_PATH = "/schema/oneOf/0/properties/collection/enum";
    private static final ArrayList<String> COLLECTIONS = new ArrayList<>();
    static {
        COLLECTIONS.add("Sentinel-2 L2A");
        COLLECTIONS.add("Sentinel-2 L1C");
        COLLECTIONS.add("Sentinel-3 SYN");
    }

    @Test
    public void testInjectCollections() {
        JsonNode areaTimeCollectionInput = CalvalusToOGCProcessTranslator.areaTimeCollectionInput(COLLECTIONS);
        JsonNode actualCollectionsNode = areaTimeCollectionInput.at(COLLECTIONS_PATH);
        assertTrue(actualCollectionsNode.isArray());
        List<String> actualCollections = new ArrayList<>();
        actualCollectionsNode.forEach(item -> actualCollections.add(item.asText()));
        assertEquals(new HashSet<>(COLLECTIONS), new HashSet<>(actualCollections));
    }

}
