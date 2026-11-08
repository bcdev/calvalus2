package com.bc.calvalus.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.*;
import static org.junit.Assert.assertEquals;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class ProductionTypeServiceTest {

    @Test
    public void testJsonComment() {
        Pattern p = Pattern.compile("(.*)/[*].*[*]/(.*)");
        Matcher m = p.matcher("123/*456*/789");
        assertEquals(m.matches(), true);
        assertEquals(m.group(1), "123");
        assertEquals(m.group(2), "789");
    }

}