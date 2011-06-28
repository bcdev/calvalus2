package com.bc.calvalus.portal.client.binding;

import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.ui.HasValue;
import com.google.gwt.user.client.ui.TextBox;

/**
 * @author Norman Fomferra
 */
public class BindingContextTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testWithWidget() {
        TextBox widget = new TextBox();
        DefaultModel model = new DefaultModel();

        BindingContext binding = new BindingContext(model);

        binding.bind("x", widget);

        model.setValue("x", "A");
        assertEquals("A", widget.getValue());

        widget.setValue("B", true);
        assertEquals("B", model.getValue("x"));

        binding.unbind("x");

        model.setValue("x", "U");
        assertEquals("B", widget.getValue());

        widget.setValue("V", true);
        assertEquals("U", model.getValue("x"));
    }

}
