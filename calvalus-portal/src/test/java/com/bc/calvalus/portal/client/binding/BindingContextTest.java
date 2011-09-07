package com.bc.calvalus.portal.client.binding;

import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.ui.HasValue;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;

/**
 * @author Norman Fomferra
 */
public class BindingContextTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testWithTextBoxWidget() {
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

    public void testWithListBoxWidget() {
        SelectionBox<Object> widget = new SelectionBox<Object>();
        Object item1 = new Object() {
            @Override
            public String toString() {
                return "A";
            }
        };
        Object item2 = new Object() {
            @Override
            public String toString() {
                return "B";
            }
        };
        widget.addItem(item1.toString(), item1);
        widget.addItem(item1.toString(), item2);

        DefaultModel model = new DefaultModel();

        BindingContext binding = new BindingContext(model);

        binding.bind("x", widget);

        model.setValue("x", item1);
        assertSame(item1, widget.getValue());
        assertSame(item1, model.getValue("x"));

        widget.setValue(item2, true);
        assertSame(item2, widget.getValue());
        assertSame(item2, model.getValue("x"));

        binding.unbind("x");

        model.setValue("x", item1);
        assertSame(item2, widget.getValue());
        assertSame(item1, model.getValue("x"));

        model.setValue("x", item2);
        widget.setValue(item1, true);
        assertSame(item1, widget.getValue());
        assertSame(item2, model.getValue("x"));
    }
}
