package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Focusable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.user.datepicker.client.DateBox;

/**
 * An exception that is thrown if form validation fails.
 *
 * @author Norman
 */
public class ValidationException extends Exception {
    private final Widget widget;

    public ValidationException(Widget widget, String message) {
        super(message);
        this.widget = widget;
    }

    public Widget getWidget() {
        return widget;
    }

    public void handle() {
        Dialog dialog = new Dialog("Calvalus", new HTML(getMessage()), Dialog.ButtonType.CLOSE){
            @Override
            protected void onHide() {
                super.onHide();
                if (getWidget() instanceof Focusable) {
                    ((Focusable) getWidget()).setFocus(true);
                } else if (getWidget() instanceof DateBox) {
                    ((DateBox) getWidget()).setFocus(true);
                }
            }
        };
        dialog.show();
    }
}
