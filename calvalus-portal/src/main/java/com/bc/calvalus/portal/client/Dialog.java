package com.bc.calvalus.portal.client;


import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

/**
 * A simple dialog component.
 *
 * @author Norman Fomferra
 */
public class Dialog {
    private final String title;
    private final Widget dialogContents;
    private final ButtonType[] buttonTypes;
    private ButtonType selectedButtonType;
    private int selectedButtonIndex;
    private DialogBox dialogBox;

    public enum ButtonType {
        OK("OK"),
        CANCEL("Cancel"),
        YES("Yes"),
        NO("No"),
        CLOSE("Close"),;

        private final String label;

        ButtonType(String label) {
            this.label = label;
        }

        public String getLabel() {
            return label;
        }
    }

    public Dialog(String title, Widget dialogContents, ButtonType... buttonTypes) {
        this.title = title;
        this.dialogContents = dialogContents;
        this.buttonTypes = buttonTypes;
    }

    public static void showMessage(String htmlMessage) {
        new Dialog("Message", new HTML(htmlMessage), ButtonType.CLOSE).show();
    }

    public static void showWarning(String htmlMessage) {
        new Dialog("Warning", new HTML(htmlMessage), ButtonType.CLOSE).show();
    }

    public ButtonType show() {
        if (dialogBox == null) {
            this.dialogBox = createDialogBox();
            dialogBox.setWidget(createMainPanel());
        }
        dialogBox.center();
        dialogBox.show();
        return selectedButtonType;
    }

    public int getSelectedButtonIndex() {
        return selectedButtonIndex;
    }

    public ButtonType getSelectedButtonType() {
        return selectedButtonType;
    }

    private void close(ButtonType buttonType, int buttonIndex) {
        selectedButtonType = buttonType;
        selectedButtonIndex = buttonIndex;
        if (buttonType == ButtonType.OK) {
            onOK();
            onClose();
        } else if (buttonType == ButtonType.CANCEL) {
            onCancel();
            onClose();
        } else if (buttonType == ButtonType.YES) {
            onYes();
            onClose();
        } else if (buttonType == ButtonType.NO) {
            onNo();
            onClose();
        }
        dialogBox.hide();
    }

    protected void onOK() {
    }

    protected void onCancel() {
    }

    protected void onYes() {
    }

    protected void onNo() {
    }

    protected void onClose() {
    }

    protected DialogBox createDialogBox() {
        final DialogBox dialogBox = new DialogBox();
        dialogBox.ensureDebugId("cwDialogBox");
        dialogBox.setText(title);
        dialogBox.setGlassEnabled(true);
        dialogBox.setAnimationEnabled(true);
        return dialogBox;
    }

    private VerticalPanel createMainPanel() {
        VerticalPanel mainPanel = new VerticalPanel();
        mainPanel.setSpacing(4);

        mainPanel.add(dialogContents);
        mainPanel.setCellHorizontalAlignment(dialogContents, HasHorizontalAlignment.ALIGN_CENTER);

        HorizontalPanel buttonRow = createButtonRow();
        mainPanel.add(buttonRow);
        mainPanel.setCellHorizontalAlignment(buttonRow, HasHorizontalAlignment.ALIGN_RIGHT);
        return mainPanel;
    }

    private HorizontalPanel createButtonRow() {
        final HorizontalPanel buttonRow = new HorizontalPanel();
        for (int i = 0; i < buttonTypes.length; i++) {
            final ButtonType buttonType = buttonTypes[i];
            final int buttonIndex = i;
            final Button button = new Button(buttonType.getLabel(),
                                             new ClickHandler() {
                                                 public void onClick(ClickEvent event) {
                                                     close(buttonType, buttonIndex);
                                                 }
                                             });
            buttonRow.add(button);
            buttonRow.setCellHorizontalAlignment(button, HasHorizontalAlignment.ALIGN_RIGHT);
        }
        return buttonRow;
    }

}
