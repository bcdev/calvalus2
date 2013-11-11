package com.bc.calvalus.portal.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.HasClickHandlers;
import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.user.client.Window;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marco Peters
 */
public class HelpSystem {

    private static final String WINDOW_NAME = "_CVHelp";
    private static final String HELP_HOME_LINK = "http://www.brockmann-consult.de/beam-wiki/x/W4C8Aw";
    private static Map<String, String> keyMap = new HashMap<String, String>();

    static {
        init();
    }

    private HelpSystem() {

    }

    private static void init() {

        try {
            new RequestBuilder(RequestBuilder.GET, "helpKeyMap.xml").sendRequest("", new RequestCallback() {
                @Override
                public void onResponseReceived(Request req, Response resp) {
                    String text = resp.getText();
                    parseXmlKeyMap(text);
                }

                @Override
                public void onError(Request res, Throwable throwable) {
                    // handle errors
                }
            });
        } catch (RequestException e) {
            e.printStackTrace();
        }
    }

    public static void addClickHandler(HasClickHandlers helpWidget, String url) {
        String link = keyMap.get(url);
        if (link == null) {
            // todo - get this link from helpKeyMap.xml; introduce a header section
            link = HELP_HOME_LINK;
        }
        helpWidget.addClickHandler(new HelpClickHandler(link));
    }

    private native static void parseXmlKeyMap(String xmlKeyMap)/*-{
        if ($wnd.DOMParser) {
            parser = new DOMParser();
            xmlDoc = parser.parseFromString(xmlKeyMap, "text/xml");
        }
        else // Internet Explorer
        {
            xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
            xmlDoc.async = false;
            xmlDoc.loadXML(xmlKeyMap);
        }

        helpMap = xmlDoc.getElementsByTagName("helpMap")[0].children;
        for (i = 0; i < helpMap.length; i++) {
            mapElem = helpMap[i];
            var key = mapElem.nodeName;
            var value = mapElem.childNodes[0].nodeValue;
            @com.bc.calvalus.portal.client.HelpSystem::addToMap(Ljava/lang/String;Ljava/lang/String;)(key, value);
        }
    }-*/;

    private static void addToMap(String key, String value) {
        keyMap.put(key, value);
    }

    private static class HelpClickHandler implements ClickHandler {

        private String url;

        public HelpClickHandler(String url) {
            this.url = url;
        }

        @Override
        public void onClick(ClickEvent event) {
            showHelp(url);
        }

    }

    private static void showHelp(String url) {
        // Description of window.open and its features:
        // https://developer.mozilla.org/en-US/docs/Web/API/window.open?redirectlocale=en-US&redirectslug=DOM%2Fwindow.open
        // The top feature is used to force a new browser window, otherwise the link would be opened in a new tab
        Window.open(url, WINDOW_NAME, "top=10");
    }

}
