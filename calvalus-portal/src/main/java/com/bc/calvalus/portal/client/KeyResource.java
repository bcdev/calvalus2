package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.resources.client.ClientBundle;
import com.google.gwt.resources.client.TextResource;

interface KeyResource extends ClientBundle {

    KeyResource INSTANCE = GWT.create(KeyResource.class);

        @Source("key.txt")
        TextResource mapsKey();
}
