package com.bc.calvalus.portal.client.binding;

import com.google.gwt.user.client.ui.HasValue;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public interface Binding<T> {
    String getName();
    HasValue<T> getWidget();

    Binding<T> match(String pattern);
    Binding<T> notEmpty();
    Binding<T> inRange(T v1, T v2);
}
