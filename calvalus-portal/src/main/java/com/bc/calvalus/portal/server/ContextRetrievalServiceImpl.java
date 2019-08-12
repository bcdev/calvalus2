package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.ContextRetrievalService;
import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * @author hans
 */
public class ContextRetrievalServiceImpl extends RemoteServiceServlet implements ContextRetrievalService {

    private static final String CATALOGUE_SEARCH_PREFIX = "catalogueSearch_";

    @Override
    public DtoInputSelection getInputSelection(String userName) {
        return (DtoInputSelection) getServletContext().getAttribute(CATALOGUE_SEARCH_PREFIX + userName);
    }
}
