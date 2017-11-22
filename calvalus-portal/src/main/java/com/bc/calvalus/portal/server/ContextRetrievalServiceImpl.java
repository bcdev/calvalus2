package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.ContextRetrievalService;
import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * @author hans
 */
public class ContextRetrievalServiceImpl extends RemoteServiceServlet implements ContextRetrievalService {

    @Override
    public DtoInputSelection getInputSelection() {
        return (DtoInputSelection) getServletContext().getAttribute("catalogueSearch");
    }
}
