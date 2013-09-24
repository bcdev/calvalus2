package com.google.gwt.core.client;

/**
 * This class provides a workaround for the GWT Bug 7527 (http://code.google.com/p/google-web-toolkit/issues/detail?id=7527)
 * <p/>
 * The solution is taken from http://alexluca.com/2013/01/17/gwt-25-and-extgwt-224-classnotfoundexception-comgooglegwtcoreclientgwtbridge/
 * <p/>
 * This should be fixed with GWT release 2.5.2, hopefully.
 */
public abstract class GWTBridge extends com.google.gwt.core.shared.GWTBridge {

}