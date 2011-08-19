package com.bc.calvalus.processing.xml;

/**
 * An object that can be converted to XML.
 * <p/>
 * Classes implementing this interface usually also provide a static method that
 * generates instances of that class from XML:
 * <pre>
 *     public static &lt;T&gt; fromXml(String xml) { ... }
 * </pre>
 * @author Norman
 */
public interface XmlConvertible {
    String toXml();
}
