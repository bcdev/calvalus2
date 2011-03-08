/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.beam;

import com.bc.ceres.binding.dom.DomElement;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Note: this class is not thread save, nor may the input node be changed.
 */
class NodeDomElement implements DomElement {
    private final Node node;
    private DomElement parent;
    private ArrayList<NodeDomElement> elementList;

    NodeDomElement(Node node) {
        this.node = node;
    }

    @Override
    public String getName() {
        return node.getNodeName();
    }

    @Override
    public String getValue() {
        return node.getTextContent();
    }

    @Override
    public void setParent(DomElement parent) {
        this.parent = parent;
    }

    @Override
    public int getChildCount() {
        return getElementList().size();
    }

    @Override
    public DomElement getChild(int index) {
        return getElementList().get(index);
    }

    @Override
    public String toXml() {
        return "";
    }

    @Override
    public String getAttribute(String attributeName) {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes == null) {
            return null;
        }
        Node namedItem = attributes.getNamedItem(attributeName);
        if (namedItem == null) {
            return null;
        }
        return namedItem.getNodeValue();
    }

    @Override
    public String[] getAttributeNames() {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes == null) {
            return new String[0];
        }
        String[] attributeNames = new String[attributes.getLength()];
        for (int i = 0; i < attributeNames.length; i++) {
            attributeNames[i] = attributes.item(i).getNodeName();
        }
        return attributeNames;
    }

    @Override
    public DomElement getParent() {
        return parent;
    }

    @Override
    public DomElement getChild(String elementName) {
        List<NodeDomElement> elementList = getElementList();
        for (NodeDomElement nodeDomElement : elementList) {
            if (nodeDomElement.getName().equals(elementName)) {
                return nodeDomElement;
            }
        }
        return null;
    }

    @Override
    public DomElement[] getChildren() {
        List<NodeDomElement> elementList = getElementList();
        return elementList.toArray(new DomElement[elementList.size()]);
    }

    @Override
    public DomElement[] getChildren(String elementName) {
        List<NodeDomElement> elementList = getElementList();
        List<DomElement> children = new ArrayList<DomElement>(elementList.size());
        for (NodeDomElement element : elementList) {
            if (element.getName().equals(elementName)) {
                children.add(element);
            }
        }
        return children.toArray(new DomElement[children.size()]);
    }

    private List<NodeDomElement> getElementList() {
        if (elementList == null) {
            elementList = new ArrayList<NodeDomElement>();
            NodeList childNodes = node.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node childNode = childNodes.item(i);
                short nodeType = childNode.getNodeType();
                if (nodeType == 1) {
                    elementList.add(new NodeDomElement(childNode));
                }
            }
        }
        return elementList;
    }

    @Override
    public void setAttribute(String name, String value) {
        throw new IllegalStateException("NodeDomElement is immutable.");
    }

    @Override
    public DomElement createChild(String name) {
        throw new IllegalStateException("NodeDomElement is immutable.");
    }

    @Override
    public void addChild(DomElement childElement) {
        throw new IllegalStateException("NodeDomElement is immutable.");
    }

    @Override
    public void setValue(String value) {
        throw new IllegalStateException("NodeDomElement is immutable.");
    }

}
