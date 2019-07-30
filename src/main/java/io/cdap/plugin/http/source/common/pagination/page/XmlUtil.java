/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.http.source.common.pagination.page;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Charsets;
import com.nerdforge.unxml.parsers.Parser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * Utility functions for working with xml document.
 */
public class XmlUtil {
  private static XPathFactory xPathfactory = XPathFactory.newInstance();

  /**
   * Create xml document instance out of a String.
   *
   * @param xmlString xml in string format
   * @return a Document instance representing input xml
   */
  public static Document createXmlDocument(String xmlString) {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setIgnoringComments(true);

    try {
      InputStream input = new ByteArrayInputStream(xmlString.getBytes(Charsets.UTF_8));
      return factory.newDocumentBuilder().parse(input);
    } catch (ParserConfigurationException | SAXException | IOException e) {
      throw new IllegalStateException("Failed to parse xml document", e);
    }
  }

  /**
   * Get a parser used by unxml to parse nodes.
   * This parser returns value of node. If only one single node is present.
   * If multiple nodes or node tree is found, it's xml text representation is returned
   *
   * @return unxml parser
   */
  public static Parser<JsonNode> xmlTextNodeParser() {
    return node -> {
      if (node.getChildNodes().getLength() == 1) {
        return new TextNode(node.getTextContent());
      } else {
        return new TextNode(nodeToString(node));
      }
    };
  }

  /**
   * Converts xml node object. Into it's text representation
   *
   * @param node a node object
   * @return xml text representation of object
   */
  public static String nodeToString(Node node) {
    StringWriter sw = new StringWriter();
    try {
      Transformer t = TransformerFactory.newInstance().newTransformer();
      t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      t.setOutputProperty(OutputKeys.INDENT, "yes");
      t.transform(new DOMSource(node), new StreamResult(sw));
    } catch (TransformerException e) {
      throw new IllegalStateException("Failed to parse xml document", e);
    }
    return sw.toString();
  }

  /**
   * Get element of given type from given document by XPath.
   * Throws an exception if element is not of given path.
   * Returns null if element not found
   *
   * @param document document instance
   * @param path xpath string representation
   * @param returnType a type of element expected to be returned
   * @return element found by XPath or null if not found.
   */
  public static Object getByXPath(Document document, String path, QName returnType) {
    XPath xpath = xPathfactory.newXPath();
    try {
      XPathExpression expr = xpath.compile(path);
      return expr.evaluate(document, returnType);
    } catch (XPathExpressionException e) {
      return null;
    }
  }
}
