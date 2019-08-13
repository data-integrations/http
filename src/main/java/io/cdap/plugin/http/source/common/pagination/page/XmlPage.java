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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.nerdforge.unxml.Parsing;
import com.nerdforge.unxml.factory.ParsingFactory;
import com.nerdforge.unxml.parsers.Parser;
import com.nerdforge.unxml.parsers.builders.ObjectNodeParserBuilder;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpResponse;
import org.w3c.dom.Document;

import java.util.Iterator;
import java.util.Map;
import javax.xml.xpath.XPathConstants;

/**
 * Returns sub elements which are specified by XPath, one by one.
 * If primitive is specified by XPath only inner value is returned.
 * If non-primitive text representation of xml for that XPath is returned.
 */
class XmlPage extends BasePage {
  private final Map<String, String> fieldsMapping;
  private final Iterator<JsonElement> iterator;
  private final Document document;
  private final Schema schema;
  private final BaseHttpSourceConfig config;

  XmlPage(BaseHttpSourceConfig config, HttpResponse httpResponse) {
    super(httpResponse);
    this.config = config;
    this.fieldsMapping = config.getFullFieldsMapping();
    this.document = XmlUtil.createXmlDocument(httpResponse.getBody());
    this.iterator = getDocumentElementsIterator();
    this.schema = config.getSchema();
  }

  @Override
  public boolean hasNext() {
    return this.iterator.hasNext();
  }

  @Override
  public PageEntry next() {
    String nodeString = this.iterator.next().getAsJsonObject().toString();
    try {
      StructuredRecord record = StructuredRecordStringConverter.fromJsonString(nodeString, schema);
      return new PageEntry(record);
    } catch (Throwable e) {
      return new PageEntry(InvalidEntryCreator.buildStringError(nodeString, e), config.getErrorHandling());
    }
  }

  /**
   * Get primitive element by XPath from document. If not found returns null.
   * If element is not a primitive (xml node with children) exception is thrown.
   *
   * @param path XPath
   * @return a primitive found by XPath
   */
  @Override
  public String getPrimitiveByPath(String path) {
    return (String) XmlUtil.getByXPath(document, path, XPathConstants.STRING);
  }

  /**
   * 1. Converts xml to a structure which is defined by "Fields Mapping" configuration. This is done using unxml.
   * 2. The result entity is a json array.
   * 3. An iterator for elements of json array is returned.
   *
   * @return an iterator for elements of result json array.
   */
  private Iterator<JsonElement> getDocumentElementsIterator() {
    Parsing parsing = ParsingFactory.getInstance().create();
    ObjectNodeParserBuilder obj = parsing.obj();

    for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
      String schemaFieldName = entry.getKey();
      String fieldPath = entry.getValue();

      obj = obj.attribute(schemaFieldName, fieldPath, XmlUtil.xmlTextNodeParser());
    }

    Parser<ArrayNode> parser = parsing.arr(config.getResultPath(), obj).build();
    ArrayNode node = parser.apply(document);
    JsonArray jsonArray = JSONUtil.toJsonArray(node.toString());
    return jsonArray.iterator();
  }

  @Override
  public void close() {

  }
}
