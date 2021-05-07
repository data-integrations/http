/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpResponse;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Returns elements from json one by one by given json path.
 */
class JsonPage extends BasePage {
  private final String insideElementJsonPathPart;
  private final Iterator<JsonElement> iterator;
  private final JsonElement json;
  private final Map<String, String> fieldsMapping;
  private final Schema schema;
  private final BaseHttpSourceConfig config;
  private final List<String> optionalFields;

  JsonPage(BaseHttpSourceConfig config, HttpResponse httpResponse) {
    super(httpResponse);
    this.config = config;
    this.json = JSONUtil.toJsonElement(httpResponse.getBody());
    this.schema = config.getSchema();
    this.optionalFields = getOptionalFields();

    JsonElement jsonElement = json;
    if (json.isJsonObject()) {
      JSONUtil.JsonQueryResponse queryResponse =
        JSONUtil.getJsonElementByPath(json.getAsJsonObject(), config.getResultPath(), optionalFields);
      this.insideElementJsonPathPart = queryResponse.getUnretrievedPath();
      jsonElement = queryResponse.get();
    } else {
      this.insideElementJsonPathPart = config.getResultPath() == null ? "" : config.getResultPath();
    }

    if (jsonElement.isJsonArray()) {
      this.iterator = jsonElement.getAsJsonArray().iterator();
    } else if (jsonElement.isJsonObject()) {
      this.iterator = Collections.singleton(jsonElement).iterator();
    } else {
      throw new IllegalArgumentException(String.format("Element found by '%s' json path is expected to be an object " +
                                                         "or an array. Primitive found", config.getResultPath()));
    }

    this.fieldsMapping = config.getFullFieldsMapping();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  /**
   * Converts a next element from json into a json object which is defined by fieldsMapping.
   *
   * Example next element:
   *   {
   *      "id":"19124",
   *      "key":"NETTY-13",
   *      "fields":{
   *         "issuetype":{
   *            "self":"https://issues.cask.co/rest/api/2/issuetype/4",
   *            "name":"Improvement",
   *            "subtask":false
   *         },
   *         "fixVersions":[
   *
   *         ],
   *         "description":"Test description for NETTY-13",
   *         "project":{
   *            "id":"10301",
   *            "key":"NETTY",
   *            "name":"Netty-HTTP",
   *            "projectCategory":{
   *               "id":"10002",
   *               "name":"Infrastructure"
   *            }
   *         }
   *      }
   *   }
   *
   * The mapping is:
   *
   * | Field Name      | Field Path                                |
   * | --------------- |:-----------------------------------------:|
   * | type            | /fields/issuetype/name                    |
   * | description     | /fields/description                       |
   * | projectCategory | /fields/project/projectCategory/name      |
   * | isSubtask       | /fields/issuetype/subtask                 |
   * | fixVersions     | /fields/fixVersions                       |
   *
   * The result returned by function is:
   *
   * {
   *    "key":"NETTY-13",
   *    "type":"Improvement",
   *    "isSubtask":false,
   *    "description":"Test description for NETTY-13",
   *    "projectCategory":"Infrastructure",
   *    "fixVersions":[
   *
   *    ]
   * }
   *
   * Note:
   * This also supports "insideElementJsonPath". Example would be the following: if path is
   * '/bookstore/items/bookPublisherDetails'. The array which elements are retrieved from is /bookstore/items
   * while insideElementJsonPath is "bookPublisherDetails". So for each element from "/bookstore/items" only contents
   * of "/bookPublisherDetails" will be parsed. Which is expected to a json object (not primitive or array)
   *
   * @return the result is a string representation of json with the following fields:
   */
  @Override
  public PageEntry next() {
    JsonObject currentJsonObject = iterator.next().getAsJsonObject();

    JsonObject resultJson = new JsonObject();
    int numPartiallyRetrieved = 0;
    for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
      String schemaFieldName = entry.getKey();
      String fieldPath = insideElementJsonPathPart + "/" + StringUtils.stripStart(entry.getValue(), "/");

      JSONUtil.JsonQueryResponse queryResponse =
        JSONUtil.getJsonElementByPath(currentJsonObject, fieldPath, optionalFields);

      if (!queryResponse.isFullyRetrieved()) {
        numPartiallyRetrieved++;
      }

      if (!queryResponse.getRetrievedPath().equals("/")){
        resultJson.add(schemaFieldName, queryResponse.get());
      }
    }

    String jsonString = resultJson.toString();
    try {
      StructuredRecord record = StructuredRecordStringConverter.fromJsonString(jsonString, schema);
      if (numPartiallyRetrieved > 0) {
        InvalidEntry<StructuredRecord> error =
          new InvalidEntry<>(1, "Couldn't find all required fields in the record", record);
        return new PageEntry(error, config.getErrorHandling());
      }
      return new PageEntry(record);
    } catch (Throwable e) {
      return new PageEntry(InvalidEntryCreator.buildStringError(jsonString, e), config.getErrorHandling());
    }
  }

  private List<String> getOptionalFields() {
    List<String> optionalFields = new ArrayList<>();
    List<Schema.Field> allFields = schema.getFields();
    if (allFields == null) {
      return optionalFields;
    }
    for (Schema.Field field : allFields) {
      if (field.getSchema().isNullable()) {
        optionalFields.add(field.getName());
      }
    }
    return optionalFields;
  }

  /**
   * Get primitive from json by json path or return null if not found.
   * If element found is not a primitive (object or array) exception is thrown.
   *
   * @param path a json path. E.g. "/city/schools/students"
   * @return a primitive converted to string
   */
  @Override
  public String getPrimitiveByPath(String path) {
    if (json.isJsonObject()) {
      JSONUtil.JsonQueryResponse queryResponse = JSONUtil.getJsonElementByPath(json.getAsJsonObject(),
                                                                               path, optionalFields);
      if (queryResponse.isFullyRetrieved()) {
        return queryResponse.getAsJsonPrimitive().getAsString();
      }
    }
    return null;
  }

  @Override
  public void close() {

  }
}
