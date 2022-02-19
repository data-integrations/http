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
package io.cdap.plugin.http.common.pagination.page;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Utility functions for working with JSON document.
 */
public class JSONUtil {
  private static final JsonParser JSON_PARSER = new JsonParser();

  public static JsonObject toJsonObject(String text) {
    return JSON_PARSER.parse(text).getAsJsonObject();
  }

  public static JsonElement toJsonElement(String text) {
    return JSON_PARSER.parse(text);
  }

  public static JsonArray toJsonArray(String text) {
    return JSON_PARSER.parse(text).getAsJsonArray();
  }

  /**
   * Find an element by jsonPath in given json object. If element not found, information about the search is returned.
   * Like until which element json path evaluation was successful.
   *
   * @param jsonObject a jsonObject on which the search should be performed
   * @param jsonPath a slash separated path. E.g. "/bookstore/books"
   * @param optionalFields a list of fields that may or may not exist in the response
   * @return an object containing information about search results, success/failure.
   */
  public static JsonQueryResponse getJsonElementByPath(JsonObject jsonObject,  @Nullable String jsonPath,
                                                       List<String> optionalFields) {
    String[] pathParts = {};
    if (jsonPath != null) {
      String stripped = StringUtils.strip(jsonPath.trim(), "/");
      pathParts = stripped.isEmpty() ? new String[0] : stripped.split("/");
    }

    JsonElement currentElement = jsonObject;
    for (int i = 0; i < pathParts.length; i++) {
      String pathPart = pathParts[i];

      if (currentElement.isJsonObject()) {
        jsonObject = currentElement.getAsJsonObject();
      }

      if (!currentElement.isJsonObject() || jsonObject.get(pathPart) == null) {
        return new JsonQueryResponse(
          Arrays.copyOfRange(pathParts, 0, i),
          Arrays.copyOfRange(pathParts, i, pathParts.length),
          optionalFields,
          currentElement
        );
      }

      currentElement = jsonObject.get(pathPart);
    }
    return new JsonQueryResponse(
      Arrays.copyOfRange(pathParts, 0, pathParts.length),
      new String[0],
      optionalFields,
      currentElement
    );
  }

  /**
   * A class which contains information regarding results of searching an element in json by json path.
   */
  public static class JsonQueryResponse {
    private final String[] unretrievedPathParts;
    private final List<String> optionalFields;
    private final String retrievedPath;
    private final String unretrievedPath;
    private final JsonElement result;

    private JsonQueryResponse(String[] retrievedPathParts, String[] unretrievedPathParts,
                              List<String> optionalFields, JsonElement result) {
      this.unretrievedPathParts = unretrievedPathParts;
      this.retrievedPath = "/" + StringUtils.join(retrievedPathParts, '/');
      this.unretrievedPath = "/" + StringUtils.join(unretrievedPathParts, '/');
      this.optionalFields = optionalFields;
      this.result = result;
    }

    /**
     * Assert if an element found is or correct type (e.g. primitive/object/array)
     *
     * @param expectedClass a class representing a type of json element.
     */
    public void assertClass(Class<? extends JsonElement> expectedClass) {
      if (!expectedClass.isInstance(result)) {
        throw new IllegalArgumentException(String.format(
          "Element retrieved by path '%s' expected to be '%s', but found '%s'.\nResult json is: '%s'",
          getRetrievedPath(), expectedClass.getSimpleName(),
          result.getClass().getSimpleName(), result.toString()));
      }
    }

    /**
     * @return true if the json path was fully successfully retrieved till the last element.
     */
    boolean isFullyRetrieved() {
      return (unretrievedPathParts.length == 0) || optionalFields.containsAll(Arrays.asList(unretrievedPathParts));
    }

    /**
     * Get a part of json path which was fully retrieved.
     * E.g. for path "/bookstore/store/books" retrieved path may be "/bookstore/store" and unretrieved may be
     * "/books".
     *
     * @return a retrieved part of path.
     */
    @VisibleForTesting
    String getRetrievedPath() {
      return retrievedPath;
    }

    /**
     * Get a part of json path which was not retrieved.
     * E.g. for path "/bookstore/store/books" retrieved path may be "/bookstore/store" and unretrieved may be
     * "/books".
     *
     * @return an unretrieved part of path.
     */
    public String getUnretrievedPath() {
      return unretrievedPath;
    }


    public JsonObject getAsJsonObject() {
      assertClass(JsonObject.class);
      return result.getAsJsonObject();
    }

    public JsonPrimitive getAsJsonPrimitive() {
      assertClass(JsonPrimitive.class);
      return result.getAsJsonPrimitive();
    }

    public JsonArray getAsJsonArray() {
      assertClass(JsonArray.class);
      return result.getAsJsonArray();
    }

    public JsonElement get() {
      return result;
    }

    @Override
    public String toString() {
      return "JsonQueryResponse{" +
        "retrievedPath='" + retrievedPath + '\'' +
        ", unretrievedPath='" + unretrievedPath + '\'' +
        ", optionalFields='" + optionalFields + '\'' +
        ", result=" + result +
        '}';
    }
  }
}
