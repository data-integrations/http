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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * Utility functions for working with JSON document.
 */
public class JSONUtil {
  private static final JsonParser parser = new JsonParser();

  public static JsonObject toJsonObject(String text) {
    return parser.parse(text).getAsJsonObject();
  }

  public static JsonArray toJsonArray(String text) {
    return parser.parse(text).getAsJsonArray();
  }

  /**
   * Find an element by jsonPath in given json object. If element not found, information about the search is returned.
   * Like until which element json path evaluation was successful.
   *
   * @param jsonObject a jsonObject on which the search should be performed
   * @param jsonPath a slash separated path. E.g. "/bookstore/books"
   * @return an object containing information about search results, success/failure.
   */
  public static JsonQueryResponse getJsonElementByPath(JsonObject jsonObject, String jsonPath) {
    String stripped = StringUtils.strip(jsonPath.trim(), "/");
    String[] pathParts;
    if (!stripped.isEmpty()) {
      pathParts = stripped.split("/");
    } else {
      pathParts = new String[0];
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
          currentElement
        );
      }

      currentElement = jsonObject.get(pathPart);
    }
    return new JsonQueryResponse(
      Arrays.copyOfRange(pathParts, 0, pathParts.length),
      new String[0],
      currentElement
    );
  }

  /**
   * A class which contains information regarding results of searching an element in json by json path.
   */
  public static class JsonQueryResponse {
    private final String[] retrievedPathParts;
    private final String[] unretrievedPathParts;
    private final String retrievedPath;
    private final String unretrievedPath;
    private final JsonElement result;

    public JsonQueryResponse(String[] retrievedPathParts, String[] unretrievedPathParts, JsonElement result) {
      this.retrievedPathParts = retrievedPathParts;
      this.unretrievedPathParts = unretrievedPathParts;
      this.retrievedPath = "/" + StringUtils.join(retrievedPathParts, '/');
      this.unretrievedPath = "/" + StringUtils.join(unretrievedPathParts, '/');
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
    public boolean isFullyRetrieved() {
      return (unretrievedPathParts.length == 0);
    }

    /**
     * Fails if json path was not fully successfully retrieved till the last element.
     */
    public void assertFullyRetrieved() {
      if (!isFullyRetrieved()) {
        throw new IllegalArgumentException(String.format(
          "Cannot retrieve the path part '%s' of path '%s'. \nLast successfully retrieved part is: '%s'",
          getUnretrievedPath(), getRetrievedPath() + getUnretrievedPath(), result.toString()));
      }
    }

    /**
     * Get a part of json path which was fully retrieved.
     * E.g. for path "/bookstore/store/books" retrieved path may be "/bookstore/store" and unretrieved may be
     * "/books".
     *
     * @return a retrieved part of path.
     */
    public String getRetrievedPath() {
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
        ", result=" + result +
        '}';
    }
  }
}
