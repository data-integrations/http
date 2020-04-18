package io.cdap.plugin.http.source.common.pagination.page;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * A class which contains information regarding results of searching an element in json by json path.
 */
class JsonQueryResponse {
  private final String[] unretrievedPathParts;
  private final List<String> optionalFields;
  private final String retrievedPath;
  private final String unretrievedPath;
  private final JsonElement result;

  JsonQueryResponse(String[] retrievedPathParts, String[] unretrievedPathParts,
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