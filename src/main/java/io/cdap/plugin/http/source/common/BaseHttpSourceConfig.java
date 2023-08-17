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
package io.cdap.plugin.http.source.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.http.common.BaseHttpConfig;
import io.cdap.plugin.http.source.common.error.ErrorHandling;
import io.cdap.plugin.http.source.common.error.HttpErrorHandlerEntity;
import io.cdap.plugin.http.source.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.source.common.http.KeyStoreType;
import io.cdap.plugin.http.source.common.pagination.PaginationIteratorFactory;
import io.cdap.plugin.http.source.common.pagination.PaginationType;
import io.cdap.plugin.http.source.common.pagination.page.PageFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Base configuration for HTTP Streaming and Batch plugins.
 */
public abstract class BaseHttpSourceConfig extends BaseHttpConfig {
  public static final String PROPERTY_REFERENCE_NAME = "referenceName";
  public static final String PROPERTY_URL = "url";
  public static final String PROPERTY_HTTP_METHOD = "httpMethod";
  public static final String PROPERTY_HEADERS = "headers";
  public static final String PROPERTY_REQUEST_BODY = "requestBody";
  public static final String PROPERTY_FORMAT = "format";
  public static final String PROPERTY_RESULT_PATH = "resultPath";
  public static final String PROPERTY_FIELDS_MAPPING = "fieldsMapping";
  public static final String PROPERTY_CSV_SKIP_FIRST_ROW = "csvSkipFirstRow";
  public static final String PROPERTY_HTTP_ERROR_HANDLING = "httpErrorsHandling";
  public static final String PROPERTY_ERROR_HANDLING = "errorHandling";
  public static final String PROPERTY_RETRY_POLICY = "retryPolicy";
  public static final String PROPERTY_LINEAR_RETRY_INTERVAL = "linearRetryInterval";
  public static final String PROPERTY_MAX_RETRY_DURATION = "maxRetryDuration";
  public static final String PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  public static final String PROPERTY_READ_TIMEOUT = "readTimeout";
  public static final String PROPERTY_PAGINATION_TYPE = "paginationType";
  public static final String PROPERTY_START_INDEX = "startIndex";
  public static final String PROPERTY_MAX_INDEX = "maxIndex";
  public static final String PROPERTY_INDEX_INCREMENT = "indexIncrement";
  public static final String PROPERTY_NEXT_PAGE_FIELD_PATH = "nextPageFieldPath";
  public static final String PROPERTY_NEXT_PAGE_TOKEN_PATH = "nextPageTokenPath";
  public static final String PROPERTY_NEXT_PAGE_URL_PARAMETER = "nextPageUrlParameter";
  public static final String PROPERTY_CUSTOM_PAGINATION_CODE = "customPaginationCode";
  public static final String PROPERTY_WAIT_TIME_BETWEEN_PAGES = "waitTimeBetweenPages";
  public static final String PROPERTY_OAUTH2_ENABLED = "oauth2Enabled";
  public static final String PROPERTY_VERIFY_HTTPS = "verifyHttps";
  public static final String PROPERTY_KEYSTORE_FILE = "keystoreFile";
  public static final String PROPERTY_KEYSTORE_TYPE = "keystoreType";
  public static final String PROPERTY_KEYSTORE_PASSWORD = "keystorePassword";
  public static final String PROPERTY_KEYSTORE_KEY_ALGORITHM = "keystoreKeyAlgorithm";
  public static final String PROPERTY_TRUSTSTORE_FILE = "trustStoreFile";
  public static final String PROPERTY_TRUSTSTORE_TYPE = "trustStoreType";
  public static final String PROPERTY_TRUSTSTORE_PASSWORD = "trustStorePassword";
  public static final String PROPERTY_TRUSTSTORE_KEY_ALGORITHM = "trustStoreKeyAlgorithm";
  public static final String PROPERTY_TRANSPORT_PROTOCOLS = "transportProtocols";
  public static final String PROPERTY_CIPHER_SUITES = "cipherSuites";
  public static final String PROPERTY_SCHEMA = "schema";

  public static final String PAGINATION_INDEX_PLACEHOLDER_REGEX = "\\{pagination.index\\}";
  public static final String PAGINATION_INDEX_PLACEHOLDER = "{pagination.index}";

  @Name(PROPERTY_URL)
  @Description("Url to fetch to the first page. The url must start with a protocol (e.g. http://).")
  @Macro
  protected String url;

  @Name(PROPERTY_HTTP_METHOD)
  @Description("HTTP request method.")
  @Macro
  protected String httpMethod;

  @Name(PROPERTY_HEADERS)
  @Nullable
  @Description("Headers to send with each HTTP request.")
  @Macro
  protected String headers;

  @Nullable
  @Name(PROPERTY_REQUEST_BODY)
  @Description("Body to send with each HTTP request.")
  @Macro
  protected String requestBody;

  @Name(PROPERTY_FORMAT)
  @Description("Format of the HTTP response. This determines how the response is converted into output records.")
  @Macro
  protected String format;

  @Nullable
  @Name(PROPERTY_RESULT_PATH)
  @Description("Path to the results. When the format is XML, this is an XPath. " +
    "When the format is JSON, this is a JSON path.")
  @Macro
  protected String resultPath;

  @Nullable
  @Name(PROPERTY_FIELDS_MAPPING)
  @Description("Mapping of fields in a record to fields in retrieved element. The left column contains the " +
    "name of schema field. The right column contains path to it within a relative to an element. " +
    "It can be either XPath or JSON path.")
  @Macro
  protected String fieldsMapping;

  @Nullable
  @Name(PROPERTY_CSV_SKIP_FIRST_ROW)
  @Description("Whether to skip the first row of the HTTP response. " +
    "This is usually set if the first row is a header row.")
  @Macro
  protected String csvSkipFirstRow;

  @Nullable
  @Name(PROPERTY_HTTP_ERROR_HANDLING)
  @Description("Defines the error handling strategy to use for certain HTTP response codes." +
    "The left column contains a regular expression for HTTP status code. The right column contains an action which" +
    "is done in case of match. If HTTP status code matches multiple regular expressions, " +
    "the first specified in mapping is matched.")
  protected String httpErrorsHandling;

  @Name(PROPERTY_ERROR_HANDLING)
  @Description("Error handling strategy to use when the HTTP response cannot be transformed to an output record.")
  protected String errorHandling;

  @Name(PROPERTY_RETRY_POLICY)
  @Description("Policy used to calculate delay between retries.")
  protected String retryPolicy;

  @Nullable
  @Name(PROPERTY_LINEAR_RETRY_INTERVAL)
  @Description("Interval between retries. Is only used if retry policy is \"linear\".")
  @Macro
  protected Long linearRetryInterval;

  @Name(PROPERTY_MAX_RETRY_DURATION)
  @Description("Maximum time in seconds retries can take.")
  @Macro
  protected Long maxRetryDuration;

  @Name(PROPERTY_CONNECT_TIMEOUT)
  @Description("Maximum time in seconds connection initialization is allowed to take.")
  @Macro
  protected Integer connectTimeout;

  @Name(PROPERTY_READ_TIMEOUT)
  @Description("Maximum time in seconds fetching data from the server is allowed to take.")
  @Macro
  protected Integer readTimeout;

  @Name(PROPERTY_PAGINATION_TYPE)
  @Description("Strategy used to determine how to get next page.")
  protected String paginationType;

  @Nullable
  @Name(PROPERTY_START_INDEX)
  @Description("[Pagination: Increment an index] Start value of {pagination.index} placeholder.")
  @Macro
  protected Long startIndex;

  @Nullable
  @Name(PROPERTY_MAX_INDEX)
  @Description("[Pagination: Increment an index] Maximum value of {pagination.index} placeholder. " +
    "If empty, pagination will happen until the page with no elements.")
  @Macro
  protected Long maxIndex;

  @Nullable
  @Name(PROPERTY_INDEX_INCREMENT)
  @Description("[Pagination: Increment an index] A value which the {pagination.index} placeholder is incremented by. " +
    "Increment can be negative.")
  @Macro
  protected Long indexIncrement;

  @Nullable
  @Name(PROPERTY_NEXT_PAGE_FIELD_PATH)
  @Description("[Pagination: Link in response body] A JSON path or an XPath to a field which contains next page url." +
    "It can be either relative or absolute url.")
  @Macro
  protected String nextPageFieldPath;

  @Nullable
  @Name(PROPERTY_NEXT_PAGE_TOKEN_PATH)
  @Description("[Pagination: Token in response body] " +
    "A JSON path or an XPath to a field which contains next page token.")
  @Macro
  protected String nextPageTokenPath;

  @Nullable
  @Name(PROPERTY_NEXT_PAGE_URL_PARAMETER)
  @Description("[Pagination: Token in response body] " +
    "A parameter which is appended to url in order to specify next page token.")
  @Macro
  protected String nextPageUrlParameter;

  @Nullable
  @Name(PROPERTY_CUSTOM_PAGINATION_CODE)
  @Description("[Pagination: Custom] A code which implements retrieving a next page url based " +
    "on previous page contents and headers.")
  protected String customPaginationCode;

  @Name(PROPERTY_WAIT_TIME_BETWEEN_PAGES)
  @Nullable
  @Description("Time in milliseconds to wait between HTTP requests for the next page.")
  @Macro
  protected Long waitTimeBetweenPages;

  @Name(PROPERTY_VERIFY_HTTPS)
  @Description("If false, untrusted trust certificates (e.g. self signed), will not lead to an" +
    "error. Do not disable this in production environment on a network you do not entirely trust. " +
    "Especially public internet.")
  @Macro
  protected String verifyHttps;

  @Nullable
  @Name(PROPERTY_KEYSTORE_FILE)
  @Description("A path to a file which contains keystore.")
  @Macro
  protected String keystoreFile;

  @Nullable
  @Name(PROPERTY_KEYSTORE_TYPE)
  @Description("Format of a keystore.")
  @Macro
  protected String keystoreType;

  @Nullable
  @Name(PROPERTY_KEYSTORE_PASSWORD)
  @Description("Password for a keystore. If a keystore is not password protected leave it empty.")
  @Macro
  protected String keystorePassword;

  @Nullable
  @Name(PROPERTY_KEYSTORE_KEY_ALGORITHM)
  @Description("An algorithm used for keystore.")
  @Macro
  protected String keystoreKeyAlgorithm;

  @Nullable
  @Name(PROPERTY_TRUSTSTORE_FILE)
  @Description("A path to a file which contains truststore.")
  @Macro
  protected String trustStoreFile;

  @Nullable
  @Name(PROPERTY_TRUSTSTORE_TYPE)
  @Description("Format of a truststore.")
  @Macro
  protected String trustStoreType;

  @Nullable
  @Name(PROPERTY_TRUSTSTORE_PASSWORD)
  @Description("Password for a truststore. If a truststore is not password protected leave it empty.")
  @Macro
  protected String trustStorePassword;

  @Nullable
  @Name(PROPERTY_TRUSTSTORE_KEY_ALGORITHM)
  @Description("An algorithm used for truststore.")
  @Macro
  protected String trustStoreKeyAlgorithm;

  @Nullable
  @Name(PROPERTY_TRANSPORT_PROTOCOLS)
  @Description("Transport protocols which are allowed for connection.")
  @Macro
  protected String transportProtocols;

  @Nullable
  @Name(PROPERTY_CIPHER_SUITES)
  @Description("Cipher suites which are allowed for connection. " +
    "Colons, commas or spaces are also acceptable separators.")
  @Macro
  protected String cipherSuites;

  @Nullable
  @Name(PROPERTY_SCHEMA)
  @Macro
  @Description("Output schema. Is required to be set.")
  protected String schema;

  protected BaseHttpSourceConfig(String referenceName) {
    super(referenceName);
  }

  public String getUrl() {
    return url;
  }

  public String getHttpMethod() {
    return httpMethod;
  }

  @Nullable
  public String getHeaders() {
    return headers;
  }

  @Nullable
  public String getRequestBody() {
    return requestBody;
  }

  public PageFormat getFormat() {
    return getEnumValueByString(PageFormat.class, format, PROPERTY_FORMAT);
  }

  @Nullable
  public String getResultPath() {
    return resultPath;
  }

  @Nullable
  public String getFieldsMapping() {
    return fieldsMapping;
  }

  public Boolean getCsvSkipFirstRow() {
    return Boolean.parseBoolean(csvSkipFirstRow);
  }

  @Nullable
  public String getHttpErrorsHandling() {
    return httpErrorsHandling;
  }

  public ErrorHandling getErrorHandling() {
    return getEnumValueByString(ErrorHandling.class, errorHandling, PROPERTY_ERROR_HANDLING);
  }

  public RetryPolicy getRetryPolicy() {
    return getEnumValueByString(RetryPolicy.class, retryPolicy, PROPERTY_RETRY_POLICY);
  }

  @Nullable
  public Long getLinearRetryInterval() {
    return linearRetryInterval;
  }

  public Long getMaxRetryDuration() {
    return maxRetryDuration;
  }

  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  public Integer getReadTimeout() {
    return readTimeout;
  }

  public PaginationType getPaginationType() {
    return getEnumValueByString(PaginationType.class, paginationType, PROPERTY_PAGINATION_TYPE);
  }

  @Nullable
  public Long getStartIndex() {
    return startIndex;
  }

  @Nullable
  public Long getMaxIndex() {
    return maxIndex;
  }

  @Nullable
  public Long getIndexIncrement() {
    return indexIncrement;
  }

  @Nullable
  public String getNextPageFieldPath() {
    return nextPageFieldPath;
  }

  @Nullable
  public String getNextPageTokenPath() {
    return nextPageTokenPath;
  }

  @Nullable
  public String getNextPageUrlParameter() {
    return nextPageUrlParameter;
  }

  @Nullable
  public String getCustomPaginationCode() {
    return customPaginationCode;
  }

  @Nullable
  public Long getWaitTimeBetweenPages() {
    return waitTimeBetweenPages;
  }

  public Boolean getVerifyHttps() {
    return Boolean.parseBoolean(verifyHttps);
  }

  @Nullable
  public String getKeystoreFile() {
    return keystoreFile;
  }

  @Nullable
  public KeyStoreType getKeystoreType() {
    return getEnumValueByString(KeyStoreType.class, keystoreType, PROPERTY_KEYSTORE_TYPE);
  }


  public void setServiceAccountType(String serviceAccountType) {
    this.serviceAccountType = serviceAccountType;
  }

  public void setServiceAccountJson(String serviceAccountJson) {
    this.serviceAccountJson = serviceAccountJson;
  }

  public void setServiceAccountFilePath(String serviceAccountFilePath) {
    this.serviceAccountFilePath = serviceAccountFilePath;
  }

  @Nullable
  public String getKeystorePassword() {
    return keystorePassword;
  }

  @Nullable
  public String getKeystoreKeyAlgorithm() {
    return keystoreKeyAlgorithm;
  }

  @Nullable
  public String getTrustStoreFile() {
    return trustStoreFile;
  }

  @Nullable
  public KeyStoreType getTrustStoreType() {
    return getEnumValueByString(KeyStoreType.class, trustStoreType, PROPERTY_TRUSTSTORE_TYPE);
  }

  @Nullable
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  @Nullable
  public String getTrustStoreKeyAlgorithm() {
    return trustStoreKeyAlgorithm;
  }

  @Nullable
  public String getTransportProtocols() {
    return transportProtocols;
  }

  @Nullable
  public String getCipherSuites() {
    return cipherSuites;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new InvalidConfigPropertyException("Unable to parse output schema: " +
                                                 schema, e, PROPERTY_SCHEMA);
    }
  }

  @Nullable
  public Map<String, String> getHeadersMap() {
    return getMapFromKeyValueString(headers);
  }

  public List<HttpErrorHandlerEntity> getHttpErrorHandlingEntries() {
    Map<String, String> httpErrorsHandlingMap = getMapFromKeyValueString(httpErrorsHandling);
    List<HttpErrorHandlerEntity> results = new ArrayList<>(httpErrorsHandlingMap.size());

    for (Map.Entry<String, String> entry : httpErrorsHandlingMap.entrySet()) {
      String regex = entry.getKey();
      try {
        results.add(new HttpErrorHandlerEntity(Pattern.compile(regex),
                   getEnumValueByString(RetryableErrorHandling.class,
                                        entry.getValue(), PROPERTY_HTTP_ERROR_HANDLING)));
      } catch (PatternSyntaxException e) {
        // We embed causing exception message into this one. Since this message is shown on UI when validation fails.
        throw new InvalidConfigPropertyException(
          String.format(
            "Error handling regex '%s' is not valid. %s", regex, e.getMessage()), PROPERTY_HTTP_ERROR_HANDLING);
      }
    }
    return results;
  }

  public Map<String, String> getFullFieldsMapping() {
    Map<String, String> result = new HashMap<>();

    if (!Strings.isNullOrEmpty(schema)) {
      for (Schema.Field field : getSchema().getFields()) {
        result.put(field.getName(), "/" + field.getName());
      }
    }

    Map<String, String> fieldsMappingMap = getMapFromKeyValueString(fieldsMapping);
    result.putAll(fieldsMappingMap);
    return result;
  }

  public List<String> getTransportProtocolsList() {
    return getListFromString(transportProtocols);
  }

  public String getReferenceNameOrNormalizedFQN() {
    return Strings.isNullOrEmpty(referenceName) ? ReferenceNames.normalizeFqn(url) : referenceName;
  }

  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);

    // Validate URL
    if (!containsMacro(PROPERTY_URL)) {
      try {
        // replace with placeholder with anything just during pagination
        new URI(getUrl().replaceAll(PAGINATION_INDEX_PLACEHOLDER_REGEX, "0"));

        // Validate HTTP Error Handling Map
        if (!containsMacro(PROPERTY_HTTP_ERROR_HANDLING)) {
          List<HttpErrorHandlerEntity> httpErrorsHandlingEntries = getHttpErrorHandlingEntries();
          boolean supportsSkippingPages = PaginationIteratorFactory
            .createInstance(this, null).supportsSkippingPages();

          if (!supportsSkippingPages) {
            for (HttpErrorHandlerEntity httpErrorsHandlingEntry : httpErrorsHandlingEntries) {
              ErrorHandling postRetryStrategy = httpErrorsHandlingEntry.getStrategy().getAfterRetryStrategy();
              if (postRetryStrategy.equals(ErrorHandling.SEND) ||
                postRetryStrategy.equals(ErrorHandling.SKIP)) {
                throw new InvalidConfigPropertyException(
                  String.format("Error handling strategy '%s' is not support in combination with pagination type",
                                httpErrorsHandlingEntry.getStrategy(), getPaginationType()),
                  PROPERTY_HTTP_ERROR_HANDLING);
              }
            }
          }
        }
      } catch (URISyntaxException e) {
        throw new InvalidConfigPropertyException(
          String.format("URL value is not valid: '%s'", getUrl()), e, PROPERTY_URL);
      }
    }

    // Validate Linear Retry Interval
    if (!containsMacro(PROPERTY_RETRY_POLICY) && getRetryPolicy() == RetryPolicy.LINEAR) {
      assertIsSet(getLinearRetryInterval(), PROPERTY_LINEAR_RETRY_INTERVAL, "retry policy is linear");
    }

    // Validate pagination type related properties
    if (!containsMacro(PROPERTY_PAGINATION_TYPE)) {
      String reasonPagination = String.format("pagination type is '%s'", getPaginationType());

      Map<String, Object> propertiesShouldBeNull = new HashMap<String, Object>() {{
        put(PROPERTY_START_INDEX, getStartIndex());
        put(PROPERTY_MAX_INDEX, getMaxIndex());
        put(PROPERTY_INDEX_INCREMENT, getIndexIncrement());
        put(PROPERTY_NEXT_PAGE_FIELD_PATH, getNextPageFieldPath());
        put(PROPERTY_NEXT_PAGE_TOKEN_PATH, getNextPageTokenPath());
        put(PROPERTY_NEXT_PAGE_URL_PARAMETER, getNextPageUrlParameter());
        put(PROPERTY_CUSTOM_PAGINATION_CODE, getCustomPaginationCode());
      }};

      HashMap<String, Object> propertiesShouldBeNotNull = new HashMap<>();

      switch (getPaginationType()) {
        case LINK_IN_RESPONSE_BODY:
          propertiesShouldBeNotNull.put(PROPERTY_NEXT_PAGE_FIELD_PATH,
                                        propertiesShouldBeNull.remove(PROPERTY_NEXT_PAGE_FIELD_PATH));
          break;
        case TOKEN_IN_RESPONSE_BODY:
          propertiesShouldBeNotNull.put(PROPERTY_NEXT_PAGE_TOKEN_PATH,
                                        propertiesShouldBeNull.remove(PROPERTY_NEXT_PAGE_TOKEN_PATH));
          propertiesShouldBeNotNull.put(PROPERTY_NEXT_PAGE_URL_PARAMETER,
                                        propertiesShouldBeNull.remove(PROPERTY_NEXT_PAGE_URL_PARAMETER));
          break;
        case INCREMENT_AN_INDEX:
          propertiesShouldBeNotNull.put(PROPERTY_START_INDEX,
                                        propertiesShouldBeNull.remove(PROPERTY_START_INDEX));
          propertiesShouldBeNotNull.put(PROPERTY_INDEX_INCREMENT,
                                        propertiesShouldBeNull.remove(PROPERTY_INDEX_INCREMENT));
          propertiesShouldBeNull.remove(PROPERTY_MAX_INDEX); // can be both null and non null

          if (!containsMacro(PROPERTY_URL) && !url.contains(PAGINATION_INDEX_PLACEHOLDER)) {
            throw new InvalidConfigPropertyException(
              String.format("Url '%s' must contain '%s' placeholder when pagination type is '%s'", getUrl(),
                            PAGINATION_INDEX_PLACEHOLDER, getPaginationType()),
              PROPERTY_URL);
          }
          break;
        case CUSTOM:
          propertiesShouldBeNotNull.put(PROPERTY_CUSTOM_PAGINATION_CODE,
                                        propertiesShouldBeNull.remove(PROPERTY_CUSTOM_PAGINATION_CODE));
          break;
        // other types don't require any fields. Check for unknown values is already done. Do nothing here
      }

      for (Map.Entry<String, Object> entry : propertiesShouldBeNull.entrySet()) {
        String propertyName = entry.getKey();
        Object propertyValue = entry.getValue();
        assertIsNotSet(propertyValue, propertyName, reasonPagination);
      }

      for (Map.Entry<String, Object> entry : propertiesShouldBeNotNull.entrySet()) {
        String propertyName = entry.getKey();
        Object propertyValue = entry.getValue();
        assertIsSet(propertyValue, propertyName, reasonPagination);
      }
    }

    // Validate format properties
    if (!containsMacro(PROPERTY_FORMAT)) {
      String reasonFormat = String.format("page format is '%s'", getFormat());

      if (getFormat().equals(PageFormat.JSON) || getFormat().equals(PageFormat.XML)) {
        if (!getFormat().equals(PageFormat.JSON)) {
          assertIsSet(getResultPath(), PROPERTY_RESULT_PATH, reasonFormat);
        }
        getFullFieldsMapping(); // can be null, but call getter to verify correctness of regexps
      } else {
        assertIsNotSet(getResultPath(), PROPERTY_RESULT_PATH, reasonFormat);
        assertIsNotSet(getFieldsMapping(), PROPERTY_FIELDS_MAPPING, reasonFormat);
      }
    }

    if (!containsMacro(PROPERTY_VERIFY_HTTPS) && !getVerifyHttps()) {
      assertIsNotSet(getTrustStoreFile(), PROPERTY_TRUSTSTORE_FILE,
                     String.format("trustore settings are ignored due to disabled %s", PROPERTY_VERIFY_HTTPS));
    }
  }

  public void validateSchema() {
    Schema schema = getSchema();
    if (!containsMacro(PROPERTY_SCHEMA) && schema == null) {
      throw new InvalidConfigPropertyException(
              String.format("Output schema cannot be empty"), PROPERTY_SCHEMA);
    }
    if (!containsMacro(PROPERTY_FORMAT)) {
      PageFormat format = getFormat();

      if (format.equals(PageFormat.TEXT) || format.equals(PageFormat.BLOB)) {
        Schema.Type expectedFieldType = format.equals(PageFormat.TEXT) ? Schema.Type.STRING : Schema.Type.BYTES;
        List<Schema.Field> fields = getSchema().getFields();
        if (fields == null || fields.size() != 1 || fields.get(0).getSchema().getType() != expectedFieldType) {
          throw new InvalidStageException(String.format("Schema must be a record with a single %s field.",
                                                        expectedFieldType.toString().toLowerCase()));
        }
      }
    }
  }

  public static void assertIsSet(Object propertyValue, String propertyName, String reason) {
    if (propertyValue == null) {
      throw new InvalidConfigPropertyException(
        String.format("Property '%s' must be set, since %s", propertyName, reason), propertyName);
    }
  }

  public static void assertIsNotSet(Object propertyValue, String propertyName, String reason) {
    if (propertyValue != null) {
      throw new InvalidConfigPropertyException(
        String.format("Property '%s' must not be set, since %s", propertyName, reason), propertyName);
    }
  }


  public static <T extends EnumWithValue> T
  getEnumValueByString(Class<T> enumClass, String stringValue, String propertyName) {
    return Stream.of(enumClass.getEnumConstants())
      .filter(keyType -> keyType.getValue().equalsIgnoreCase(stringValue))
      .findAny()
      .orElseThrow(() -> new InvalidConfigPropertyException(
        String.format("Unsupported value for '%s': '%s'", propertyName, stringValue), propertyName));
  }

  @Nullable
  public static Long toLong(String value, String propertyName) {
    if (Strings.isNullOrEmpty(value)) {
      return null;
    }

    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ex) {
      throw new InvalidConfigPropertyException(String.format("Unsupported value for '%s': '%s'", propertyName, value),
                                               propertyName);
    }
  }

  public static List<String> getListFromString(String value) {
    if (Strings.isNullOrEmpty(value)) {
      return Collections.emptyList();
    }
    return Arrays.asList(value.split(","));
  }

  public static Map<String, String> getMapFromKeyValueString(String keyValueString) {
    Map<String, String> result = new LinkedHashMap<>();

    if (Strings.isNullOrEmpty(keyValueString)) {
      return result;
    }

    String[] mappings = keyValueString.split(",");
    for (String map : mappings) {
      String[] columns = map.split(":");
      if (columns.length < 2) { //For scenario where either of key or value not provided
        throw new IllegalArgumentException(String.format("Missing value for key %s", columns[0]));
      }
      result.put(columns[0], columns[1]);
    }
    return result;
  }
}
