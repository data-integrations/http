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

package io.cdap.plugin.http.sink.batch;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;

/**
 * Config class for {@link HTTPSink}.
 */
public class HTTPSinkConfig extends ReferencePluginConfig {
  public static final String URL = "url";
  public static final String METHOD = "method";
  public static final String BATCH_SIZE = "batchSize";
  public static final String DELIMETER_FOR_MESSAGE = "delimiterForMessages";
  public static final String MESSAGE_FORMAT = "messageFormat";
  public static final String BODY = "body";
  public static final String REQUEST_HEADERS = "requestHeaders";
  public static final String CHARSET = "charset";
  public static final String FOLLOW_REDIRECTS = "followRedirects";
  public static final String DISABLE_SSL_VALIDATION = "disableSSLValidation";
  public static final String NUM_RETRIES = "numRetries";
  public static final String CONNECTION_TIMEOUT = "connectTimeout";
  public static final String READ_TIMEOUT = "readTimeout";
  public static final String FAIL_ON_NON_200_RESPONSE = "failOnNon200Response";

  private static final String KV_DELIMITER = ":";
  private static final String DELIMITER = "\n";
  private static final Set<String> METHODS = ImmutableSet.of(HttpMethod.GET, HttpMethod.POST,
                                                             HttpMethod.PUT, HttpMethod.DELETE);

  @Name(URL)
  @Description("The URL to post data to. (Macro Enabled)")
  @Macro
  private final String url;

  @Name(METHOD)
  @Description("The http request method. Defaults to POST. (Macro Enabled)")
  @Macro
  private final String method;

  @Name(BATCH_SIZE)
  @Description("Batch size. Defaults to 1. (Macro Enabled)")
  @Macro
  private final Integer batchSize;

  @Name(DELIMETER_FOR_MESSAGE)
  @Nullable
  @Description("Delimiter for messages to be used while batching. Defaults to \"\\n\". (Macro Enabled)")
  @Macro
  private final String delimiterForMessages;

  @Name(MESSAGE_FORMAT)
  @Description("Format to send messsage in. (Macro Enabled)")
  @Macro
  private final String messageFormat;

  @Name(BODY)
  @Nullable
  @Description("Optional custom message. This is required if the message format is set to 'Custom'." +
    "User can leverage incoming message fields in the post payload. For example-" +
    "User has defined payload as \\{ \"messageType\" : \"update\", \"name\" : \"#firstName\" \\}" +
    "where #firstName will be substituted for the value that is in firstName in the incoming message. " +
    "(Macro enabled)")
  @Macro
  private final String body;

  @Name(REQUEST_HEADERS)
  @Nullable
  @Description("Request headers to set when performing the http request. (Macro enabled)")
  @Macro
  private final String requestHeaders;

  @Name(CHARSET)
  @Description("Charset. Defaults to UTF-8. (Macro enabled)")
  @Macro
  private final String charset;

  @Name(FOLLOW_REDIRECTS)
  @Description("Whether to automatically follow redirects. Defaults to true. (Macro enabled)")
  @Macro
  private final Boolean followRedirects;

  @Name(DISABLE_SSL_VALIDATION)
  @Description("If user enables SSL validation, they will be expected to add the certificate to the trustStore" +
    " on each machine. Defaults to true. (Macro enabled)")
  @Macro
  private final Boolean disableSSLValidation;

  @Name(NUM_RETRIES)
  @Description("The number of times the request should be retried if the request fails. Defaults to 3. " +
    "(Macro enabled)")
  @Macro
  private final Integer numRetries;

  @Name(CONNECTION_TIMEOUT)
  @Description("Sets the connection timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute). " +
    "(Macro enabled)")
  @Nullable
  @Macro
  private final Integer connectTimeout;

  @Name(READ_TIMEOUT)
  @Description("The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute). " +
    "(Macro enabled)")
  @Nullable
  @Macro
  private final Integer readTimeout;

  @Name(FAIL_ON_NON_200_RESPONSE)
  @Description("Whether to fail the pipeline on non-200 response from the http end point. Defaults to true. " +
    "(Macro enabled)")
  @Macro
  private final Boolean failOnNon200Response;

  public HTTPSinkConfig(String referenceName, String url, String method, Integer batchSize,
                        @Nullable String delimiterForMessages, String messageFormat, @Nullable String body,
                        @Nullable String requestHeaders, String charset,
                        boolean followRedirects, boolean disableSSLValidation, @Nullable int numRetries,
                        @Nullable int readTimeout, @Nullable int connectTimeout, boolean failOnNon200Response) {
    super(referenceName);
    this.url = url;
    this.method = method;
    this.batchSize = batchSize;
    this.delimiterForMessages = delimiterForMessages;
    this.messageFormat = messageFormat;
    this.body = body;
    this.requestHeaders = requestHeaders;
    this.charset = charset;
    this.followRedirects = followRedirects;
    this.disableSSLValidation = disableSSLValidation;
    this.numRetries = numRetries;
    this.readTimeout = readTimeout;
    this.connectTimeout = connectTimeout;
    this.failOnNon200Response = failOnNon200Response;
  }

  private HTTPSinkConfig(Builder builder) {
    super(builder.referenceName);
    url = builder.url;
    method = builder.method;
    batchSize = builder.batchSize;
    delimiterForMessages = builder.delimiterForMessages;
    messageFormat = builder.messageFormat;
    body = builder.body;
    requestHeaders = builder.requestHeaders;
    charset = builder.charset;
    followRedirects = builder.followRedirects;
    disableSSLValidation = builder.disableSSLValidation;
    numRetries = builder.numRetries;
    connectTimeout = builder.connectTimeout;
    readTimeout = builder.readTimeout;
    failOnNon200Response = builder.failOnNon200Response;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(HTTPSinkConfig copy) {
    Builder builder = new Builder();
    builder.referenceName = copy.referenceName;
    builder.url = copy.getUrl();
    builder.method = copy.getMethod();
    builder.batchSize = copy.getBatchSize();
    builder.delimiterForMessages = copy.getDelimiterForMessages();
    builder.messageFormat = copy.getMessageFormat();
    builder.body = copy.getBody();
    builder.requestHeaders = copy.getRequestHeaders();
    builder.charset = copy.getCharset();
    builder.followRedirects = copy.getFollowRedirects();
    builder.disableSSLValidation = copy.getDisableSSLValidation();
    builder.numRetries = copy.getNumRetries();
    builder.connectTimeout = copy.getConnectTimeout();
    builder.readTimeout = copy.getReadTimeout();
    builder.failOnNon200Response = copy.getFailOnNon200Response();
    return builder;
  }

  public String getUrl() {
    return url;
  }

  public String getMethod() {
    return method;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  @Nullable
  public String getDelimiterForMessages() {
    return delimiterForMessages;
  }

  public String getMessageFormat() {
    return messageFormat;
  }

  @Nullable
  public String getBody() {
    return body;
  }

  @Nullable
  public String getRequestHeaders() {
    return requestHeaders;
  }

  public String getCharset() {
    return charset;
  }

  public Boolean getFollowRedirects() {
    return followRedirects;
  }

  public Boolean getDisableSSLValidation() {
    return disableSSLValidation;
  }

  public Integer getNumRetries() {
    return numRetries;
  }

  @Nullable
  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  @Nullable
  public Integer getReadTimeout() {
    return readTimeout;
  }

  public Boolean getFailOnNon200Response() {
    return failOnNon200Response;
  }

  public Map<String, String> getRequestHeadersMap() {
    return convertHeadersToMap(requestHeaders);
  }

  public String getReferenceNameOrNormalizedFQN() {
    return Strings.isNullOrEmpty(referenceName) ? ReferenceNames.normalizeFqn(url) : referenceName;
  }

  public void validate(FailureCollector collector) {
    if (!containsMacro(URL)) {
      try {
        new URL(url);
      } catch (MalformedURLException e) {
        collector.addFailure(String.format("URL '%s' is malformed: %s", url, e.getMessage()), null)
          .withConfigProperty(URL);
      }
    }

    if (!containsMacro(CONNECTION_TIMEOUT) && Objects.nonNull(connectTimeout) && connectTimeout < 0) {
      collector.addFailure("Connection Timeout cannot be a negative number.", null)
        .withConfigProperty(CONNECTION_TIMEOUT);
    }

    try {
      convertHeadersToMap(requestHeaders);
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(REQUEST_HEADERS);
    }

    if (!containsMacro(METHOD) && !METHODS.contains(method.toUpperCase())) {
      collector.addFailure(
        String.format("Invalid request method %s, must be one of %s.", method, Joiner.on(',').join(METHODS)), null)
        .withConfigProperty(METHOD);
    }

    if (!containsMacro(NUM_RETRIES) && numRetries < 0) {
      collector.addFailure("Number of Retries cannot be a negative number.", null)
        .withConfigProperty(NUM_RETRIES);
    }

    if (!containsMacro(READ_TIMEOUT) && Objects.nonNull(readTimeout) && readTimeout < 0) {
      collector.addFailure("Read Timeout cannot be a negative number.", null)
        .withConfigProperty(READ_TIMEOUT);
    }

    if (!containsMacro(MESSAGE_FORMAT) && !containsMacro("body") && messageFormat.equalsIgnoreCase("Custom")
      && body == null) {
      collector.addFailure("For Custom message format, message cannot be null.", null)
        .withConfigProperty(MESSAGE_FORMAT);
    }
  }

  public void validateSchema(@Nullable Schema schema, FailureCollector collector) {
    if (schema == null) {
      return;
    }
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Schema must contain at least one field", null);
      throw collector.getOrThrowException();
    }
  }

  private Map<String, String> convertHeadersToMap(String headersString) {
    Map<String, String> headersMap = new HashMap<>();
    if (!Strings.isNullOrEmpty(headersString)) {
      for (String chunk : headersString.split(DELIMITER)) {
        String[] keyValue = chunk.split(KV_DELIMITER, 2);
        if (keyValue.length == 2) {
          headersMap.put(keyValue[0], keyValue[1]);
        } else {
          throw new IllegalArgumentException(String.format("Unable to parse key-value pair '%s'.", chunk));
        }
      }
    }
    return headersMap;
  }

  /**
   * Builder for creating a {@link HTTPSinkConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String url;
    private String method;
    private Integer batchSize;
    private String delimiterForMessages;
    private String messageFormat;
    private String body;
    private String requestHeaders;
    private String charset;
    private Boolean followRedirects;
    private Boolean disableSSLValidation;
    private Integer numRetries;
    private Integer connectTimeout;
    private Integer readTimeout;
    private Boolean failOnNon200Response;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setUrl(String url) {
      this.url = url;
      return this;
    }

    public Builder setMethod(String method) {
      this.method = method;
      return this;
    }

    public Builder setBatchSize(Integer batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setDelimiterForMessages(String delimiterForMessages) {
      this.delimiterForMessages = delimiterForMessages;
      return this;
    }

    public Builder setMessageFormat(String messageFormat) {
      this.messageFormat = messageFormat;
      return this;
    }

    public Builder setBody(String body) {
      this.body = body;
      return this;
    }

    public Builder setRequestHeaders(String requestHeaders) {
      this.requestHeaders = requestHeaders;
      return this;
    }

    public Builder setCharset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder setFollowRedirects(Boolean followRedirects) {
      this.followRedirects = followRedirects;
      return this;
    }

    public Builder setDisableSSLValidation(Boolean disableSSLValidation) {
      this.disableSSLValidation = disableSSLValidation;
      return this;
    }

    public Builder setNumRetries(Integer numRetries) {
      this.numRetries = numRetries;
      return this;
    }

    public Builder setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder setReadTimeout(Integer readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    public Builder setFailOnNon200Response(Boolean failOnNon200Response) {
      this.failOnNon200Response = failOnNon200Response;
      return this;
    }

    public HTTPSinkConfig build() {
      return new HTTPSinkConfig(this);
    }
  }
}
