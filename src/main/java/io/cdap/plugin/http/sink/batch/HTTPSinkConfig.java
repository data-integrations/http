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
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.ReferencePluginConfig;

import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import io.cdap.plugin.http.source.common.http.AuthType;
import io.cdap.plugin.http.source.common.http.OAuthUtil;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
  public static final String PROPERTY_OAUTH2_ENABLED = "oauth2Enabled";
  public static final String PROPERTY_AUTH_URL = "authUrl";
  public static final String PROPERTY_TOKEN_URL = "tokenUrl";
  public static final String PROPERTY_CLIENT_ID = "clientId";
  public static final String PROPERTY_CLIENT_SECRET = "clientSecret";
  public static final String PROPERTY_SCOPES = "scopes";
  public static final String PROPERTY_REFRESH_TOKEN = "refreshToken";

  public static final String PROPERTY_AUTH_TYPE = "authType";

  public static final String PROPERTY_AUTH_TYPE_LABEL = "Auth type";

  public static final String PROPERTY_USERNAME = "username";

  public static final String PROPERTY_PASSWORD = "password";

  public static final String PROPERTY_NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";

  public static final String PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceAccountFilePath";

  public static final String PROPERTY_NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";

  public static final String PROPERTY_SERVICE_ACCOUNT_FILE_PATH = "filePath";

  public static final String PROPERTY_SERVICE_ACCOUNT_JSON = "JSON";

  public static final String PROPERTY_AUTO_DETECT_VALUE = "auto-detect";

  public static final String PROPERTY_SERVICE_ACCOUNT_SCOPE = "serviceAccountScope";

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

  @Name(PROPERTY_OAUTH2_ENABLED)
  @Description("If true, plugin will perform OAuth2 authentication.")
  @Nullable
  protected String oauth2Enabled;

  @Nullable
  @Name(PROPERTY_AUTH_URL)
  @Description("Endpoint for the authorization server used to retrieve the authorization code.")
  @Macro
  protected String authUrl;

  @Nullable
  @Name(PROPERTY_TOKEN_URL)
  @Description("Endpoint for the resource server, which exchanges the authorization code for an access token.")
  @Macro
  protected String tokenUrl;

  @Nullable
  @Name(PROPERTY_CLIENT_ID)
  @Description("Client identifier obtained during the Application registration process.")
  @Macro
  protected String clientId;

  @Nullable
  @Name(PROPERTY_CLIENT_SECRET)
  @Description("Client secret obtained during the Application registration process.")
  @Macro
  protected String clientSecret;

  @Nullable
  @Name(PROPERTY_SCOPES)
  @Description("Scope of the access request, which might have multiple space-separated values.")
  @Macro
  protected String scopes;

  @Nullable
  @Name(PROPERTY_REFRESH_TOKEN)
  @Description("Token used to receive accessToken, which is end product of OAuth2.")
  @Macro
  protected String refreshToken;

  @Nullable
  @Name(PROPERTY_USERNAME)
  @Description("Username for basic authentication.")
  @Macro
  protected String username;

  @Nullable
  @Name(PROPERTY_PASSWORD)
  @Description("Password for basic authentication.")
  @Macro
  protected String password;

  @Name(PROPERTY_AUTH_TYPE)
  @Description("Type of authentication used to submit request. \n" +
          "OAuth2, Service account, Basic Authentication types are available.")
  protected String authType;

  @Name(PROPERTY_NAME_SERVICE_ACCOUNT_TYPE)
  @Description("Service account type, file path where the service account is located or the JSON content of the " +
          "service account.")
  @Nullable
  protected String serviceAccountType;

  @Nullable
  @Macro
  @Name(PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used for authorization. " +
          "Can be set to 'auto-detect' for getting service account from system variable. " +
          "The file/system variable must be present on every node in the cluster. " +
          "Service account json can be generated on Google Cloud " +
          "Service Account page (https://console.cloud.google.com/iam-admin/serviceaccounts).")
  protected String serviceAccountFilePath;

  @Name(PROPERTY_NAME_SERVICE_ACCOUNT_JSON)
  @Description("Content of the service account file.")
  @Nullable
  @Macro
  protected String serviceAccountJson;

  @Nullable
  @Name(PROPERTY_SERVICE_ACCOUNT_SCOPE)
  @Description("The additional Google credential scopes required to access entered url, " +
          "cloud-platform is included by default, " +
          "visit https://developers.google.com/identity/protocols/oauth2/scopes " +
          "for more information.")
  @Macro
  protected String serviceAccountScope;

  public HTTPSinkConfig(String referenceName, String url, String method, Integer batchSize,
                        @Nullable String delimiterForMessages, String messageFormat, @Nullable String body,
                        @Nullable String requestHeaders, String charset,
                        boolean followRedirects, boolean disableSSLValidation, @Nullable int numRetries,
                        @Nullable int readTimeout, @Nullable int connectTimeout, boolean failOnNon200Response,
                        String oauth2Enabled, String authType) {
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
    this.oauth2Enabled = oauth2Enabled;
    this.authType = authType;
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
    oauth2Enabled = builder.oauth2Enabled;
    authType = builder.authType;
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
    builder.oauth2Enabled = copy.getOAuth2Enabled();
    builder.authType = copy.getAuthTypeString();
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

  public Boolean getOauth2Enabled() {
    return Boolean.parseBoolean(oauth2Enabled);
  }

  public String getOAuth2Enabled() {
    return oauth2Enabled;
  }

  @Nullable
  public String getAuthUrl() {
    return authUrl;
  }

  @Nullable
  public String getTokenUrl() {
    return tokenUrl;
  }

  @Nullable
  public String getClientId() {
    return clientId;
  }

  @Nullable
  public String getClientSecret() {
    return clientSecret;
  }

  @Nullable
  public String getScopes() {
    return scopes;
  }

  @Nullable
  public String getRefreshToken() {
    return refreshToken;
  }

  public AuthType getAuthType() {
    return AuthType.fromValue(authType);
  }

  public String getAuthTypeString() {
    return authType;
  }

  public void setAuthType(String authType) {
    this.authType = authType;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public void setServiceAccountType(String serviceAccountType) {
    this.serviceAccountType = serviceAccountType;
  }

  @Nullable
  public String getServiceAccountType() {
    return serviceAccountType;
  }

  public void setServiceAccountJson(String serviceAccountJson) {
    this.serviceAccountJson = serviceAccountJson;
  }

  @Nullable
  public String getServiceAccountJson() {
    return serviceAccountJson;
  }

  public void setServiceAccountFilePath(String serviceAccountFilePath) {
    this.serviceAccountFilePath = serviceAccountFilePath;
  }

  @Nullable
  public String getServiceAccountFilePath() {
    return serviceAccountFilePath;
  }

  @Nullable
  public String getServiceAccountScope() {
    return serviceAccountScope;
  }

  public Map<String, String> getRequestHeadersMap() {
    return convertHeadersToMap(requestHeaders);
  }

  public Map<String, String> getHeadersMap(String header) {
    return convertHeadersToMap(header);
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

    // Validate OAuth2 properties
    if (!containsMacro(PROPERTY_OAUTH2_ENABLED) && this.getOauth2Enabled()) {
      String reasonOauth2 = "OAuth2 is enabled";
      assertIsSet(getAuthUrl(), PROPERTY_AUTH_URL, reasonOauth2);
      assertIsSet(getTokenUrl(), PROPERTY_TOKEN_URL, reasonOauth2);
      assertIsSet(getClientId(), PROPERTY_CLIENT_ID, reasonOauth2);
      assertIsSet(getClientSecret(), PROPERTY_CLIENT_SECRET, reasonOauth2);
      assertIsSet(getRefreshToken(), PROPERTY_REFRESH_TOKEN, reasonOauth2);
    }

    // Validate Authentication properties
    AuthType authType = getAuthType();
    switch (authType) {
      case OAUTH2:
        String reasonOauth2 = "OAuth2 is enabled";
        if (!containsMacro(PROPERTY_AUTH_URL)) {
          assertIsSet(getAuthUrl(), PROPERTY_AUTH_URL, reasonOauth2);
        }
        if (!containsMacro(PROPERTY_TOKEN_URL)) {
          assertIsSet(getTokenUrl(), PROPERTY_TOKEN_URL, reasonOauth2);
        }
        if (!containsMacro(PROPERTY_CLIENT_ID)) {
          assertIsSet(getClientId(), PROPERTY_CLIENT_ID, reasonOauth2);
        }
        if (!containsMacro((PROPERTY_CLIENT_SECRET))) {
          assertIsSet(getClientSecret(), PROPERTY_CLIENT_SECRET, reasonOauth2);
        }
        if (!containsMacro(PROPERTY_REFRESH_TOKEN)) {
          assertIsSet(getRefreshToken(), PROPERTY_REFRESH_TOKEN, reasonOauth2);
        }
        break;
      case SERVICE_ACCOUNT:
        String reasonSA = "Service Account is enabled";
        assertIsSet(getServiceAccountType(), PROPERTY_NAME_SERVICE_ACCOUNT_TYPE, reasonSA);
        boolean propertiesAreValid = validateServiceAccount(collector);
        if (propertiesAreValid) {
          try {
            String accessToken = OAuthUtil.getAccessTokenByServiceAccount(this);
          } catch (Exception e) {
            collector.addFailure("Unable to authenticate given service account info. ",
                            "Please make sure all infomation entered correctly")
                    .withStacktrace(e.getStackTrace());
          }
        }
        break;
      case BASIC_AUTH:
        String reasonBasicAuth = "Basic Authentication is enabled";
        if (!containsMacro(PROPERTY_USERNAME)) {
          assertIsSet(getUsername(), PROPERTY_USERNAME, reasonBasicAuth);
        }
        if (!containsMacro(PROPERTY_PASSWORD)) {
          assertIsSet(getPassword(), PROPERTY_PASSWORD, reasonBasicAuth);
        }
        break;
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

  private boolean validateServiceAccount(FailureCollector collector) {
    if (containsMacro(PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH) || containsMacro(PROPERTY_NAME_SERVICE_ACCOUNT_JSON)) {
      return false;
    }
    final Boolean bServiceAccountFilePath = isServiceAccountFilePath();
    final Boolean bServiceAccountJson = isServiceAccountJson();

    // we don't want the validation to fail because the VM used during the validation
    // may be different from the VM used during runtime and may not have the Google Drive Api scope.
    if (bServiceAccountFilePath && PROPERTY_AUTO_DETECT_VALUE.equalsIgnoreCase(serviceAccountFilePath)) {
      return false;
    }

    if (bServiceAccountFilePath != null && bServiceAccountFilePath) {
      if (!PROPERTY_AUTO_DETECT_VALUE.equals(serviceAccountFilePath) && !new File(serviceAccountFilePath).exists()) {
        collector.addFailure("Service Account File Path is not available.",
                        "Please provide path to existing Service Account file.")
                .withConfigProperty(PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH);
      }
    }
    if (bServiceAccountJson != null && bServiceAccountJson) {
      if (!Optional.ofNullable(getServiceAccountJson()).isPresent()) {
        collector.addFailure("Service Account JSON can not be empty.",
                        "Please provide Service Account JSON.")
                .withConfigProperty(PROPERTY_NAME_SERVICE_ACCOUNT_JSON);
      }
    }
    return collector.getValidationFailures().size() == 0;
  }

  @Nullable
  public Boolean isServiceAccountJson() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(PROPERTY_SERVICE_ACCOUNT_JSON);
  }

  @Nullable
  public Boolean isServiceAccountFilePath() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null :
            serviceAccountType.equals(PROPERTY_SERVICE_ACCOUNT_FILE_PATH);
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

  public static void assertIsSet(Object propertyValue, String propertyName, String reason) {
    if (propertyValue == null) {
      throw new InvalidConfigPropertyException(
              String.format("Property '%s' must be set, since %s", propertyName, reason), propertyName);
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
    private String oauth2Enabled;
    private String authType;

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

    public Builder setOauth2Enabled(String oauth2Enabled) {
      this.oauth2Enabled = oauth2Enabled;
      return this;
    }

    public Builder setAuthType(String authType) {
      this.authType = authType;
      return this;
    }

    public HTTPSinkConfig build() {
      return new HTTPSinkConfig(this);
    }
  }
}
