/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.plugin.http.action;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.http.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.common.RetryPolicy;
import io.cdap.plugin.http.common.error.ErrorHandling;
import io.cdap.plugin.http.common.error.HttpErrorHandlerEntity;
import io.cdap.plugin.http.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.common.http.HttpConstants;
import io.cdap.plugin.http.common.http.IHttpConfig;
import io.cdap.plugin.http.common.http.KeyStoreType;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.annotation.Nullable;

/**
 * Http Action plugin Config
 */
public class HttpActionConfig extends PluginConfig implements IHttpConfig {

  @Name(HttpConstants.PROPERTY_URL)
  @Description("Url to fetch to the first page. The url must start with a protocol (e.g. http://).")
  @Macro
  protected String url;

  @Name(HttpConstants.PROPERTY_HTTP_METHOD)
  @Description("HTTP request method.")
  @Macro
  protected String httpMethod;

  @Name(HttpConstants.PROPERTY_HEADERS)
  @Nullable
  @Description("Headers to send with each HTTP request.")
  @Macro
  protected String headers;

  @Nullable
  @Name(HttpConstants.PROPERTY_REQUEST_BODY)
  @Description("Body to send with each HTTP request.")
  @Macro
  protected String requestBody;

  @Nullable
  @Name(HttpConstants.PROPERTY_USERNAME)
  @Description("Username for basic authentication.")
  @Macro
  protected String username;

  @Nullable
  @Name(HttpConstants.PROPERTY_PASSWORD)
  @Description("Password for basic authentication.")
  @Macro
  protected String password;

  @Nullable
  @Name(HttpConstants.PROPERTY_PROXY_URL)
  @Description("Proxy URL. Must contain a protocol, address and port.")
  @Macro
  protected String proxyUrl;

  @Nullable
  @Name(HttpConstants.PROPERTY_PROXY_USERNAME)
  @Description("Proxy username.")
  @Macro
  protected String proxyUsername;

  @Nullable
  @Name(HttpConstants.PROPERTY_PROXY_PASSWORD)
  @Description("Proxy password.")
  @Macro
  protected String proxyPassword;

  @Nullable
  @Name(HttpConstants.PROPERTY_HTTP_ERROR_HANDLING)
  @Description("Defines the error handling strategy to use for certain HTTP response codes." +
    "The left column contains a regular expression for HTTP status code. The right column contains an action which" +
    "is done in case of match. If HTTP status code matches multiple regular expressions, " +
    "the first specified in mapping is matched.")
  protected String httpErrorsHandling;

  @Name(HttpConstants.PROPERTY_ERROR_HANDLING)
  @Description("Error handling strategy to use when the HTTP response cannot be transformed to an output record.")
  protected String errorHandling;


  @Name(HttpConstants.PROPERTY_CONNECT_TIMEOUT)
  @Description("Maximum time in seconds connection initialization is allowed to take.")
  @Macro
  protected Integer connectTimeout;

  @Name(HttpConstants.PROPERTY_READ_TIMEOUT)
  @Description("Maximum time in seconds fetching data from the server is allowed to take.")
  @Macro
  protected Integer readTimeout;

  @Name(HttpConstants.PROPERTY_OAUTH2_ENABLED)
  @Description("If true, plugin will perform OAuth2 authentication.")
  protected String oauth2Enabled;

  @Nullable
  @Name(HttpConstants.PROPERTY_AUTH_URL)
  @Description("Endpoint for the authorization server used to retrieve the authorization code.")
  @Macro
  protected String authUrl;

  @Nullable
  @Name(HttpConstants.PROPERTY_TOKEN_URL)
  @Description("Endpoint for the resource server, which exchanges the authorization code for an access token.")
  @Macro
  protected String tokenUrl;

  @Nullable
  @Name(HttpConstants.PROPERTY_CLIENT_ID)
  @Description("Client identifier obtained during the Application registration process.")
  @Macro
  protected String clientId;

  @Nullable
  @Name(HttpConstants.PROPERTY_CLIENT_SECRET)
  @Description("Client secret obtained during the Application registration process.")
  @Macro
  protected String clientSecret;

  @Nullable
  @Name(HttpConstants.PROPERTY_SCOPES)
  @Description("Scope of the access request, which might have multiple space-separated values.")
  @Macro
  protected String scopes;

  @Nullable
  @Name(HttpConstants.PROPERTY_REFRESH_TOKEN)
  @Description("Token used to receive accessToken, which is end product of OAuth2.")
  @Macro
  protected String refreshToken;

  @Name(HttpConstants.PROPERTY_VERIFY_HTTPS)
  @Description("If false, untrusted trust certificates (e.g. self signed), will not lead to an" +
    "error. Do not disable this in production environment on a network you do not entirely trust. " +
    "Especially public internet.")
  @Macro
  protected String verifyHttps;

  @Nullable
  @Name(HttpConstants.PROPERTY_KEYSTORE_FILE)
  @Description("A path to a file which contains keystore.")
  @Macro
  protected String keystoreFile;

  @Nullable
  @Name(HttpConstants.PROPERTY_KEYSTORE_TYPE)
  @Description("Format of a keystore.")
  @Macro
  protected String keystoreType;

  @Nullable
  @Name(HttpConstants.PROPERTY_KEYSTORE_PASSWORD)
  @Description("Password for a keystore. If a keystore is not password protected leave it empty.")
  @Macro
  protected String keystorePassword;

  @Nullable
  @Name(HttpConstants.PROPERTY_KEYSTORE_KEY_ALGORITHM)
  @Description("An algorithm used for keystore.")
  @Macro
  protected String keystoreKeyAlgorithm;

  @Nullable
  @Name(HttpConstants.PROPERTY_TRUSTSTORE_FILE)
  @Description("A path to a file which contains truststore.")
  @Macro
  protected String trustStoreFile;

  @Nullable
  @Name(HttpConstants.PROPERTY_TRUSTSTORE_TYPE)
  @Description("Format of a truststore.")
  @Macro
  protected String trustStoreType;

  @Nullable
  @Name(HttpConstants.PROPERTY_TRUSTSTORE_PASSWORD)
  @Description("Password for a truststore. If a truststore is not password protected leave it empty.")
  @Macro
  protected String trustStorePassword;

  @Nullable
  @Name(HttpConstants.PROPERTY_TRUSTSTORE_KEY_ALGORITHM)
  @Description("An algorithm used for truststore.")
  @Macro
  protected String trustStoreKeyAlgorithm;

  @Nullable
  @Name(HttpConstants.PROPERTY_TRANSPORT_PROTOCOLS)
  @Description("Transport protocols which are allowed for connection.")
  @Macro
  protected String transportProtocols;

  @Nullable
  @Name(HttpConstants.PROPERTY_CIPHER_SUITES)
  @Description("Cipher suites which are allowed for connection. " +
    "Colons, commas or spaces are also acceptable separators.")
  @Macro
  protected String cipherSuites;

  @Name(HttpConstants.PROPERTY_KEYSTORE_CERT_ALIAS)
  @Macro
  @Nullable
  @Description("Alias of the key in the keystore to be used for communication")
  protected String keystoreCertAliasName;

  @Override
  public String getUrl() {
    return url;
  }

  @Override
  public String getHttpMethod() {
    return httpMethod;
  }

  @Override
  @Nullable
  public String getHeaders() {
    return headers;
  }

  @Override
  @Nullable
  public String getRequestBody() {
    return requestBody;
  }

  @Override
  @Nullable
  public String getUsername() {
    return username;
  }

  @Override
  @Nullable
  public String getPassword() {
    return password;
  }

  @Override
  @Nullable
  public String getProxyUrl() {
    return proxyUrl;
  }

  @Override
  @Nullable
  public String getProxyUsername() {
    return proxyUsername;
  }

  @Override
  @Nullable
  public String getProxyPassword() {
    return proxyPassword;
  }

  @Override
  public ErrorHandling getErrorHandling() {
    return BaseHttpSourceConfig.getEnumValueByString(
      ErrorHandling.class,
      errorHandling,
      HttpConstants.PROPERTY_ERROR_HANDLING
    );
  }

  @Override
  public List<HttpErrorHandlerEntity> getHttpErrorHandlingEntries() {
    Map<String, String> httpErrorsHandlingMap = BaseHttpSourceConfig.getMapFromKeyValueString(httpErrorsHandling);
    List<HttpErrorHandlerEntity> results = new ArrayList<>(httpErrorsHandlingMap.size());

    for (Map.Entry<String, String> entry : httpErrorsHandlingMap.entrySet()) {
      String regex = entry.getKey();
      try {
        results.add(new HttpErrorHandlerEntity(Pattern.compile(regex),
                BaseHttpSourceConfig.getEnumValueByString(RetryableErrorHandling.class,
                        entry.getValue(), HttpConstants.PROPERTY_HTTP_ERROR_HANDLING)));
      } catch (PatternSyntaxException e) {
        // We embed causing exception message into this one. Since this message is shown on UI when validation fails.
        throw new InvalidConfigPropertyException(
                String.format("Error handling regex '%s' is not valid. %s", regex, e.getMessage()),
                HttpConstants.PROPERTY_HTTP_ERROR_HANDLING
        );
      }
    }
    return results;
  }

  @Override
  public Long getLinearRetryInterval() {
    return 0L;
  }

  @Override
  public Long getMaxRetryDuration() {
    return 1L;
  }

  @Override
  public RetryPolicy getRetryPolicy() {
    return RetryPolicy.LINEAR;
  }

  @Override
  public Integer getConnectTimeout() {
    return connectTimeout;
  }

  @Override
  public Integer getReadTimeout() {
    return 30;
  }

  @Override
  public Boolean getOauth2Enabled() {
    return Boolean.parseBoolean(oauth2Enabled);
  }

  @Override
  @Nullable
  public String getAuthUrl() {
    return authUrl;
  }

  @Override
  @Nullable
  public String getTokenUrl() {
    return tokenUrl;
  }

  @Override
  @Nullable
  public String getClientId() {
    return clientId;
  }

  @Override
  @Nullable
  public String getClientSecret() {
    return clientSecret;
  }

  @Override
  @Nullable
  public String getRefreshToken() {
    return refreshToken;
  }

  @Override
  public Boolean getVerifyHttps() {
    return Boolean.parseBoolean(verifyHttps);
  }

  @Override
  @Nullable
  public String getKeystoreFile() {
    return keystoreFile;
  }

  @Override
  @Nullable
  public KeyStoreType getKeystoreType() {
    return BaseHttpSourceConfig.getEnumValueByString(
      KeyStoreType.class,
      keystoreType,
      HttpConstants.PROPERTY_KEYSTORE_TYPE
    );
  }

  @Override
  @Nullable
  public String getKeystorePassword() {
    return keystorePassword;
  }

  @Override
  @Nullable
  public String getKeystoreKeyAlgorithm() {
    return keystoreKeyAlgorithm;
  }

  @Override
  @Nullable
  public String getTrustStoreFile() {
    return trustStoreFile;
  }

  @Override
  @Nullable
  public KeyStoreType getTrustStoreType() {
    return BaseHttpSourceConfig.getEnumValueByString(
      KeyStoreType.class,
      trustStoreType,
      HttpConstants.PROPERTY_TRUSTSTORE_TYPE
    );
  }

  @Override
  @Nullable
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  @Override
  @Nullable
  public String getTrustStoreKeyAlgorithm() {
    return trustStoreKeyAlgorithm;
  }

  @Override
  @Nullable
  public String getCipherSuites() {
    return cipherSuites;
  }

  @Override
  @Nullable
  public String getKeystoreCertAliasName() {
    return keystoreCertAliasName;
  }

  @Override
  @Nullable
  public Map<String, String> getHeadersMap() {
    return BaseHttpSourceConfig.getMapFromKeyValueString(headers);
  }

  @Override
  public List<String> getTransportProtocolsList() {
    return BaseHttpSourceConfig.getListFromString(transportProtocols);
  }

  @Override
  public void validate() {
    // Validate URL
    if (!containsMacro(HttpConstants.PROPERTY_URL)) {
      try {
        // replace with placeholder with anything just during pagination
        new URI(getUrl());
      } catch (URISyntaxException e) {
        throw new InvalidConfigPropertyException(
          String.format("URL value is not valid: '%s'", getUrl()), e, HttpConstants.PROPERTY_URL);
      }
    }

    // Validate OAuth2 properties
    if (!containsMacro(HttpConstants.PROPERTY_OAUTH2_ENABLED) && this.getOauth2Enabled()) {
      String reasonOauth2 = "OAuth2 is enabled";
      BaseHttpSourceConfig.assertIsSet(getAuthUrl(), HttpConstants.PROPERTY_AUTH_URL, reasonOauth2);
      BaseHttpSourceConfig.assertIsSet(getTokenUrl(), HttpConstants.PROPERTY_TOKEN_URL, reasonOauth2);
      BaseHttpSourceConfig.assertIsSet(getClientId(), HttpConstants.PROPERTY_CLIENT_ID, reasonOauth2);
      BaseHttpSourceConfig.assertIsSet(getClientSecret(), HttpConstants.PROPERTY_CLIENT_SECRET, reasonOauth2);
      BaseHttpSourceConfig.assertIsSet(getRefreshToken(), HttpConstants.PROPERTY_REFRESH_TOKEN, reasonOauth2);
    }

    if (!containsMacro(HttpConstants.PROPERTY_VERIFY_HTTPS) && !getVerifyHttps()) {
      BaseHttpSourceConfig.assertIsNotSet(
        getTrustStoreFile(),
        HttpConstants.PROPERTY_TRUSTSTORE_FILE,
        String.format("trustore settings are ignored due to disabled %s", HttpConstants.PROPERTY_VERIFY_HTTPS)
      );
    }
  }

}
