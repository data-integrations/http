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
package io.cdap.plugin.http.source.batch;

import com.google.common.base.Strings;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.http.common.http.AuthType;
import io.cdap.plugin.http.common.http.OAuthUtil;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpClient;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Provides all the configurations required for configuring the {@link HttpBatchSource} plugin.
 */
public class HttpBatchSourceConfig extends BaseHttpSourceConfig {
  protected HttpBatchSourceConfig(String referenceName) {
    super(referenceName);
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);
    validateCredentials(failureCollector);
  }

  public void validateCredentials(FailureCollector collector) {
    try {
      if (getAuthType() == AuthType.OAUTH2) {
        validateOAuth2Credentials(collector);
      } else if (getAuthType() == AuthType.BASIC_AUTH) {
        validateBasicAuthCredentials(collector);
      }
    } catch (IOException e) {
      String errorMessage = "Unable to authenticate the given info : " + e.getMessage();
      collector.addFailure(errorMessage, null);
    }
  }

  private void validateOAuth2Credentials(FailureCollector collector) throws IOException {
    if (!containsMacro(PROPERTY_CLIENT_ID) && !containsMacro(PROPERTY_CLIENT_SECRET) &&
      !containsMacro(PROPERTY_TOKEN_URL) && !containsMacro(PROPERTY_REFRESH_TOKEN) &&
      !containsMacro(PROPERTY_PROXY_PASSWORD) && !containsMacro(PROPERTY_PROXY_USERNAME) &&
      !containsMacro(PROPERTY_PROXY_URL)) {
      HttpClientBuilder httpclientBuilder = HttpClients.custom();
      if (!Strings.isNullOrEmpty(getProxyUrl())) {
        HttpHost proxyHost = HttpHost.create(getProxyUrl());
        if (!Strings.isNullOrEmpty(getProxyUsername()) && !Strings.isNullOrEmpty(getProxyPassword())) {
          CredentialsProvider credsProvider = new BasicCredentialsProvider();
          credsProvider.setCredentials(new AuthScope(proxyHost),
            new UsernamePasswordCredentials(getProxyUsername(), getProxyPassword()));
          httpclientBuilder.setDefaultCredentialsProvider(credsProvider);
        }
        httpclientBuilder.setProxy(proxyHost);
      }

      try (CloseableHttpClient closeableHttpClient = httpclientBuilder.build()) {
        OAuthUtil.getAccessTokenByRefreshToken(closeableHttpClient, this);
      } catch (JsonSyntaxException | HttpHostConnectException e) {
        String errorMessage = "Error occurred during credential validation : " + e.getMessage();
        collector.addFailure(errorMessage, null);
      }
    }
  }

  public void validateBasicAuthCredentials(FailureCollector collector) throws IOException {
    try {
      if (!containsMacro(PROPERTY_URL) && !containsMacro(PROPERTY_USERNAME) && !containsMacro(PROPERTY_PASSWORD) &&
        !containsMacro(PROPERTY_PROXY_USERNAME) && !containsMacro(PROPERTY_PROXY_PASSWORD)
        && !containsMacro(PROPERTY_PROXY_URL)) {
        HttpClient httpClient = new HttpClient(this);
        validateBasicAuthResponse(collector, httpClient);
      }
    } catch (HttpHostConnectException e) {
      String errorMessage = "Error occurred during credential validation : " + e.getMessage();
      collector.addFailure(errorMessage, "Please ensure that correct credentials are provided.");
    }
  }

  public void validateBasicAuthResponse(FailureCollector collector, HttpClient httpClient) throws IOException {
    try (CloseableHttpResponse response = httpClient.executeHTTP(getUrl())) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          String errorResponse = EntityUtils.toString(entity, "UTF-8");
          String errorMessage = String.format("Credential validation request failed with Http Status code: '%d', " +
            "Response: '%s'", statusCode, errorResponse);
          collector.addFailure(errorMessage, "Please ensure that correct credentials are provided.");
        }
      }
    }
  }

  private HttpBatchSourceConfig(HttpBatchSourceConfigBuilder builder) {
    super(builder.referenceName);
    this.url = builder.url;
    this.httpMethod = builder.httpMethod;
    this.headers = builder.headers;
    this.format = builder.format;
    this.oauth2Enabled = builder.oauth2Enabled;
    this.errorHandling = builder.errorHandling;
    this.retryPolicy = builder.retryPolicy;
    this.maxRetryDuration = builder.maxRetryDuration;
    this.connectTimeout = builder.connectTimeout;
    this.readTimeout = builder.readTimeout;
    this.paginationType = builder.paginationType;
    this.verifyHttps = builder.verifyHttps;
    this.authType = builder.authType;
    this.authUrl = builder.authUrl;
    this.clientId = builder.clientId;
    this.clientSecret = builder.clientSecret;
    this.username = builder.username;
    this.password = builder.password;
    this.tokenUrl = builder.tokenUrl;
    this.refreshToken = builder.refreshToken;
    this.proxyUrl = builder.proxyUrl;
    this.proxyUsername = builder.proxyUsername;
    this.proxyPassword = builder.proxyPassword;
  }

  public static HttpBatchSourceConfigBuilder builder() {
    return new HttpBatchSourceConfigBuilder();
  }

  /**
   * Builder for HttpBatchSourceConfig
   */
  public static class HttpBatchSourceConfigBuilder {

    private String referenceName;
    private String url;
    private String httpMethod;
    private String headers;
    private String format;
    private String oauth2Enabled;
    private String errorHandling;
    private String retryPolicy;
    private Long maxRetryDuration;
    private Integer connectTimeout;
    private Integer readTimeout;
    private String paginationType;
    private String verifyHttps;
    private String authType;
    private String authUrl;
    private String tokenUrl;
    private String clientId;
    private String clientSecret;
    private String scopes;
    private String refreshToken;
    private String proxyUrl;
    private String proxyUsername;
    private String proxyPassword;
    private String username;
    private String password;


    public HttpBatchSourceConfigBuilder setReferenceName (String referenceName) {
      this.referenceName = referenceName;
      return this;
    }
    public HttpBatchSourceConfigBuilder setAuthUrl(String authUrl) {
      this.authUrl = authUrl;
      return this;
    }

    public HttpBatchSourceConfigBuilder setTokenUrl(String tokenUrl) {
      this.tokenUrl = tokenUrl;
      return this;
    }

    public HttpBatchSourceConfigBuilder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public HttpBatchSourceConfigBuilder setClientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public HttpBatchSourceConfigBuilder setScopes(String scopes) {
      this.scopes = scopes;
      return this;
    }

    public HttpBatchSourceConfigBuilder setRefreshToken(String refreshToken) {
      this.refreshToken = refreshToken;
      return this;
    }

    public HttpBatchSourceConfigBuilder setProxyUrl(String proxyUrl) {
      this.proxyUrl = proxyUrl;
      return this;
    }

    public HttpBatchSourceConfigBuilder setProxyUsername(String proxyUsername) {
      this.proxyUsername = proxyUsername;
      return this;
    }

    public HttpBatchSourceConfigBuilder setProxyPassword(String proxyPassword) {
      this.proxyPassword = proxyPassword;
      return this;
    }

    public HttpBatchSourceConfigBuilder setUsername(String username) {
      this.username = username;
      return this;
    }

    public HttpBatchSourceConfigBuilder setPassword(String password) {
      this.password = password;
      return this;
    }

    public HttpBatchSourceConfigBuilder setUrl(String url) {
      this.url = url;
      return this;
    }

    public HttpBatchSourceConfigBuilder setHttpMethod(String httpMethod) {
      this.httpMethod = httpMethod;
      return this;
    }

    public HttpBatchSourceConfigBuilder setHeaders(String headers) {
      this.headers = headers;
      return this;
    }

    public HttpBatchSourceConfigBuilder setFormat(String format) {
      this.format = format;
      return this;
    }

    public HttpBatchSourceConfigBuilder setOauth2Enabled(String oauth2Enabled) {
      this.oauth2Enabled = oauth2Enabled;
      return this;
    }

    public HttpBatchSourceConfigBuilder setErrorHandling(String errorHandling) {
      this.errorHandling = errorHandling;
      return this;
    }

    public HttpBatchSourceConfigBuilder setRetryPolicy(String retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    public HttpBatchSourceConfigBuilder setMaxRetryDuration(Long maxRetryDuration) {
      this.maxRetryDuration = maxRetryDuration;
      return this;
    }

    public HttpBatchSourceConfigBuilder setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public HttpBatchSourceConfigBuilder setReadTimeout(Integer readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    public HttpBatchSourceConfigBuilder setPaginationType(String paginationType) {
      this.paginationType = paginationType;
      return this;
    }

    public HttpBatchSourceConfigBuilder setVerifyHttps(String verifyHttps) {
      this.verifyHttps = verifyHttps;
      return this;
    }

    public HttpBatchSourceConfigBuilder setAuthType(String authType) {
      this.authType = authType;
      return this;
    }

    public HttpBatchSourceConfig build() {
      return new HttpBatchSourceConfig(this);
    }
  }
}
