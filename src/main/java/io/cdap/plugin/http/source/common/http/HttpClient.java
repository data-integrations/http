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
package io.cdap.plugin.http.source.common.http;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An http client used to get data from given url. It follows the configurations from {@link BaseHttpSourceConfig}
 */
public class HttpClient implements Closeable {
  private final Map<String, String> headers;
  private final BaseHttpSourceConfig config;
  private final StringEntity requestBody;
  private CloseableHttpClient httpClient;

  public HttpClient(BaseHttpSourceConfig config) {
    this.config = config;
    this.headers = config.getHeadersMap();

    String requestBodyString = config.getRequestBody();
    if (requestBodyString != null) {
      requestBody = new StringEntity(requestBodyString, Charsets.UTF_8.toString());
    } else {
      requestBody = null;
    }
  }

  /**
   * Executes HTTP request with parameters configured in plugin config and returns response.
   * Is called to load every page by pagination iterator.
   *
   * @param uri URI of resource
   * @return a response object
   * @throws IOException in case of a problem or the connection was aborted
   */
  public CloseableHttpResponse executeHTTP(String uri) throws IOException {
    // lazy init. So we are able to initialize the class for different checks during validations etc.
    if (httpClient == null) {
      httpClient = createHttpClient();
    }

    HttpEntityEnclosingRequestBase request = new HttpRequest(URI.create(uri), config.getHttpMethod());

    if (requestBody != null) {
      request.setEntity(requestBody);
    }

    return httpClient.execute(request);
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  private CloseableHttpClient createHttpClient() throws IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    httpClientBuilder.setSSLSocketFactory(new SSLConnectionSocketFactoryCreator(config).create());

    // set timeouts
    Long connectTimeoutMillis = TimeUnit.SECONDS.toMillis(config.getConnectTimeout());
    Long readTimeoutMillis = TimeUnit.SECONDS.toMillis(config.getReadTimeout());
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder.setSocketTimeout(readTimeoutMillis.intValue());
    requestBuilder.setConnectTimeout(connectTimeoutMillis.intValue());
    requestBuilder.setConnectionRequestTimeout(connectTimeoutMillis.intValue());
    httpClientBuilder.setDefaultRequestConfig(requestBuilder.build());

    // basic auth
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    if (!Strings.isNullOrEmpty(config.getUsername()) && !Strings.isNullOrEmpty(config.getPassword())) {
      AuthScope authScope = new AuthScope(HttpHost.create(config.getUrl()));
      credentialsProvider.setCredentials(authScope,
                                         new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
    }

    // proxy and proxy auth
    if (!Strings.isNullOrEmpty(config.getProxyUrl())) {
      HttpHost proxyHost = HttpHost.create(config.getProxyUrl());
      if (!Strings.isNullOrEmpty(config.getProxyUsername()) && !Strings.isNullOrEmpty(config.getProxyPassword())) {
        credentialsProvider.setCredentials(new AuthScope(proxyHost),
                                           new UsernamePasswordCredentials(
                                             config.getProxyUsername(), config.getProxyPassword()));
      }
      httpClientBuilder.setProxy(proxyHost);
    }
    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

    ArrayList<Header> clientHeaders = new ArrayList<>();

    String accessToken = null;
    // oAuth2
    if (config.getServiceAccountEnabled()) {
      accessToken = OAuthUtil.getAccessTokenByServiceAccount(HttpClients.createDefault(),
                                                             config.getServiceAccountJson(),
                                                             config.getServiceAccountScope());
    }
    if (config.getOauth2Enabled()) {
      accessToken = OAuthUtil.getAccessTokenByRefreshToken(HttpClients.createDefault(), config.getTokenUrl(),
                                                           config.getClientId(), config.getClientSecret(),
                                                           config.getRefreshToken());
    }

    clientHeaders.add(new BasicHeader("Authorization", "Bearer " + accessToken));
    // set default headers
    if (headers != null) {
      for (Map.Entry<String, String> headerEntry : this.headers.entrySet())  {
        clientHeaders.add(new BasicHeader(headerEntry.getKey(), headerEntry.getValue()));
      }
    }
    httpClientBuilder.setDefaultHeaders(clientHeaders);

    return httpClientBuilder.build();
  }

  /**
   * This class allows us to send body not only in POST/PUT but also in other requests.
   */
  private static class HttpRequest extends HttpEntityEnclosingRequestBase {
    private final String methodName;

    HttpRequest(URI uri, String methodName) {
      super();
      this.setURI(uri);
      this.methodName = methodName;
    }

    @Override
    public String getMethod() {
      return methodName;
    }
  }
}
