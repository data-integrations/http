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
package io.cdap.plugin.http.common.http;

import com.google.auth.oauth2.AccessToken;
import com.google.common.annotations.VisibleForTesting;
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

  private AccessToken accessToken;

  public HttpClient(BaseHttpSourceConfig config) {
    this.config = config;
    this.headers = config.getHeadersMap();
    this.accessToken = null;

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
      httpClient = createHttpClient(uri);
    }

    HttpEntityEnclosingRequestBase request = new HttpRequest(URI.create(uri), config.getHttpMethod());

    if (requestBody != null) {
      request.setEntity(requestBody);
    }

    // Set the Request Headers(along with Authorization Header) in the HttpRequest
    request.setHeaders(getRequestHeaders());

    return httpClient.execute(request);
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  @VisibleForTesting
  public CloseableHttpClient createHttpClient(String pageUriStr) throws IOException {
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
      URI uri = URI.create(pageUriStr);
      AuthScope authScope = new AuthScope(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()));
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

    return httpClientBuilder.build();
  }

  private Header[] getRequestHeaders() throws IOException {
    ArrayList<Header> clientHeaders = new ArrayList<>();

    if (accessToken == null || OAuthUtil.tokenExpired(accessToken)) {
      accessToken = OAuthUtil.getAccessToken(config);
    }

    if (accessToken != null) {
      Header authorizationHeader = getAuthorizationHeader(accessToken);
      if (authorizationHeader != null) {
        clientHeaders.add(authorizationHeader);
      }
    }

    // set default headers
    if (headers != null) {
      for (Map.Entry<String, String> headerEntry : this.headers.entrySet())  {
        clientHeaders.add(new BasicHeader(headerEntry.getKey(), headerEntry.getValue()));
      }
    }

    return clientHeaders.toArray(new Header[0]);
  }

  private Header getAuthorizationHeader(AccessToken accessToken) {
    return new BasicHeader("Authorization", String.format("Bearer %s", accessToken.getTokenValue()));
  }

  /**
   * This class allows us to send body not only in POST/PUT but also in other requests.
   */
  public static class HttpRequest extends HttpEntityEnclosingRequestBase {
    private final String methodName;

    public HttpRequest(URI uri, String methodName) {
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
