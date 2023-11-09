/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.auth.oauth2.AccessToken;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.http.common.http.HttpClient;
import io.cdap.plugin.http.common.http.OAuthUtil;
import io.cdap.plugin.http.common.pagination.BaseHttpPaginationIterator;
import io.cdap.plugin.http.common.pagination.PaginationIteratorFactory;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Unit tests for HttpBatchSourceConfig
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PaginationIteratorFactory.class, HttpClientBuilder.class, HttpClients.class, OAuthUtil.class,
  HttpHost.class, EntityUtils.class, HttpClient.class})
@PowerMockIgnore("javax.management.*")
public class HttpBatchSourceConfigTest {

  @Mock
  private HttpClient httpClient;

  @Mock
  private CloseableHttpResponse response;

  @Mock
  private StatusLine statusLine;

  @Mock
  private HttpEntity entity;

  @Test(expected = IllegalArgumentException.class)
  public void testMissingKeyValue() {
    FailureCollector collector = new MockFailureCollector();
    HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
      .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:")
      .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
      .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
      .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").build();
    config.validate(collector);
  }

  @Test(expected = InvalidConfigPropertyException.class)
  public void testEmptySchemaKeyValue() {
    HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
      .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:auth")
      .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
      .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
      .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").build();
    config.validateSchema();
  }

  @Test
  public void testValidateOAuth2() throws Exception {
    FailureCollector collector = new MockFailureCollector();
    HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
      .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:auth")
      .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
      .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
      .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").setAuthType("oAuth2").setClientId("id").
      setClientSecret("secret").setRefreshToken("token").setScopes("scope").setTokenUrl("https//:token").setRetryPolicy(
        "exponential").build();
    PowerMockito.mockStatic(PaginationIteratorFactory.class);
    BaseHttpPaginationIterator baseHttpPaginationIterator = Mockito.mock(BaseHttpPaginationIterator.class);
    PowerMockito.when(PaginationIteratorFactory.createInstance(Mockito.any(), Mockito.any()))
      .thenReturn(baseHttpPaginationIterator);
    PowerMockito.when(baseHttpPaginationIterator.supportsSkippingPages()).thenReturn(true);
    PowerMockito.mockStatic(HttpClients.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    Mockito.when(HttpClients.custom()).thenReturn(httpClientBuilder);
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getTokenValue()).thenReturn("1234");
    PowerMockito.mockStatic(OAuthUtil.class);
    Mockito.when(OAuthUtil.getAccessTokenByRefreshToken(Mockito.any(), Mockito.any())).thenReturn(accessToken);
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }


  @Test
  public void testValidateOAuth2CredentialsWithProxy() throws IOException {
    FailureCollector collector = new MockFailureCollector();
    FailureCollector collectorMock = new MockFailureCollector();
    HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
      .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:auth")
      .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
      .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
      .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").setAuthType("oAuth2").setClientId("id").
      setClientSecret("secret").setRefreshToken("token").setScopes("scope").setTokenUrl("https//:token").setRetryPolicy(
        "exponential").setProxyUrl("https://proxy").setProxyUsername("proxyuser").setProxyPassword("proxypassword")
      .build();
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    CredentialsProvider credentialsProvider = Mockito.mock(CredentialsProvider.class);
    HttpHost proxy = PowerMockito.mock(HttpHost.class);
    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    httpClientBuilder.setProxy(proxy);
    PowerMockito.mockStatic(HttpClients.class);
    CloseableHttpClient closeableHttpClient = Mockito.mock(CloseableHttpClient.class);
    Mockito.when(HttpClients.createDefault()).thenReturn(closeableHttpClient);
    Mockito.when(HttpClients.custom()).thenReturn(httpClientBuilder);
    Mockito.when(HttpClients.custom()
      .setDefaultCredentialsProvider(credentialsProvider)
      .setProxy(proxy)
      .build()).thenReturn(closeableHttpClient);
    AccessToken accessToken = Mockito.mock(AccessToken.class);
    Mockito.when(accessToken.getTokenValue()).thenReturn("1234");
    PowerMockito.mockStatic(OAuthUtil.class);
    Mockito.when(OAuthUtil.getAccessTokenByRefreshToken(Mockito.any(), Mockito.any())).thenReturn(accessToken);
    config.validate(collectorMock);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateCredentialsOAuth2WithInvalidAccessTokenRequest() throws Exception {
    FailureCollector collector = new MockFailureCollector();
    HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
      .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:auth")
      .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
      .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
      .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").setAuthType("oAuth2").setClientId("id").
      setClientSecret("secret").setRefreshToken("token").setScopes("scope").setTokenUrl("https//:token").setRetryPolicy(
        "exponential").build();
    CloseableHttpClient httpClientMock = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    Mockito.when(httpClientMock.execute(Mockito.any())).thenReturn(httpResponse);
    HttpEntity entity = Mockito.mock(HttpEntity.class);
    Mockito.when(httpResponse.getEntity()).thenReturn(entity);
    PowerMockito.mockStatic(EntityUtils.class);
    String response = "  <title>Error 404 (Not Found)!!1</title>\n" +
      "  <a href=//www.google.com/><span id=logo aria-label=Google></span></a>\n" +
      "  <p><b>404.</b> <ins>That’s an error.</ins>\n";

    Mockito.when(EntityUtils.toString(entity, "UTF-8")).thenReturn(response);
    PowerMockito.mockStatic(PaginationIteratorFactory.class);
    BaseHttpPaginationIterator baseHttpPaginationIterator = Mockito.mock(BaseHttpPaginationIterator.class);
    PowerMockito.when(PaginationIteratorFactory.createInstance(Mockito.any(), Mockito.any()))
      .thenReturn(baseHttpPaginationIterator);
    PowerMockito.when(baseHttpPaginationIterator.supportsSkippingPages()).thenReturn(true);
    PowerMockito.mockStatic(HttpClients.class);
    HttpClientBuilder httpClientBuilder = Mockito.mock(HttpClientBuilder.class);
    Mockito.when(HttpClients.custom()).thenReturn(httpClientBuilder);
    Mockito.when(httpClientBuilder.build()).thenReturn(httpClientMock);
    try {
      config.validate(collector);
    } catch (IllegalStateException e) {
      Assert.assertEquals(1, collector.getValidationFailures().size());
    }
  }

  @Test
  public void testBasicAuthWithValidResponse() throws IOException {
    FailureCollector failureCollector = new MockFailureCollector();
    HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
      .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:auth")
      .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
      .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
      .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").setAuthType("basicAuth").setUsername(
        "username").setPassword("password").setRetryPolicy(
        "exponential").build();
    Mockito.when(httpClient.executeHTTP(Mockito.any())).thenReturn(response);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    config.validateBasicAuthResponse(failureCollector, httpClient);
    Assert.assertEquals(0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidConfigWithInvalidResponse() throws IOException {
    FailureCollector failureCollector = new MockFailureCollector();
    HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
      .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:auth")
      .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
      .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
      .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").setAuthType("basicAuth").setUsername(
        "username").setPassword("password").setRetryPolicy(
        "exponential").build();
    Mockito.when(httpClient.executeHTTP(Mockito.any())).thenReturn(response);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(statusLine.getStatusCode()).thenReturn(400);
    Mockito.when(response.getEntity()).thenReturn(entity);
    config.validateBasicAuthResponse(failureCollector, httpClient);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Credential validation request failed with Http Status code: '400', Response: 'null'",
      failureCollector
      .getValidationFailures().get(0).getMessage());
  }

}
