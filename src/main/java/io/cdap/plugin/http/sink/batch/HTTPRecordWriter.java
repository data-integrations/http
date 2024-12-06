/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.auth.oauth2.AccessToken;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.plugin.http.common.HttpErrorDetailsProvider;
import io.cdap.plugin.http.common.RetryPolicy;
import io.cdap.plugin.http.common.error.ErrorHandling;
import io.cdap.plugin.http.common.error.HttpErrorHandler;
import io.cdap.plugin.http.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.common.http.HttpRequest;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.common.http.OAuthUtil;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.awaitility.pollinterval.FixedPollInterval;
import org.awaitility.pollinterval.IterativePollInterval;
import org.awaitility.pollinterval.PollInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * RecordWriter for HTTP.
 */
public class HTTPRecordWriter extends RecordWriter<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPRecordWriter.class);
  private static final String REGEX_HASHED_VAR = "#(\\w+)";
  public static final String REQUEST_METHOD_POST = "POST";
  public static final String REQUEST_METHOD_PUT = "PUT";
  public static final String REQUEST_METHOD_DELETE = "DELETE";
  public static final String REQUEST_METHOD_PATCH = "PATCH";

  private final HTTPSinkConfig config;
  private final MessageBuffer messageBuffer;
  private String contentType;
  private final String url;
  private String configURL;
  private final List<PlaceholderBean> placeHolderList;
  private final Map<String, String> headers;

  private AccessToken accessToken;
  private final HttpErrorHandler httpErrorHandler;
  private final PollInterval pollInterval;
  private int httpStatusCode;
  private String httpResponseBody;
  private static int retryCount;

  HTTPRecordWriter(HTTPSinkConfig config, Schema inputSchema) {
    this.headers = config.getRequestHeadersMap();
    this.config = config;
    this.accessToken = null;
    this.messageBuffer = new MessageBuffer(
      config.getMessageFormat(), config.getJsonBatchKey(), config.shouldWriteJsonAsArray(),
      config.getDelimiterForMessages(), config.getCharset(), config.getBody(), inputSchema
    );
    this.httpErrorHandler = new HttpErrorHandler(config);
    if (config.getRetryPolicy().equals(RetryPolicy.LINEAR)) {
      pollInterval = FixedPollInterval.fixed(config.getLinearRetryInterval(), TimeUnit.SECONDS);
    } else {
      pollInterval = IterativePollInterval.iterative(duration -> duration.multiply(2),
                                                     Duration.FIVE_HUNDRED_MILLISECONDS);
    }
    url = config.getUrl();
    placeHolderList = getPlaceholderListFromURL();
  }

  @Override
  public void write(StructuredRecord input, StructuredRecord unused) {
    configURL = url;
    if (config.getMethod().equals(REQUEST_METHOD_POST) || config.getMethod().equals(REQUEST_METHOD_PUT) ||
      config.getMethod().equals(REQUEST_METHOD_PATCH)) {
      messageBuffer.add(input);
    }

    if (config.getMethod().equals(REQUEST_METHOD_PUT) || config.getMethod().equals(REQUEST_METHOD_PATCH) ||
      config.getMethod().equals(REQUEST_METHOD_DELETE)
      && !placeHolderList.isEmpty()) {
      configURL = updateURLWithPlaceholderValue(input);
    }

    if (config.getBatchSize() == messageBuffer.size() || config.getMethod().equals(REQUEST_METHOD_DELETE)) {
      flushMessageBuffer();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    // Process remaining messages after batch executions.
    if (!config.getMethod().equals(REQUEST_METHOD_DELETE)) {
      flushMessageBuffer();
    }
  }

  private void disableSSLValidation() {
    TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      public void checkClientTrusted(X509Certificate[] certs, String authType) {
      }

      public void checkServerTrusted(X509Certificate[] certs, String authType) {
      }
    }
    };
    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      throw new IllegalStateException(
        String.format("Failed while installing the trust manager with message: %s", e.getMessage()), e);
    }
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HostnameVerifier allHostsValid = (hostname, session) -> true;
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
  }

  private boolean executeHTTPServiceAndCheckStatusCode() {
    LOG.debug("HTTP Request Attempt No. : {}", ++retryCount);
    // Try-with-resources ensures proper resource management
    try (CloseableHttpClient httpClient = createHttpClient(configURL);
         CloseableHttpResponse response = executeHttpRequest(httpClient, new URL(configURL))) {
      httpStatusCode = response.getStatusLine().getStatusCode();
      httpResponseBody = new HttpResponse(response).getBody();
      RetryableErrorHandling errorHandlingStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode);
      boolean shouldRetry = errorHandlingStrategy.shouldRetry();
      if (!shouldRetry) {
        messageBuffer.clear();
        retryCount = 0;
      }
      return !shouldRetry;
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Invalid URL: " + configURL, e);
    } catch (IOException e) {
      LOG.warn("Error making {} request to URL {}.", config.getMethod(), config.getUrl());
      String errorMessage = String.format(
          "Failed to execute %s request to %s with error code %s with message: %s. ",
          config.getMethod(), config.getUrl(), httpStatusCode, e.getMessage());
      throw getProgramFailureException(errorMessage,
          "Unable to write record and error clearing message buffer: %s. %s. "
              + "For more details, see %s.", e);
    }
  }

  private CloseableHttpResponse executeHttpRequest(CloseableHttpClient httpClient, URL url) {
    try {
      HttpEntityEnclosingRequestBase request = new HttpRequest(URI.create(url.toString()), config.getMethod());
      if ("https".equalsIgnoreCase(url.getProtocol())) {
        configureHttpsSettings();
      }
      if (!messageBuffer.isEmpty()) {
        String requestBodyString = messageBuffer.getMessage();
        if (requestBodyString != null) {
          StringEntity requestBody = new StringEntity(requestBodyString, Charsets.UTF_8.toString());
          request.setEntity(requestBody);
        }
      }

      request.setHeaders(getRequestHeaders());

      // Execute the request and return the response
      return httpClient.execute(request);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Error encoding the request Reason: " + e.getMessage(), e);
    } catch (IOException e) {
      throw getProgramFailureException(String.format("Unable to execute HTTP request to URL: %s. "
              + "Failed to write record and error clearing message buffer with error code %s with "
              + "message: %s.", url, httpStatusCode, e.getMessage()),
          "Failed to write record and error clearing message buffer: %s. %s. For more details, "
              + "see %s", e);
    } catch (Exception e) {
      throw getProgramFailureException(String.format(
              "Unexpected error occurred, unable to write record and error "
                  + "clearing message buffer. Failed to execute HTTP request to %s with error code %s "
                  + "with message: %s.", url, httpStatusCode, e.getMessage()),
          "Unexpected error occurred, unable to write record: %s. %s. For more details, "
              + "see %s.", e);
    }
  }

  private void configureHttpsSettings() {
    System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
    if (Boolean.TRUE.equals(config.getDisableSSLValidation())) {
      disableSSLValidation();
    }
  }

  public CloseableHttpClient createHttpClient(String pageUriStr) {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    // set timeouts
    long connectTimeoutMillis = TimeUnit.SECONDS.toMillis(config.getConnectTimeout());
    long readTimeoutMillis = TimeUnit.SECONDS.toMillis(config.getReadTimeout());
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder.setSocketTimeout((int) readTimeoutMillis);
    requestBuilder.setConnectTimeout((int) connectTimeoutMillis);
    requestBuilder.setConnectionRequestTimeout((int) connectTimeoutMillis);
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

  private Header[] getRequestHeaders() {
    ArrayList<Header> clientHeaders = new ArrayList<>();

    if (accessToken == null || OAuthUtil.tokenExpired(accessToken)) {
      try {
        accessToken = OAuthUtil.getAccessToken(config);
      } catch (IOException e) {
        String errorReason = String.format("Failed to get access token with message: %s", e.getMessage());
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          errorReason, errorReason, ErrorType.SYSTEM, true, e);
      }
    }

    if (accessToken != null) {
      Header authorizationHeader = getAuthorizationHeader(accessToken);
      clientHeaders.add(authorizationHeader);
    }

    headers.put("Request-Method", config.getMethod().toUpperCase());
    headers.put("Instance-Follow-Redirects", String.valueOf(config.getFollowRedirects()));
    headers.put("charset", config.getCharset());

    if ((config.getMethod().equals(REQUEST_METHOD_POST)
      || config.getMethod().equals(REQUEST_METHOD_PATCH)
      || config.getMethod().equals(REQUEST_METHOD_PUT)) && !headers.containsKey("Content-Type")) {
      headers.put("Content-Type", contentType);
    }

    // set default headers
    if (headers != null) {
      for (Map.Entry<String, String> headerEntry : this.headers.entrySet()) {
        clientHeaders.add(new BasicHeader(headerEntry.getKey(), headerEntry.getValue()));
      }
    }

    return clientHeaders.toArray(new Header[clientHeaders.size()]);
  }

  private Header getAuthorizationHeader(AccessToken accessToken) {
    return new BasicHeader("Authorization", String.format("Bearer %s", accessToken.getTokenValue()));
  }

  /**
   * @return List of placeholders which should be replaced by actual value in the URL.
   */
  private List<PlaceholderBean> getPlaceholderListFromURL() {
    List<PlaceholderBean> placeholderList = new ArrayList<>();
    if (!(config.getMethod().equals(REQUEST_METHOD_PUT) || config.getMethod().equals(REQUEST_METHOD_PATCH) ||
      config.getMethod().equals(REQUEST_METHOD_DELETE))) {
      return placeholderList;
    }
    Pattern pattern = Pattern.compile(REGEX_HASHED_VAR);
    Matcher matcher = pattern.matcher(url);
    while (matcher.find()) {
      placeholderList.add(new PlaceholderBean(url, matcher.group(1)));
    }
    return placeholderList; // Return blank list if no match found
  }

  private String updateURLWithPlaceholderValue(StructuredRecord inputRecord) {
    try {
      StringBuilder finalURLBuilder = new StringBuilder(url);
      //Running a loop backwards so that it does not impact the start and end index for next record.
      for (int i = placeHolderList.size() - 1; i >= 0; i--) {
        PlaceholderBean key = placeHolderList.get(i);
        String replacement = inputRecord.get(key.getPlaceHolderKey());
        if (replacement != null) {
          String encodedReplacement = URLEncoder.encode(replacement, config.getCharset());
          finalURLBuilder.replace(key.getStartIndex(), key.getEndIndex(), encodedReplacement);
        }
      }
      return finalURLBuilder.toString();
    } catch (UnsupportedEncodingException e) {
      String errorReason = String.format("Failed to encode URL with placeholder value with message: %s",
        e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorReason, ErrorType.USER, false, e);
    }
  }

  /**
   * Clears the message buffer if it is empty and the HTTP method is not 'DELETE'.
   */
  private void flushMessageBuffer() {
    if (messageBuffer.isEmpty() && !config.getMethod().equals(REQUEST_METHOD_DELETE)) {
      return;
    }
    contentType = messageBuffer.getContentType();
      Awaitility
        .await().with()
        .pollInterval(pollInterval)
        .pollDelay(config.getWaitTimeBetweenPages(), TimeUnit.MILLISECONDS)
        .timeout(config.getMaxRetryDuration(), TimeUnit.SECONDS)
        .until(this::executeHTTPServiceAndCheckStatusCode);
    messageBuffer.clear();

    ErrorHandling postRetryStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode)
      .getAfterRetryStrategy();

    switch (postRetryStrategy) {
      case SUCCESS:
        break;
      case STOP:
        throw getProgramFailureException(String.format(
                "Retry failed! Unable to write and execute request. "
                    + "Fetching from '%s' returned http error status code '%d' with response '%s'.",
                config.getUrl(), httpStatusCode, httpResponseBody),
            "Unable to write and execute request: %s. %s. For more details, see %s.",
            null);
      case SKIP:
      case SEND:
        LOG.warn(String.format("Fetching from url '%s' returned status code '%d' and body '%s'",
                               config.getUrl(), httpStatusCode, httpResponseBody));
        break;
      default:
        throw new IllegalArgumentException(String.format("Unexpected http error handling: '%s'", postRetryStrategy));
    }
  }

  /**
   * Return program failure exception
   *
   * @param errorMessage
   * @param errorInfo
   * @param e
   * @return
   */
  private ProgramFailureException getProgramFailureException(String errorMessage, String errorInfo,
      Exception e) {
    ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(httpStatusCode);
    String errorReason = String.format(errorInfo, httpStatusCode, pair.getCorrectiveAction(),
        HttpErrorDetailsProvider.getSupportedDocumentUrl());
    return ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason, errorMessage,
        pair.getErrorType(), true, ErrorCodeType.HTTP, String.valueOf(httpStatusCode),
        HttpErrorDetailsProvider.getSupportedDocumentUrl(), e);
  }
}
