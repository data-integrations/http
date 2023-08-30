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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.common.RetryPolicy;
import io.cdap.plugin.http.common.error.HttpErrorHandler;
import io.cdap.plugin.http.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.common.http.OAuthUtil;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.awaitility.pollinterval.FixedPollInterval;
import org.awaitility.pollinterval.IterativePollInterval;
import org.awaitility.pollinterval.PollInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
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
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * RecordWriter for HTTP.
 */
public class HTTPRecordWriter extends RecordWriter<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPRecordWriter.class);
  private static final String REGEX_HASHED_VAR = "#(\\w+)";

  private final HTTPSinkConfig config;
  private final MessageBuffer messageBuffer;
  private String contentType;
  private String url;
  private String configURL;
  private List<PlaceholderBean> placeHolderList;

  private AccessToken accessToken;
  private final HttpErrorHandler httpErrorHandler;
  private final PollInterval pollInterval;
  private int httpStatusCode;

  HTTPRecordWriter(HTTPSinkConfig config, Schema inputSchema) {
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
  public void write(StructuredRecord input, StructuredRecord unused) throws IOException {
    configURL = url;
    if (config.getMethod().equals("POST") || config.getMethod().equals("PUT")) {
      messageBuffer.add(input);
    }
    
    if (config.getMethod().equals("PUT") || config.getMethod().equals("DELETE") && !placeHolderList.isEmpty()) {
      configURL = updateURLWithPlaceholderValue(input);
    }

    if (config.getBatchSize() == messageBuffer.size()) {
      flushMessageBuffer();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    // Process remaining messages after batch executions.
    flushMessageBuffer();
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
      throw new IllegalStateException("Error while installing the trust manager: " + e.getMessage(), e);
    }
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HostnameVerifier allHostsValid = new HostnameVerifier() {
      public boolean verify(String hostname, SSLSession session) {
        return true;
      }
    };
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
  }

  private boolean executeHTTPServiceAndCheckStatusCode() throws IOException {
    HttpURLConnection conn = null;

    Map<String, String> headers = config.getRequestHeadersMap();

    if (accessToken == null || OAuthUtil.tokenExpired(accessToken)) {
      accessToken = OAuthUtil.getAccessToken(config);
    }

    if (accessToken != null) {
      Header authorizationHeader = new BasicHeader("Authorization",
                                                   String.format("Bearer %s", accessToken.getTokenValue()));
      headers.putAll(config.getHeadersMap(String.valueOf(authorizationHeader)));
    }

    try {
      URL url = new URL(configURL);
      conn = (HttpURLConnection) url.openConnection();
      if (conn instanceof HttpsURLConnection) {
        //Disable SSLv3
        System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
        if (config.getDisableSSLValidation()) {
          disableSSLValidation();
        }
      }
      conn.setRequestMethod(config.getMethod().toUpperCase());
      conn.setConnectTimeout(config.getConnectTimeout());
      conn.setReadTimeout(config.getReadTimeout());
      conn.setInstanceFollowRedirects(config.getFollowRedirects());
      conn.addRequestProperty("charset", config.getCharset());
      for (Map.Entry<String, String> propertyEntry : headers.entrySet()) {
        conn.addRequestProperty(propertyEntry.getKey(), propertyEntry.getValue());
      }
      //Default contentType value would be added in the request properties if user has not added in the headers.
      if (config.getMethod().equals("POST") || config.getMethod().equals("PUT")) {
        if (!headers.containsKey("Content-Type")) {
          conn.addRequestProperty("Content-Type", contentType);
        }
      }
      if (!messageBuffer.isEmpty()) {
        conn.setDoOutput(true);
        try (OutputStream outputStream = conn.getOutputStream()) {
          outputStream.write(messageBuffer.getMessage().trim().getBytes(config.getCharset()));
        }
      }
      httpStatusCode = conn.getResponseCode();
    } catch (MalformedURLException | ProtocolException e) {
      throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
    } catch (IOException e) {
      LOG.warn("Error making {} request to url {} with headers {}.", config.getMethod(), config.getUrl(), headers);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    RetryableErrorHandling errorHandlingStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode);
    boolean shouldRetry = errorHandlingStrategy.shouldRetry();
    if (!shouldRetry) {
      messageBuffer.clear();
    }
    return !shouldRetry;
  }

  /**
   * @return List of placeholders which should be replaced by actual value in the URL.
   */
  private List<PlaceholderBean> getPlaceholderListFromURL() {
    List<PlaceholderBean> placeholderList = new ArrayList<>();
    if (!(config.getMethod().equals("PUT") || config.getMethod().equals("DELETE"))) {
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
      throw new IllegalStateException("Error encoding URL with placeholder value. Reason: " + e.getMessage(), e);
    }
  }

  private void flushMessageBuffer() {
    if (messageBuffer.isEmpty()) {
      return;
    }
    contentType = messageBuffer.getContentType();
    try {
      Awaitility
        .await().with()
        .pollInterval(pollInterval)
        .pollDelay(config.getReadTimeout() == null ? 0L : config.getReadTimeout(), TimeUnit.MILLISECONDS)
        .timeout(config.getMaxRetryDuration(), TimeUnit.SECONDS)
        .until(this::executeHTTPServiceAndCheckStatusCode);
    } catch (Exception e) {
      throw new RuntimeException("Error while executing http request for remaining input messages " +
                                   "after the batch execution. " + e);
    }
    messageBuffer.clear();
  }
  
}
