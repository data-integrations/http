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
import io.cdap.plugin.http.common.http.OAuthUtil;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;

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
  private final HTTPSinkConfig config;
  private final MessageBuffer messageBuffer;
  private String contentType;
  private AccessToken accessToken;

  HTTPRecordWriter(HTTPSinkConfig config, Schema inputSchema) {
    this.config = config;
    this.accessToken = null;
    this.messageBuffer = new MessageBuffer(
            config.getMessageFormat(), config.getJsonBatchKey(), config.shouldWriteJsonAsArray(),
            config.getDelimiterForMessages(), config.getCharset(), config.getBody(), inputSchema
    );
  }

  @Override
  public void write(StructuredRecord input, StructuredRecord unused) throws IOException {
    if (config.getMethod().equals("POST") || config.getMethod().equals("PUT")) {
      messageBuffer.add(input);
    }
    if (config.getBatchSize() == messageBuffer.size()) {
      flushMessageBuffer();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    // Process remaining messages after batch executions.
    flushMessageBuffer();
  }

  private void executeHTTPService() throws IOException {
    int responseCode;
    int retries = 0;
    IOException exception = null;
    do {
      exception = null;
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
        URL url = new URL(config.getUrl());
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
        responseCode = conn.getResponseCode();
        messageBuffer.clear();
        if (config.getFailOnNon200Response() && !(responseCode >= 200 && responseCode < 300)) {
          exception = new IOException("Received error response. Response code: " + responseCode);
        }
        break;
      } catch (MalformedURLException | ProtocolException e) {
        throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
      } catch (IOException e) {
        LOG.warn("Error making {} request to url {} with headers {}.", config.getMethod(), config.getMethod(), headers);
        exception = e;
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      retries++;
    } while (retries < config.getNumRetries());
    if (exception != null) {
      throw exception;
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

  private void flushMessageBuffer() throws IOException {
    if (messageBuffer.isEmpty()) {
      return;
    }
    contentType = messageBuffer.getContentType();
    executeHTTPService();
    messageBuffer.clear();
  }

}
