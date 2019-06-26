/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.plugin.http.batch;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.HttpMethod;

/**
 * Sink plugin to send the messages from the pipeline to an external http endpoint.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HTTP")
@Description("Sink plugin to send the messages from the pipeline to an external http endpoint.")
public class HTTPSink extends ReferenceBatchSink<StructuredRecord, Void, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);
  private static final String KV_DELIMITER = ":";
  private static final String DELIMITER = "\n";
  private static final String REGEX_HASHED_VAR = "#s*(\\w+)";
  private static final Set<String> METHODS = ImmutableSet.of(HttpMethod.GET, HttpMethod.POST,
                                                             HttpMethod.PUT, HttpMethod.DELETE);
  private static StringBuilder messages = new StringBuilder("");
  private String contentType;
  private HTTPSinkConfig config;

  public HTTPSink(HTTPSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(config.referenceName, new HTTPSink.HTTPOutputFormatProvider()));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
    config.validate();
    String message = null;
    if (config.method.equals("POST") || config.method.equals("PUT")) {
      if (config.messageFormat.equals("JSON")) {
        message = StructuredRecordStringConverter.toJsonString(input);
        contentType = "application/json";
      } else if (config.messageFormat.equals("Form")) {
        message = createFormMessage(input);
        contentType = " application/x-www-form-urlencoded";
      } else if (config.messageFormat.equals("Custom")) {
        message = createCustomMessage(config.body, input);
        contentType = " text/plain";
      }
      messages.append(message).append(config.delimiterForMessages);
    }
    StringTokenizer tokens = new StringTokenizer(messages.toString().trim(), config.delimiterForMessages);
    if (config.batchSize == 1 || tokens.countTokens() == config.batchSize) {
      executeHTTPService();
    }
  }

  @Override
  public void destroy() {
    // Process remaining messages after batch executions.
    if (!messages.toString().isEmpty()) {
      try {
        executeHTTPService();
      } catch (Exception e) {
        throw new RuntimeException("Error while executing http request for remaining input messages " +
                                     "after the batch execution. " + e);
      }
    }
  }

  private void executeHTTPService() throws Exception {
    int responseCode;
    int retries = 0;
    Exception exception = null;
    do {
      HttpURLConnection conn = null;
      Map<String, String> headers = config.getRequestHeadersMap();
      try {
        URL url = new URL(config.url);
        conn = (HttpURLConnection) url.openConnection();
        if (conn instanceof HttpsURLConnection) {
          //Disable SSLv3
          System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
          if (config.disableSSLValidation) {
            disableSSLValidation();
          }
        }
        conn.setRequestMethod(config.method.toUpperCase());
        conn.setConnectTimeout(config.connectTimeout);
        conn.setReadTimeout(config.readTimeout);
        conn.setInstanceFollowRedirects(config.followRedirects);
        conn.addRequestProperty("charset", config.charset);
        for (Map.Entry<String, String> propertyEntry : headers.entrySet()) {
          conn.addRequestProperty(propertyEntry.getKey(), propertyEntry.getValue());
        }
        //Default contentType value would be added in the request properties if user has not added in the headers.
        if (config.method.equals("POST") || config.method.equals("PUT")) {
          if (!headers.containsKey("Content-Type")) {
            conn.addRequestProperty("Content-Type", contentType);
          }
        }
        if (messages.length() > 0) {
          conn.setDoOutput(true);
          try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(messages.toString().trim().getBytes(config.charset));
          }
        }
        responseCode = conn.getResponseCode();
        messages.setLength(0);
        if (config.failOnNon200Response && !(responseCode >= 200 && responseCode < 300)) {
          exception = new IllegalStateException("Received error response. Response code: " + responseCode);
        }
        break;
      } catch (MalformedURLException | ProtocolException e) {
        throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
      } catch (Exception e) {
        LOG.warn("Error making {} request to url {} with headers {}.", config.method, config.url, headers);
        exception = e;
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      retries++;
    } while (retries < config.numRetries);
    if (exception != null) {
      throw exception;
    }
  }

  private String createFormMessage(StructuredRecord input) {
    boolean first = true;
    String formMessage = null;
    StringBuilder sb = new StringBuilder("");
    for (Schema.Field field : input.getSchema().getFields()) {
      if (first) {
        first = false;
      } else {
        sb.append("&");
      }
      sb.append(field.getName());
      sb.append("=");
      sb.append((String) input.get(field.getName()));
    }
    try {
      formMessage = URLEncoder.encode(sb.toString(), config.charset);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Error encoding Form message. Reason: " + e.getMessage(), e);
    }
    return formMessage;
  }

  private String createCustomMessage(String body, StructuredRecord input) {
    String customMessage = body;
    Matcher matcher = Pattern.compile(REGEX_HASHED_VAR).matcher(customMessage);
    HashMap<String, String> findReplaceMap = new HashMap();
    while (matcher.find()) {
      if (input.get(matcher.group(1)) != null) {
        findReplaceMap.put(matcher.group(1), (String) input.get(matcher.group(1)));
      } else {
        throw new IllegalArgumentException(String.format(
          "Field %s doesnt exist in the input schema.", matcher.group(1)));
      }
    }
    Matcher replaceMatcher = Pattern.compile(REGEX_HASHED_VAR).matcher(customMessage);
    while (replaceMatcher.find()) {
      String val = replaceMatcher.group().replace("#", "");
      customMessage = (customMessage.replace(replaceMatcher.group(), findReplaceMap.get(val)));
    }
    return customMessage;
  }

  /**
   * Output format provider for HTTP Sink.
   */
  private static class HTTPOutputFormatProvider implements OutputFormatProvider {
    private Map<String, String> conf = new HashMap<>();

    @Override
    public String getOutputFormatClassName() {
      return NullOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
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

  /**
   * Config for the HTTP sink.
   */
  public static class HTTPSinkConfig extends ReferencePluginConfig {

    @Description("The URL to post data to. (Macro Enabled)")
    @Macro
    private String url;

    @Description("The http request method. Defaults to POST. (Macro Enabled)")
    @Macro
    private String method;

    @Description("Batch size. Defaults to 1. (Macro Enabled)")
    @Macro
    private Integer batchSize;

    @Nullable
    @Description("Delimiter for messages to be used while batching. Defaults to \"\\n\". (Macro Enabled)")
    @Macro
    private String delimiterForMessages;

    @Description("Format to send messsage in. (Macro Enabled)")
    @Macro
    private String messageFormat;

    @Nullable
    @Description("Optional custom message. This is required if the message format is set to 'Custom'." +
      "User can leverage incoming message fields in the post payload. For example-" +
      "User has defined payload as \\{ \"messageType\" : \"update\", \"name\" : \"#firstName\" \\}" +
      "where #firstName will be substituted for the value that is in firstName in the incoming message. " +
      "(Macro enabled)")
    @Macro
    private String body;

    @Nullable
    @Description("Request headers to set when performing the http request. (Macro enabled)")
    @Macro
    private String requestHeaders;

    @Description("Charset. Defaults to UTF-8. (Macro enabled)")
    @Macro
    private String charset;

    @Description("Whether to automatically follow redirects. Defaults to true. (Macro enabled)")
    @Macro
    private Boolean followRedirects;

    @Description("If user enables SSL validation, they will be expected to add the certificate to the trustStore" +
      " on each machine. Defaults to true. (Macro enabled)")
    @Macro
    private Boolean disableSSLValidation;

    @Description("The number of times the request should be retried if the request fails. Defaults to 3. " +
      "(Macro enabled)")
    @Macro
    private Integer numRetries;

    @Description("Sets the connection timeout in milliseconds. Set to 0 for infinite. Default is 60000 (1 minute). " +
      "(Macro enabled)")
    @Nullable
    @Macro
    private Integer connectTimeout;

    @Description("The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute). " +
      "(Macro enabled)")
    @Nullable
    @Macro
    private Integer readTimeout;

    @Description("Whether to fail the pipeline on non-200 response from the http end point. Defaults to true. " +
      "(Macro enabled)")
    @Macro
    private Boolean failOnNon200Response;

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

    public Map<String, String> getRequestHeadersMap() {
      return convertHeadersToMap(requestHeaders);
    }

    public void validate() {
      try {
        new URL(url);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(String.format("URL '%s' is malformed: %s", url, e.getMessage()), e);
      }
      if (!containsMacro("connectTimeout") && connectTimeout < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid connectTimeout %d. Timeout must be 0 or a positive number.", connectTimeout));
      }
      convertHeadersToMap(requestHeaders);
      if (!containsMacro("method") && !METHODS.contains(method.toUpperCase())) {
        throw new IllegalArgumentException(String.format("Invalid request method %s, must be one of %s.",
                                                         method, Joiner.on(',').join(METHODS)));
      }
      if (!containsMacro("numRetries") && numRetries < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid numRetries %d. Retries cannot be a negative number.", numRetries));
      }
      if (!containsMacro("readTimeout") && readTimeout < 0) {
        throw new IllegalArgumentException(String.format(
          "Invalid readTimeout %d. Timeout must be 0 or a positive number.", readTimeout));
      }
      if (!containsMacro("messageFormat") && !containsMacro("body") && messageFormat.equalsIgnoreCase("Custom")
        && body == null) {
        throw new IllegalArgumentException("For Custom message format, message cannot be null.");
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
  }
}
