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

package io.cdap.plugin.http.sink.batch;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
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
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Sink plugin to send the messages from the pipeline to an external http endpoint.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HTTP")
@Description("Sink plugin to send the messages from the pipeline to an external http endpoint.")
public class HTTPSink extends BatchSink<StructuredRecord, Void, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);
  private static final String REGEX_HASHED_VAR = "#(\\w+)";

  private static StringBuilder messages = new StringBuilder();
  private String contentType;
  private HTTPSinkConfig config;

  public HTTPSink(HTTPSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    config.validateSchema(stageConfigurer.getInputSchema(), collector);
    collector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    config.validateSchema(context.getInputSchema(), collector);
    collector.getOrThrowException();

    Schema inputSchema = context.getInputSchema();
    Asset asset = Asset.builder(config.getReferenceNameOrNormalizedFQN())
      .setFqn(config.getUrl()).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(context.getInputSchema());
    lineageRecorder.recordWrite("Write", String.format("Wrote to HTTP '%s'", config.getUrl()),
                                Preconditions.checkNotNull(inputSchema.getFields()).stream()
                                  .map(Schema.Field::getName)
                                  .collect(Collectors.toList()));

    context.addOutput(Output.of(config.getReferenceNameOrNormalizedFQN(), new HTTPSink.HTTPOutputFormatProvider()));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
    String message = null;
    if (config.getMethod().equals("POST") || config.getMethod().equals("PUT")) {
      if (config.getMessageFormat().equals("JSON")) {
        message = StructuredRecordStringConverter.toJsonString(input);
        contentType = "application/json";
      } else if (config.getMessageFormat().equals("Form")) {
        message = createFormMessage(input);
        contentType = " application/x-www-form-urlencoded";
      } else if (config.getMessageFormat().equals("Custom")) {
        message = createCustomMessage(config.getBody(), input);
        contentType = " text/plain";
      }
      messages.append(message).append(config.getDelimiterForMessages());
    }
    StringTokenizer tokens = new StringTokenizer(messages.toString().trim(), config.getDelimiterForMessages());
    if (config.getBatchSize() == 1 || tokens.countTokens() == config.getBatchSize()) {
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
        if (messages.length() > 0) {
          conn.setDoOutput(true);
          try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(messages.toString().trim().getBytes(config.getCharset()));
          }
        }
        responseCode = conn.getResponseCode();
        messages.setLength(0);
        if (config.getFailOnNon200Response() && !(responseCode >= 200 && responseCode < 300)) {
          exception = new IllegalStateException("Received error response. Response code: " + responseCode);
        }
        break;
      } catch (MalformedURLException | ProtocolException e) {
        throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
      } catch (Exception e) {
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
      formMessage = URLEncoder.encode(sb.toString(), config.getCharset());
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
}
