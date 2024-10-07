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
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.http.common.http.HttpClient;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.common.pagination.page.PageFormat;
import io.cdap.plugin.http.source.common.DelimitedSchemaDetector;
import io.cdap.plugin.http.source.common.RawStringPerLine;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * InputFormatProvider used by cdap to provide configurations to mapreduce job
 */
public class HttpInputFormatProvider implements InputFormatProvider {
  public static final String PROPERTY_CONFIG_JSON = "cdap.http.config";
  private static final Gson gson = new GsonBuilder().create();

  private final Map<String, String> conf;
  private final HttpBatchSourceConfig config;

  HttpInputFormatProvider(HttpBatchSourceConfig config) {
    this.conf = new ImmutableMap.Builder<String, String>()
      .put(PROPERTY_CONFIG_JSON, gson.toJson(config))
      .build();
    this.config = config;
  }

  @Override
  public String getInputFormatClassName() {
    return HttpInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }

  @Nullable
  public Schema getSchema(FailureCollector failureCollector) {
    PageFormat format = config.getFormat();
    switch (format) {
      case CSV:
      case TSV:
        String delimiter = format == PageFormat.CSV ? "," : "\t";
        try (HttpClient client = new HttpClient(config)) {
          CloseableHttpResponse closeableHttpResponse = client.executeHTTP(config.getUrl());
          int statusCode = closeableHttpResponse.getStatusLine().getStatusCode();
          if (statusCode < 200 || statusCode >= 300) {
            failureCollector.addFailure(String.format("Failed to read the file, non 2xx status code received: %s",
                                                      statusCode), null);
            return null;
          }
          RawStringPerLine rawStringPerLine = new RawStringPerLine(new HttpResponse(closeableHttpResponse));
          return DelimitedSchemaDetector.detectSchema(config, delimiter, rawStringPerLine, failureCollector);
        } catch (IOException e) {
          String errorMessage = e.getMessage();
          if (Strings.isNullOrEmpty(errorMessage) && e.getCause() != null) {
            errorMessage = e.getCause().getMessage();
          }
          failureCollector.addFailure(String.format("Error while reading the file to infer the schema. Error: %s",
                                                    errorMessage), null);
        }
        return null;
      default:
        return null;
    }
  }
}
