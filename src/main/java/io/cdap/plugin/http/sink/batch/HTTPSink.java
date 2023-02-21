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

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sink plugin to send the messages from the pipeline to an external http endpoint.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("HTTP")
@Description("Sink plugin to send the messages from the pipeline to an external http endpoint.")
public class HTTPSink extends BatchSink<StructuredRecord, StructuredRecord, StructuredRecord> {
  private HTTPSinkConfig config;

  public HTTPSink(HTTPSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    context.addOutput(Output.of(config.referenceName, new HTTPSink.HTTPOutputFormatProvider(config)));
  }

  /**
   * Output format provider for HTTP Sink.
   */
  private static class HTTPOutputFormatProvider implements OutputFormatProvider {
    private static final Gson GSON = new Gson();
    private final HTTPSinkConfig config;

    HTTPOutputFormatProvider(HTTPSinkConfig config) {
      this.config = config;
    }

    @Override
    public String getOutputFormatClassName() {
      return HTTPOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return Collections.singletonMap("http.sink.config", GSON.toJson(config));
    }
  }

}
