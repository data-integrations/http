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

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import io.cdap.plugin.http.source.common.pagination.page.PageEntry;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Plugin returns records from HTTP source specified by link. Pagination via APIs is supported.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(HttpBatchSource.NAME)
@Description("Read data from HTTP endpoint.")
public class HttpBatchSource extends BatchSource<NullWritable, BasePage, StructuredRecord> {
  static final String NAME = "HTTP";

  private static final Logger LOG = LoggerFactory.getLogger(HttpBatchSource.class);

  private final HttpBatchSourceConfig config;
  private Schema schema;

  public HttpBatchSource(HttpBatchSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector); // validate when macros not yet substituted
    config.validateSchema();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector); // validate when macros are already substituted
    config.validateSchema();

    schema = config.getSchema();

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", String.format("Read from HTTP '%s'", config.getUrl()),
      Preconditions.checkNotNull(schema.getFields()).stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toList()));

    context.setInput(Input.of(config.referenceName, new HttpInputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    this.schema = config.getSchema();
    super.initialize(context);
  }

  @Override
  public void transform(KeyValue<NullWritable, BasePage> input, Emitter<StructuredRecord> emitter) {
    BasePage page = input.getValue();
    while (page.hasNext()) {
      PageEntry pageEntry = page.next();

      if (!pageEntry.isError()) {
        emitter.emit(pageEntry.getRecord());
      } else {
        InvalidEntry<StructuredRecord> invalidEntry = pageEntry.getError();
        switch (pageEntry.getErrorHandling()) {
          case SKIP:
            LOG.warn(invalidEntry.getErrorMsg());
            break;
          case SEND:
            emitter.emitError(invalidEntry);
            break;
          case STOP:
            throw new RuntimeException(invalidEntry.getErrorMsg());
          default:
            throw new UnexpectedFormatException(
              String.format("Unknown error handling strategy '%s'", config.getErrorHandling()));
        }
      }
    }
  }
}
