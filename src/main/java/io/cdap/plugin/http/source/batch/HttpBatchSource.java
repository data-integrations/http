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
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.http.source.common.error.HttpErrorHandler;
import io.cdap.plugin.http.source.common.error.HttpErrorHandlingStrategy;
import io.cdap.plugin.http.source.common.record.BaseStringToRecordConverter;
import io.cdap.plugin.http.source.common.record.StringToRecordConverterFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Plugin returns records from HTTP source specified by link. Pagination via APIs is supported.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(HttpBatchSource.NAME)
@Description("Read data from HTTP endpoint.")
public class HttpBatchSource extends BatchSource<IntWritable, Text, StructuredRecord> {
  static final String NAME = "HTTP";
  private static final Logger LOG = LoggerFactory.getLogger(HttpBatchSource.class);

  private static final String ERROR_SCHEMA_BODY_PROPERTY = "body";

  private static final Schema errorSchema = Schema.recordOf("error",
    Schema.Field.of(ERROR_SCHEMA_BODY_PROPERTY, Schema.of(Schema.Type.STRING))
  );

  private final HttpBatchSourceConfig config;
  private Schema schema;
  private HttpErrorHandler httpErrorHandler;

  public HttpBatchSource(HttpBatchSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(); // validate when macros not yet substituted
    config.validateSchema();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    config.validate(); // validate when macros are already substituted
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

  private BaseStringToRecordConverter stringToRecordConverter;

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    this.schema = config.getSchema();
    this.stringToRecordConverter = StringToRecordConverterFactory.createInstance(config, schema);
    this.httpErrorHandler = new HttpErrorHandler(config);

    super.initialize(context);
  }

  @Override
  public void transform(KeyValue<IntWritable, Text> input,
                        Emitter<StructuredRecord> emitter) throws IOException {
    String resultString = input.getValue().toString();
    try {
      int httpCode = input.getKey().get();
      HttpErrorHandlingStrategy httpRetryStrategy = httpErrorHandler.getErrorHandlingStrategy(httpCode)
        .getAfterRetryStrategy();

      if (httpRetryStrategy.equals(HttpErrorHandlingStrategy.SEND_TO_ERROR)) {
        emitter.emitError(buildError(httpCode, resultString,
                                     String.format("Request failed with '%d' http status code", httpCode)));
      } else if (httpRetryStrategy.equals(HttpErrorHandlingStrategy.SKIP)) {
        return;
      } else {
        StructuredRecord record = stringToRecordConverter.getRecord(resultString);
        emitter.emit(record);
      }
    } catch (Exception ex) {
      switch (config.getErrorHandling()) {
        case SKIP:
          LOG.warn("Cannot convert row '{}' to a record", resultString, ex);
          break;
        case SEND:
          emitter.emitError(buildError(0, resultString,
                                       ex.getClass().getName() + ": " + ex.getMessage()));
          break;
        case STOP:
          throw ex;
        default:
          throw new UnexpectedFormatException(
            String.format("Unknown error handling strategy '%s'", config.getErrorHandling()));
      }
    }
  }

  private InvalidEntry<StructuredRecord> buildError(int code, String recordBody, String errorText) {
    StructuredRecord.Builder builder = StructuredRecord.builder(errorSchema);
    builder.set(ERROR_SCHEMA_BODY_PROPERTY, recordBody);
    return new InvalidEntry<>(code, errorText, builder.build());
  }
}
