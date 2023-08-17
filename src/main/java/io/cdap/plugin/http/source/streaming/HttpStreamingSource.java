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

package io.cdap.plugin.http.source.streaming;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Plugin reads data from HTTP endpoint periodically waiting for updates.
 * For paginated APIs once the last page is reached it waits for the next pages.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(HttpStreamingSource.NAME)
@Description(HttpStreamingSource.DESCRIPTION)
public class HttpStreamingSource extends StreamingSource<StructuredRecord> {
  static final String NAME = "HTTP";
  static final String DESCRIPTION = "Read data from HTTP endpoint periodically waiting for updates";
  private HttpStreamingSourceConfig config;

  public HttpStreamingSource(HttpStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    // Verify that reference name meets dataset id constraints
    IdUtils.validateId(config.referenceName);
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    config.validate(failureCollector); // validate when macros are not substituted
    config.validateSchema();
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector); // validate when macros are substituted
    config.validateSchema();

    ClassTag<StructuredRecord> tag = ClassTag$.MODULE$.apply(StructuredRecord.class);
    HttpInputDStream dstream = new HttpInputDStream(context.getSparkStreamingContext().ssc(), tag, config);
    return JavaDStream.fromDStream(dstream, tag);
  }
}
