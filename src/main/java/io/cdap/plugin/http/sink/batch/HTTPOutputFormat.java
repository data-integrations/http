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

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * OutputFormat for HTTP writing
 */
public class HTTPOutputFormat extends OutputFormat<StructuredRecord, StructuredRecord> {
  private static final Gson GSON = new Gson();
  static final String CONFIG_KEY = "http.sink.config";

  @Override
  public RecordWriter<StructuredRecord, StructuredRecord> getRecordWriter(TaskAttemptContext context) {
    Configuration hConf = context.getConfiguration();
    HTTPSinkConfig config = GSON.fromJson(hConf.get(CONFIG_KEY), HTTPSinkConfig.class);
    return new HTTPRecordWriter(config);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {

  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }
}
