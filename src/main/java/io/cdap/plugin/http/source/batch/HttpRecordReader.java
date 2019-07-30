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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.http.source.common.pagination.BaseHttpPaginationIterator;
import io.cdap.plugin.http.source.common.pagination.PaginationIteratorFactory;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * RecordReader implementation, which reads text records representations and http codes
 * using {@link BaseHttpPaginationIterator} subclasses.
 */
public class HttpRecordReader extends RecordReader<NullWritable, BasePage> {
  private static final Logger LOG = LoggerFactory.getLogger(HttpRecordReader.class);
  private static final Gson gson = new GsonBuilder().create();

  private BaseHttpPaginationIterator httpPaginationIterator;
  private BasePage value;

  /**
   * Initialize an iterator and config.
   *
   * @param inputSplit specifies batch details
   * @param taskAttemptContext task context
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(HttpInputFormatProvider.PROPERTY_CONFIG_JSON);
    HttpBatchSourceConfig httpBatchSourceConfig = gson.fromJson(configJson, HttpBatchSourceConfig.class);
    httpPaginationIterator = PaginationIteratorFactory.createInstance(httpBatchSourceConfig);
  }

  @Override
  public boolean nextKeyValue() {
    if (!httpPaginationIterator.hasNext()) {
      return false;
    }
    value = httpPaginationIterator.next();
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public BasePage getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    // progress is unknown
    return 0.0f;
  }

  @Override
  public void close() throws IOException {
    if (httpPaginationIterator != null) {
      httpPaginationIterator.close();
    }
  }
}
