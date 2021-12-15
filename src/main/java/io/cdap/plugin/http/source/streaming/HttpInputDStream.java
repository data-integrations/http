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

import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.http.common.pagination.BaseHttpPaginationIterator;
import io.cdap.plugin.http.common.pagination.PaginationIteratorFactory;
import io.cdap.plugin.http.common.pagination.page.BasePage;
import io.cdap.plugin.http.common.pagination.state.PaginationIteratorState;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Iterates over the http pages and fills Spark RDD with structured records from them.
 *
 * Note: Fields of this class get checkpointed, which means that their value is restored if pipeline is re-run
 * after a stop.
 */
public class HttpInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HttpInputDStream.class);
  private final HttpStreamingSourceConfig config;

  // saves state between pipeline runs, due to checkpointing.
  private PaginationIteratorState state;

  public HttpInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> evidence1,
                          HttpStreamingSourceConfig config) {
    super(ssc, evidence1);
    this.config = config;
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }

  /**
   * Adds records to Spark RDD. This method is run every batchInterval seconds.
   * We need to give out records in portions here (instead of all at once).
   * So that the process gets more parallel.
   */
  @Override
  public Option<RDD<StructuredRecord>> compute(Time time) {
    try {
      return doCompute();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Option<RDD<StructuredRecord>> doCompute() throws IOException {
    try (BaseHttpPaginationIterator httpPaginationIterator = PaginationIteratorFactory.createInstance(config, state)) {
      int pagesFetched = 0;
      long batchInterval = getBatchIntervalMilliseconds();
      long startTime = System.currentTimeMillis();
      LinkedList<StructuredRecord> records = new LinkedList<>();

      while (httpPaginationIterator.hasNext()) {
        BasePage page = httpPaginationIterator.next();
        String pageUrl = httpPaginationIterator.getCurrentPageUrl();
        PaginationIteratorState pageState = httpPaginationIterator.getCurrentState();

        LOG.debug("Visited {}", pageUrl);

        // skip last page, if already processed
        if (pageState.equals(this.state)) {
          continue;
        }

        LOG.debug("Adding records for {}", pageUrl);
        while (page.hasNext()) {
          records.add(page.next().getRecord());
        }

        this.state = pageState;

        // Computing has been running for too long. Give out the current portion of pages to Spark.
        // And than come back for more.
        if (++pagesFetched >= config.getMaxPagesPerFetch() ||
          System.currentTimeMillis() - startTime > batchInterval) {
          break;
        }
      }

      RDD<StructuredRecord> rdds = getJavaSparkContext().parallelize(records).rdd();
      return Option.apply(rdds);
    }
  }

  private JavaSparkContext getJavaSparkContext() {
    return JavaSparkContext.fromSparkContext(ssc().sc());
  }

  private long getBatchIntervalMilliseconds() {
    return ssc().graph().batchDuration().milliseconds();
  }
}
