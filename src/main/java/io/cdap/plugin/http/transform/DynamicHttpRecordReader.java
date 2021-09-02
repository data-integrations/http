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
package io.cdap.plugin.http.transform;

import io.cdap.plugin.http.source.common.http.HttpClient;
import io.cdap.plugin.http.source.common.pagination.BaseHttpPaginationIterator;
import io.cdap.plugin.http.source.common.pagination.PaginationIteratorFactory;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;

import java.io.IOException;

/**
 * RecordReader implementation, which reads text records representations and http codes
 * using {@link BaseHttpPaginationIterator} subclasses.
 */
public class DynamicHttpRecordReader {
  protected BaseHttpPaginationIterator httpPaginationIterator;
  private BasePage value;

  public DynamicHttpRecordReader(DynamicHttpTransformConfig dynamicHttpTransformConfig, HttpClient httpClient) {
    httpPaginationIterator = PaginationIteratorFactory.createInstance(dynamicHttpTransformConfig, null,
            true, httpClient);
  }

  public boolean nextKeyValue() {
    if (!httpPaginationIterator.hasNext()) {
      return false;
    }
    value = httpPaginationIterator.next();
    return true;
  }

  public BasePage getCurrentValue() {
    return value;
  }

  public void close() throws IOException {
    if (httpPaginationIterator != null) {
      httpPaginationIterator.close();
    }
  }
}
