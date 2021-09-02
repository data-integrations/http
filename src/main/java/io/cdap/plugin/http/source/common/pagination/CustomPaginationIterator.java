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
package io.cdap.plugin.http.source.common.pagination;

import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpClient;
import io.cdap.plugin.http.source.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import io.cdap.plugin.http.source.common.pagination.state.PaginationIteratorState;
import org.apache.http.Header;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Pagination using user provided code. The code decides how to retrieve a next page url based on previous page contents
 * and headers and when to finish pagination.
 */
public class CustomPaginationIterator extends BaseHttpPaginationIterator {
  private final JythonPythonExecutor pythonExecutor;

  public CustomPaginationIterator(BaseHttpSourceConfig config, PaginationIteratorState state, HttpClient httpClient) {
    super(config, state, httpClient);
    pythonExecutor = new JythonPythonExecutor(config.getCustomPaginationCode());
    pythonExecutor.initialize();
  }

  @Override
  protected String getNextPageUrl(HttpResponse response, BasePage page) {
    Map<String, String> headersMap = new HashMap<>();
    Header[] headers = response.getAllHeaders();
    for (Header header : headers) {
      headersMap.put(header.getName(), header.getValue());
    }

    return pythonExecutor.getNextPageUrl(nextPageUrl, response.getBody(), headersMap);
  }

  @Override
  public boolean supportsSkippingPages() {
    return true;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      pythonExecutor.close();
    }
  }
}
