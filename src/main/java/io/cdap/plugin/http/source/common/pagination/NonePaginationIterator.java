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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Returns a single page of data
 */
public class NonePaginationIterator extends BaseHttpPaginationIterator {
  private static final Logger LOG = LoggerFactory.getLogger(NonePaginationIterator.class);

  boolean isMultiQuery;

  public NonePaginationIterator(BaseHttpSourceConfig config, PaginationIteratorState state, HttpClient httpClient,
                                boolean isMultiQuery) {
    super(config, state, httpClient);
    this.isMultiQuery = isMultiQuery;
  }

  @Override
  protected String getNextPageUrl(HttpResponse response, BasePage page) {
    return null;
  }

  @Override
  public boolean supportsSkippingPages() {
    return isMultiQuery;
  }
}
