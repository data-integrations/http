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
import io.cdap.plugin.http.source.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import io.cdap.plugin.http.source.common.pagination.state.IndexPaginationIteratorState;
import io.cdap.plugin.http.source.common.pagination.state.PaginationIteratorState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pagination by incrementing a {pagination.index} placeholder value in url. For this pagination type url is required
 * to contain above placeholder.
 */
public class IncrementAnIndexPaginationIterator extends BaseHttpPaginationIterator {
  public static final String PAGINATION_INDEX_PLACEHOLDER_REGEX = "\\{pagination.index\\}";

  private final Long indexIncrement;
  private final Long maxIndex;

  private Long index;

  public IncrementAnIndexPaginationIterator(BaseHttpSourceConfig config, PaginationIteratorState state) {
    super(config, state);
    this.indexIncrement = config.getIndexIncrement();
    this.maxIndex = config.getMaxIndex();

    // if loadFromState() hasn't already set it
    if (index == null) {
      this.index = config.getStartIndex() - this.indexIncrement;
    }

    this.nextPageUrl = getNextPageUrl(null);
  }

  private String getNextPageUrl(BasePage page) {
    index += indexIncrement;

    if (maxIndex != null) {
      // If the index is greater than max index, we stop the pagination
      if (index > maxIndex) {
        return null;
      }
    } else {
      // If the page received is empty, we stop the pagination
      if (page != null && page.next().getRecord() == null) {
        return null;
      }
    }

    return config.getUrl().replaceAll(PAGINATION_INDEX_PLACEHOLDER_REGEX, index.toString());
  }

  @Override
  protected String getNextPageUrl(HttpResponse response, BasePage page) {
    return getNextPageUrl(page);
  }

  @Override
  public boolean supportsSkippingPages() {
    return true;
  }

  @Override
  public PaginationIteratorState getCurrentState() {
    return new IndexPaginationIteratorState(index);
  }

  @Override
  protected void loadFromState(PaginationIteratorState state) {
    this.index = ((IndexPaginationIteratorState) state).getIndex();
  }
}
