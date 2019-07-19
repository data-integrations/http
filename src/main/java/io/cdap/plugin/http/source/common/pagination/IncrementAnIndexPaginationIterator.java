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
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pagination by incrementing a {pagination.index} placeholder value in url. For this pagination type url is required
 * to contain above placeholder.
 */
public class IncrementAnIndexPaginationIterator extends BaseHttpPaginationIterator {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementAnIndexPaginationIterator.class);
  public static final String PAGINATION_INDEX_PLACEHOLDER_REGEX = "\\{pagination.index\\}";
  public static final String PAGINATION_INDEX_PLACEHOLDER = "{pagination.index}";

  private Long index;
  private Long indexIncrement;
  private Long maxIndex;

  public IncrementAnIndexPaginationIterator(BaseHttpSourceConfig config) {
    super(config);
    this.indexIncrement = config.getIndexIncrement();
    this.maxIndex = config.getMaxIndex();
    this.index = config.getStartIndex() - this.indexIncrement;
    this.nextPageUrl = getNextPageUrl();
  }

  private String getNextPageUrl() {
    index += indexIncrement;

    if (maxIndex != null && index > maxIndex) {
      return null;
    } else {
      return config.getUrl().replaceAll(PAGINATION_INDEX_PLACEHOLDER_REGEX, index.toString());
    }
  }

  @Override
  protected String getNextPageUrl(String body, CloseableHttpResponse response, BasePage page) {
    return getNextPageUrl();
  }

  @Override
  public boolean supportsSkippingPages() {
    return true;
  }
}
