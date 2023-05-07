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

package io.cdap.plugin.http.common.pagination.state;

import java.util.Objects;

/**
 * A default state object used for most pagination iterators. Which carries url of last processed page.
 */
public class UrlPaginationIteratorState implements PaginationIteratorState {
  private final String lastProcessedPageUrl;

  public UrlPaginationIteratorState(String lastProcessedPageUrl) {
    this.lastProcessedPageUrl = lastProcessedPageUrl;
  }

  public String getLastProcessedPageUrl() {
    return lastProcessedPageUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UrlPaginationIteratorState that = (UrlPaginationIteratorState) o;
    return lastProcessedPageUrl.equals(that.lastProcessedPageUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastProcessedPageUrl);
  }
}
