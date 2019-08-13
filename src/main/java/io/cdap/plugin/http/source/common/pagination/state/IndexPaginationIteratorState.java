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

package io.cdap.plugin.http.source.common.pagination.state;

import io.cdap.plugin.http.source.common.pagination.IncrementAnIndexPaginationIterator;

import java.util.Objects;

/**
 * A state object used for {@link IncrementAnIndexPaginationIterator} and carries index of the page which was
 * processed lastest.
 */
public class IndexPaginationIteratorState implements PaginationIteratorState {
  private final Long index;

  public IndexPaginationIteratorState(Long index) {
    this.index = index;
  }

  public Long getIndex() {
    return index;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IndexPaginationIteratorState that = (IndexPaginationIteratorState) o;
    return index.equals(that.index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index);
  }
}
