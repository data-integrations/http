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

import java.io.Closeable;
import java.util.Iterator;

/**
 * An iterator which iterates over every element on all the pages of the given resource. The page urls are generated
 * based on configured {@link PaginationType}.
 */
public abstract class BaseHttpPaginationIterator implements Iterator<PageEntry>, Closeable {
  public abstract boolean supportsSkippingPages();

  // TODO: this class is not yet implemented!
}
