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
package io.cdap.plugin.http.common.pagination;

import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.common.pagination.page.BasePage;
import io.cdap.plugin.http.common.pagination.state.PaginationIteratorState;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Every page contains a next page url. This pagination type is only supported for JSON and XML formats.
 * Pagination happens until no next page field is present or until page contains no elements.
 */
public class LinkInResponseBodyPaginationIterator extends BaseHttpPaginationIterator {
  private static final Logger LOG = LoggerFactory.getLogger(LinkInResponseBodyPaginationIterator.class);

  private final String address;

  public LinkInResponseBodyPaginationIterator(BaseHttpSourceConfig config, PaginationIteratorState state) {
    super(config, state);
    URI uri = URI.create(config.getUrl());
    this.address = uri.getScheme() + "://" + uri.getAuthority();
  }

  @Override
  protected String getNextPageUrl(HttpResponse response, BasePage page) {
    String urlString = page.getPrimitiveByPath(config.getNextPageFieldPath());

    if (urlString == null) {
      return null;
    }

    try {
      URI uri = new URI(urlString);
      if (uri.isAbsolute()) {
        return uri.toString();
      } else {
        return this.address + urlString;
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("'%s' is not a valid URI", urlString), e);
    }
  }

  @Override
  public boolean supportsSkippingPages() {
    return false;
  }
}
