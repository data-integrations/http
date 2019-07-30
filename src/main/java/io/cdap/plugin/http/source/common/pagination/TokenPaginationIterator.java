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
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

/**
 * Every page contains a token, which is appended as an url parameter to obtain next page.
 * This type of pagination is only supported for JSON and XML formats. Pagination happens until no next page
 * token is present on the page or until page contains no elements.
 */
public class TokenPaginationIterator extends BaseHttpPaginationIterator {
  private static final Logger LOG = LoggerFactory.getLogger(TokenPaginationIterator.class);

  public TokenPaginationIterator(BaseHttpSourceConfig config) {
    super(config);
  }

  @Override
  protected String getNextPageUrl(HttpResponse response, BasePage page) {
    String nextPageToken = page.getPrimitiveByPath(config.getNextPageTokenPath());

    if (nextPageToken == null) {
      return null;
    }

    try {
      URIBuilder uriBuilder = new URIBuilder(config.getUrl());
      uriBuilder.addParameter(config.getNextPageUrlParameter(), nextPageToken);
      return uriBuilder.build().toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("'%s' is not a valid URI", config.getUrl()), e);
    }
  }

  @Override
  public boolean supportsSkippingPages() {
    return false;
  }
}
