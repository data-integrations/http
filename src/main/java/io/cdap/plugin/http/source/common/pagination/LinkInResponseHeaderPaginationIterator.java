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
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * In response there is a "Link" header, which contains an url marked as "next"
 */
public class LinkInResponseHeaderPaginationIterator extends BaseHttpPaginationIterator {
  private static final Logger LOG = LoggerFactory.getLogger(LinkInResponseHeaderPaginationIterator.class);
  private static final Pattern nextLinkPattern = Pattern.compile("<(.+)>; rel=next");

  public LinkInResponseHeaderPaginationIterator(BaseHttpSourceConfig config) {
    super(config);
  }

  @Override
  protected String getNextPageUrl(String body, CloseableHttpResponse response, BasePage page) {
    return getNextLinkFromHeader(response.getFirstHeader("Link"));
  }

  private static String getNextLinkFromHeader(Header header) {
    for (HeaderElement headerElement : header.getElements()) {
      Matcher matcher = nextLinkPattern.matcher(headerElement.toString());
      if (matcher.matches()) {
        return matcher.group(1);
      }
    }
    return null;
  }

  @Override
  public boolean supportsSkippingPages() {
    return true;
  }
}
