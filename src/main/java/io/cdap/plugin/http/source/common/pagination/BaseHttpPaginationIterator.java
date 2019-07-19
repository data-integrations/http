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
import io.cdap.plugin.http.source.common.RetryPolicy;
import io.cdap.plugin.http.source.common.error.HttpErrorHandler;
import io.cdap.plugin.http.source.common.error.HttpErrorHandlingStrategy;
import io.cdap.plugin.http.source.common.http.HttpClient;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import io.cdap.plugin.http.source.common.pagination.page.PageFactory;
import io.cdap.plugin.http.source.common.pagination.page.PageFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.pollinterval.FixedPollInterval;
import org.awaitility.pollinterval.IterativePollInterval;
import org.awaitility.pollinterval.PollInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An iterator which iterates over every element on all the pages of the given resource. The page urls are generated
 * based on configured {@link PaginationType}.
 */
public abstract class BaseHttpPaginationIterator implements Iterator<PageEntry>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHttpPaginationIterator.class);

  protected final BaseHttpSourceConfig config;
  private final HttpClient httpClient;
  private final HttpErrorHandler httpErrorHandler;
  private final PollInterval pollInterval;

  protected String nextPageUrl;
  private BasePage page;
  private Integer httpStatusCode;
  private CloseableHttpResponse response;

  public BaseHttpPaginationIterator(BaseHttpSourceConfig config) {
    this.config = config;
    this.httpClient = new HttpClient(config);
    this.nextPageUrl = config.getUrl();
    this.httpErrorHandler = new HttpErrorHandler(config);

    if (config.getRetryPolicy().equals(RetryPolicy.LINEAR)) {
      pollInterval = FixedPollInterval.fixed(config.getLinearRetryInterval(), TimeUnit.SECONDS);
    } else {
      pollInterval = IterativePollInterval.iterative(duration -> duration.multiply(2));
    }
  }

  protected abstract String getNextPageUrl(String body, CloseableHttpResponse response, BasePage page);
  public abstract boolean supportsSkippingPages();

  protected boolean visitPageAndCheckStatusCode() throws IOException {
    if (response != null) { // close previous response
      response.close();
    }

    response = getHttpClient().executeHTTP(nextPageUrl);
    httpStatusCode = response.getStatusLine().getStatusCode();
    HttpErrorHandlingStrategy errorHandlingStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode);

    return !errorHandlingStrategy.shouldRetry();
  }

  @Nullable
  protected BasePage getNextPage() throws IOException {
    // no more pages
    if (nextPageUrl == null) {
      return null;
    }

    // response being null, means it's the first page we are loading
    Long delay = (response == null) ? 0L : config.getWaitTimeBetweenPages();
    LOG.debug("Fetching '{}'", nextPageUrl);

    try {
      Awaitility
        .await().with()
        .pollInterval(pollInterval)
        .pollDelay(delay, TimeUnit.MILLISECONDS)
        .timeout(config.getMaxRetryDuration(), TimeUnit.SECONDS)
        .until(this::visitPageAndCheckStatusCode);
    } catch (ConditionTimeoutException ex) {
      // Retries failed. We don't need to do anything here. This will be handled using httpStatusCode below.
    }

    HttpEntity reponseEntity = response.getEntity();
    byte[] responseBytes = EntityUtils.toByteArray(reponseEntity);
    String responseBody = new String(responseBytes, StandardCharsets.UTF_8);

    HttpErrorHandlingStrategy postRetryStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode)
      .getAfterRetryStrategy();

    String errorMessage = String.format("Fetching from url '%s' returned status code '%d' and body '%s'",
                                        nextPageUrl, httpStatusCode, responseBody);

    switch (postRetryStrategy) {
      case SUCCESS:
        break;
      case FAIL:
        throw new IllegalStateException(errorMessage);
      case SKIP:
      case SEND_TO_ERROR:
        if (!this.supportsSkippingPages()) {
          throw new IllegalStateException(String.format(
            "Pagination type '%s', does not support 'skip' and 'send to error' error handling.",
            config.getPaginationType()));
        }
        LOG.warn(errorMessage);
        // this will be handled by PageFactory. Here no handling is needed.
        break;
      default:
        throw new IllegalArgumentException(String.format("Unexpected http error handling: '%s'", postRetryStrategy));
    }

    if (config.getFormat().equals(PageFormat.BLOB)) {
      responseBody = new String(Base64.encodeBase64(responseBytes), StandardCharsets.UTF_8);
    }

    BasePage page = createPageInstance(config, responseBody, postRetryStrategy);
    nextPageUrl = getNextPageUrl(responseBody, response, page);

    LOG.debug("Next Page Url is '{}'", nextPageUrl);
    //LOG.info("Schema is: {}", config.getSchemaString());

    return page;
  }

  /**
   * @return true if page still has elements to iterate. Otherwise it will load next page.
   * False if no more pages to load or the page loaded has no elements.
   */
  protected boolean ensurePageIterable() {
    try {
      if (page == null || !page.hasNext()) {
        page = getNextPage();
      }

      return page != null && page.hasNext();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to the load page", e);
    }
  }

  // for testing purposes
  HttpClient getHttpClient() {
    return httpClient;
  }

  // for testing purposes
  BasePage createPageInstance(BaseHttpSourceConfig config, String responseBody,
                              HttpErrorHandlingStrategy postRetryStrategy) {
    return PageFactory.createInstance(config, responseBody, postRetryStrategy);
  }

  @Override
  public PageEntry next() {
    if (!ensurePageIterable()) {
      throw new NoSuchElementException("No more pages to load.");
    }

    return new PageEntry(httpStatusCode, page.next());
  }

  @Override
  public boolean hasNext() {
    return ensurePageIterable();
  }

  @Override
  public void close() throws IOException {
    try {
      if (getHttpClient() != null) {
        getHttpClient().close();
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
}
