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

import io.cdap.plugin.http.common.RetryPolicy;
import io.cdap.plugin.http.common.error.ErrorHandling;
import io.cdap.plugin.http.common.error.HttpErrorHandler;
import io.cdap.plugin.http.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.common.http.ErrorHttpResponse;
import io.cdap.plugin.http.common.http.HttpClient;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.common.pagination.page.BasePage;
import io.cdap.plugin.http.common.pagination.page.PageFactory;
import io.cdap.plugin.http.common.pagination.state.PaginationIteratorState;
import io.cdap.plugin.http.common.pagination.state.UrlPaginationIteratorState;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.pollinterval.FixedPollInterval;
import org.awaitility.pollinterval.IterativePollInterval;
import org.awaitility.pollinterval.PollInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An iterator which iterates over every element on all the pages of the given resource. The page urls are generated
 * based on configured {@link PaginationType}.
 */
public abstract class BaseHttpPaginationIterator implements Iterator<BasePage>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHttpPaginationIterator.class);
  private static final int CODE_EXCEPTION = -1;

  protected final BaseHttpSourceConfig config;
  private final HttpClient httpClient;
  private final HttpErrorHandler httpErrorHandler;
  private final PollInterval pollInterval;

  protected String nextPageUrl;
  private String currentPageUrl;
  private boolean currentPageReturned = true;
  private BasePage page;
  private int httpStatusCode;
  private HttpResponse response;

  public BaseHttpPaginationIterator(BaseHttpSourceConfig config, PaginationIteratorState state) {
    this.config = config;
    this.httpClient = new HttpClient(config);
    this.nextPageUrl = config.getUrl();
    this.httpErrorHandler = new HttpErrorHandler(config);

    if (config.getRetryPolicy().equals(RetryPolicy.LINEAR)) {
      pollInterval = FixedPollInterval.fixed(config.getLinearRetryInterval(), TimeUnit.SECONDS);
    } else {
      pollInterval = IterativePollInterval.iterative(duration -> duration.multiply(2));
    }

    if (state != null) {
      loadFromState(state);
    }
  }

  protected abstract String getNextPageUrl(HttpResponse httpResponse, BasePage page);
  public abstract boolean supportsSkippingPages();

  protected boolean visitPageAndCheckStatusCode() throws IOException {
    if (response != null) { // close previous response
      response.close();
    }

    try {
      response = new HttpResponse(getHttpClient().executeHTTP(nextPageUrl));
    } catch (IOException ioException) {
      response = new ErrorHttpResponse(CODE_EXCEPTION);
    }

    currentPageUrl = nextPageUrl;
    httpStatusCode = response.getStatusCode();
    RetryableErrorHandling errorHandlingStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode);

    return !errorHandlingStrategy.shouldRetry();
  }

  @Nullable
  protected BasePage getNextPage() throws IOException {
    // no more pages
    if (nextPageUrl == null) {
      return null;
    }

    // response being null, means it's the first page we are loading
    Long delay = (response == null || config.getWaitTimeBetweenPages() == null) ? 0L : config.getWaitTimeBetweenPages();
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

    ErrorHandling postRetryStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode)
        .getAfterRetryStrategy();

    switch (postRetryStrategy) {
      case SUCCESS:
        break;
      case STOP:
        throw new IllegalStateException(String.format("Fetching from url '%s' returned status code '%d' and body '%s'",
                                                      nextPageUrl, httpStatusCode, response.getBody()));
      case SKIP:
      case SEND:
        if (!this.supportsSkippingPages()) {
          throw new IllegalStateException(String.format(
            "Pagination type '%s', does not support 'skip' and 'send to error' error handling.",
            config.getPaginationType()));
        }
        LOG.warn(String.format("Fetching from url '%s' returned status code '%d' and body '%s'",
                               nextPageUrl, httpStatusCode, response.getBody()));
        // this will be handled by PageFactory. Here no handling is needed.
        break;
      default:
        throw new IllegalArgumentException(String.format("Unexpected http error handling: '%s'", postRetryStrategy));
    }

    BasePage page = createPageInstance(nextPageUrl, config, response, postRetryStrategy, httpStatusCode);

    nextPageUrl = getNextPageUrl(response, page);
    LOG.debug("Next Page Url is '{}'", nextPageUrl);

    return page;
  }

  public String getCurrentPageUrl() {
    return currentPageUrl;
  }

  /**
   * @return true if page still has elements to iterate. Otherwise it will load next page.
   * False if no more pages to load or the page loaded has no elements.
   */
  protected boolean ensurePageIterable() {
    try {
      if (currentPageReturned) {
        page = getNextPage();
        currentPageReturned = false;
      }

      return page != null && page.hasNext(); // check hasNext() to stop on first empty page.
    } catch (IOException e) {
      throw new IllegalStateException("Failed to the load page", e);
    }
  }

  // for testing purposes
  public HttpClient getHttpClient() {
    return httpClient;
  }

  // for testing purposes
  BasePage createPageInstance(String pageUrl, BaseHttpSourceConfig config, HttpResponse httpResponse,
                              ErrorHandling postRetryStrategy, int statusCode) throws IOException {
    return PageFactory.createInstance(pageUrl, config, httpResponse, httpErrorHandler, statusCode,
                                      !postRetryStrategy.equals(ErrorHandling.SUCCESS));
  }

  public PaginationIteratorState getCurrentState() {
    return new UrlPaginationIteratorState(currentPageUrl);
  }

  protected void loadFromState(PaginationIteratorState state) {
    this.nextPageUrl = ((UrlPaginationIteratorState) state).getLastProcessedPageUrl();
  }

  @Override
  public BasePage next() {
    if (!ensurePageIterable()) {
      throw new NoSuchElementException("No more pages to load.");
    }

    currentPageReturned = true;
    return page;
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
