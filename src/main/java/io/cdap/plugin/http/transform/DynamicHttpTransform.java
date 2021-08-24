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
package io.cdap.plugin.http.transform;

import com.google.common.util.concurrent.RateLimiter;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.*;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.RetryPolicy;
import io.cdap.plugin.http.source.common.error.ErrorHandling;
import io.cdap.plugin.http.source.common.error.HttpErrorHandler;
import io.cdap.plugin.http.source.common.error.RetryableErrorHandling;
import io.cdap.plugin.http.source.common.http.HttpClient;
import io.cdap.plugin.http.source.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import io.cdap.plugin.http.source.common.pagination.page.PageEntry;
import io.cdap.plugin.http.source.common.pagination.page.PageFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.pollinterval.FixedPollInterval;
import org.awaitility.pollinterval.IterativePollInterval;
import org.awaitility.pollinterval.PollInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Plugin returns records from HTTP source specified by link for each input record. Pagination via APIs is supported.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(DynamicHttpTransform.NAME)
@Description("Read data from HTTP endpoint that changes dynamically depending on inputs data.")
public class DynamicHttpTransform extends Transform<StructuredRecord, StructuredRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicHttpTransform.class);
    static final String NAME = "HTTP";

    private final DynamicHttpTransformConfig config;
    private RateLimiter rateLimiter;
    private final HttpClient httpClient;
    private HttpResponse httpResponse;
    private final PollInterval pollInterval;
    private final HttpErrorHandler httpErrorHandler;
    private String url;
    private Integer httpStatusCode;

    private String prebuiltParameters;

    /**
     * Constructor used by Data Fusion
     *
     * @param config the plugin configuration
     */
    public DynamicHttpTransform(DynamicHttpTransformConfig config) {
        this(config, new HttpClient(config));
    }

    /**
     * Constructor used in unit tests
     *
     * @param config     the plugin configuration
     * @param httpClient the http client
     */
    public DynamicHttpTransform(DynamicHttpTransformConfig config, HttpClient httpClient) {
        this.config = config;
        if(config.throttlingEnabled()) {
            this.rateLimiter = RateLimiter.create(this.config.maxCallPerSeconds);
        }
        this.httpClient = httpClient;
        this.httpErrorHandler = new HttpErrorHandler(config);

        if (config.getRetryPolicy().equals(RetryPolicy.LINEAR)) {
            pollInterval = FixedPollInterval.fixed(config.getLinearRetryInterval(), TimeUnit.SECONDS);
        } else {
            pollInterval = IterativePollInterval.iterative(duration -> duration.multiply(2));
        }
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema()); // validate when macros not yet substituted
        config.validateSchema();

        pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
    }

    @Override
    public void initialize(TransformContext context) throws Exception {
        StringBuilder parametersBuilder = new StringBuilder();

        Map<String, String> queryParameters = config.getQueryParametersMap();
        if (queryParameters.size() > 0) {
            parametersBuilder.append("?");
            for (Map.Entry<String, String> e : queryParameters.entrySet()) {
                parametersBuilder.append(e.getKey() + "=" + e.getValue() + "&");
            }
        }
        this.prebuiltParameters = parametersBuilder.toString(); // Yes there is a '&' at the end of tURL but the url is still valid ;)

        super.initialize(context);
    }

    @Override
    public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
        // Replace placeholders in URL
        String url = config.getUrl();
        for (Map.Entry<String, String> e : config.getUrlVariablesMap().entrySet()) {
            String valueToUse = input.get(e.getValue());
            if (valueToUse != null) {
                String placeholder = "{" + e.getKey() + "}";
                if (!url.contains(placeholder)) {
                    LOG.warn("Placeholder " + placeholder + " not found in url "+url);
                } else {
                    url = url.replace(placeholder, valueToUse);
                }
            } else {
                emitter.emitError(new InvalidEntry<>(
                        -1, "Cannot find required field " + e.getValue(), input));
            }
        }

        this.url = url + prebuiltParameters;

        if(config.throttlingEnabled()) {
            rateLimiter.acquire(); // Throttle
        }

        long delay = (httpResponse == null || config.getWaitTimeBetweenPages() == null) ? 0L : config.getWaitTimeBetweenPages();

        try {
            Awaitility
                    .await().with()
                    .pollInterval(pollInterval)
                    .pollDelay(delay, TimeUnit.MILLISECONDS)
                    .timeout(config.getMaxRetryDuration(), TimeUnit.SECONDS)
                    .until(this::sendGet);  // httpResponse is setup here
        } catch (ConditionTimeoutException ex) {
            // Retries failed. We don't need to do anything here. This will be handled using httpStatusCode below.
        }

        ErrorHandling postRetryStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode)
                .getAfterRetryStrategy();

        switch (postRetryStrategy) {
            case STOP:
                throw new IllegalStateException(String.format("Fetching from url '%s' returned status code '%d' and body '%s'",
                        url, httpStatusCode, httpResponse.getBody()));
            default:
                break;
        }

        try {
            BasePage basePage = createPageInstance(config, httpResponse, postRetryStrategy);

            while (basePage.hasNext()) {
                PageEntry pageEntry = basePage.next();

                if (!pageEntry.isError()) {
                    emitter.emit(pageEntry.getRecord());
                } else {
                    emitter.emitError(pageEntry.getError());
                }
            }
        }catch (IOException e){
            emitter.emitError(new InvalidEntry<>(
                    -1, "Exception parsing HTTP Response : "+e.getMessage(), input));
        }
    }

    BasePage createPageInstance(BaseHttpSourceConfig config, HttpResponse httpResponse,
                                ErrorHandling postRetryStrategy) throws IOException {
        return PageFactory.createInstance(config, httpResponse, httpErrorHandler,
                !postRetryStrategy.equals(ErrorHandling.SUCCESS));
    }

    private boolean sendGet() throws IOException {
        CloseableHttpResponse response = httpClient.executeHTTP(this.url);
        this.httpResponse = new HttpResponse(response);
        httpStatusCode = httpResponse.getStatusCode();

        RetryableErrorHandling errorHandlingStrategy = httpErrorHandler.getErrorHandlingStrategy(httpStatusCode);
        return  !errorHandlingStrategy.shouldRetry();
    }
}
