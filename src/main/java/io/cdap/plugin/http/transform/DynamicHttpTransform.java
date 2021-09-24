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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.error.ErrorHandling;
import io.cdap.plugin.http.source.common.error.HttpErrorHandler;
import io.cdap.plugin.http.source.common.http.HttpClient;
import io.cdap.plugin.http.source.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.pagination.page.BasePage;
import io.cdap.plugin.http.source.common.pagination.page.PageEntry;
import io.cdap.plugin.http.source.common.pagination.page.PageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plugin returns records from HTTP source specified by link. Pagination via APIs is supported.
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
    private final HttpErrorHandler httpErrorHandler;

    private List<String> reusedInputs;

    private Map<String, String> reusedInputsNameMap;

    // This is a reverse map compared to config.getReusedInputsNameMap()
    // reversedReusedInputsNameMap uses the name of the field in the output schema as Key and
    // the name of the field in the input schema as Value.
    // In config.getReusedInputsNameMap(), the Key is the name of the field in the input schema,
    // and the Value is the name of the field in the output schema (which is more intuitive for the user)
    // Since the uniqueness of values is checked in configuration validation,
    // this should not pause any problem and will be more efficient.
    private Map<String, String> reversedReusedInputsNameMap;

    private Schema outputSchema;

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
        this.httpClient = httpClient;
        if (config.throttlingEnabled()) {
            this.rateLimiter = RateLimiter.create(this.config.maxCallPerSeconds);
        }
        this.httpErrorHandler = new HttpErrorHandler(config);

        this.reusedInputs = config.getReusedInputs();
        this.reusedInputsNameMap = config.getReusedInputsNameMap();
        this.reversedReusedInputsNameMap = new HashMap<>();
        for (Map.Entry<String, String> e: reusedInputsNameMap.entrySet()) {
            this.reversedReusedInputsNameMap.put(e.getValue(), e.getKey());
        }
        this.outputSchema = config.getOutputSchema();
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema());
        // validate when macros not yet substituted
        config.validateSchema();

        pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getOutputSchema());
    }

    @Override
    public void initialize(TransformContext context) throws Exception {
        super.initialize(context);
    }

    @Override
    public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
        // Replace placeholders in URL
        String url = config.getBaseUrl();
        for (Map.Entry<String, String> e : config.getUrlVariablesMap().entrySet()) {
            String valueToUse = input.get(e.getValue());
            if (valueToUse != null) {
                String placeholder = "{" + e.getKey() + "}";
                if (!url.contains(placeholder)) {
                    LOG.warn("Placeholder " + placeholder + " not found in url " + url);
                } else {
                    url = url.replace(placeholder, valueToUse);
                }
            } else {
                emitter.emitError(new InvalidEntry<>(
                        -1, "Cannot find required field " + e.getValue(), input));
            }
        }

        config.setProcessedURL(url);

        if (config.throttlingEnabled()) {
            rateLimiter.acquire(); // Throttle
        }

        DynamicHttpRecordReader reader = new DynamicHttpRecordReader(config, httpClient);
        while (reader.nextKeyValue()) {
            BasePage page = reader.getCurrentValue();

            while (page.hasNext()) {
                PageEntry pageEntry = page.next();

                if (!pageEntry.isError()) {
                    StructuredRecord retrievedDataRecord = pageEntry.getRecord();
                    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

                    for (Schema.Field f : outputSchema.getFields()) {
                        String fieldName = f.getName();
                        Object fieldValue;

                        if (Util.isReusedField(f.getName(), reusedInputs, reusedInputsNameMap)) {
                            fieldValue = getReusedFieldValue(input, fieldName);
                        } else {
                            fieldValue = retrievedDataRecord.get(fieldName);
                        }
                        builder.set(fieldName, fieldValue);
                    }

                    emitter.emit(builder.build());
                } else {
                    emitter.emitError(pageEntry.getError());
                }
            }
        }
    }

    BasePage createPageInstance(BaseHttpSourceConfig config, HttpResponse httpResponse,
                                ErrorHandling postRetryStrategy) throws IOException {
        return PageFactory.createInstance(config, httpResponse, httpErrorHandler,
                !postRetryStrategy.equals(ErrorHandling.SUCCESS));
    }

    /**
     * Retrieve the given field in the given record handling the case the field is reused.
     * If the field name have been mapped, retrieve the original field name from the mapping and
     * retrieve the field value associated to this name from record,
     * Else, just retrieve the field value associated to the given fieldName
     * @param inputRecord the record
     * @param fieldName the field
     * @return the field value
     */
    public Object getReusedFieldValue(StructuredRecord inputRecord, String fieldName) {
        if (reversedReusedInputsNameMap.containsKey(fieldName)) {
            String fieldNameInInput = reversedReusedInputsNameMap.get(fieldName);
            return inputRecord.get(fieldNameInInput);
        } else {
            return inputRecord.get(fieldName);
        }
    }
}
