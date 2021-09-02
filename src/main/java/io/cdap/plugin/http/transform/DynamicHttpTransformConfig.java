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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides all the configurations required for configuring the plugin.
 */
public class DynamicHttpTransformConfig extends BaseHttpSourceConfig {
    public static final String PROPERTY_URL_VARIABLES = "urlVariables";
    public static final String PROPERTY_QUERY_PARAMETERS = "queryParameters";
    public static final String PROPERTY_REUSED_INPUTS = "reusedInputs";
    public static final String PROPERTY_RENAME_REUSED_INPUTS = "renameReusedInputs";
    public static final String PROPERTY_MAX_CALL_PER_SECONDS = "maxCallPerSeconds";

    @Name(PROPERTY_URL_VARIABLES)
    @Nullable
    @Description("Variables used to dynamically construct the URL through placeholder. " +
            "Use the placeholder name as Key, and the field from input schema you want to use as Value.")
    @Macro
    protected String urlVariables;

    @Nullable
    @Name(PROPERTY_QUERY_PARAMETERS)
    @Description("Query parameters that will be append to the URL. For the URL http://my/url.com/, it will give : " +
       "http://my/url.com/?[QueryParametersKey1]=[QueryParametersValue1]&" +
            "[QueryParametersKey2]=[QueryParametersValue2] etc...")
    @Macro
    protected String queryParameters;

    @Nullable
    @Name(PROPERTY_REUSED_INPUTS)
    @Description("List of fields from inputSchema that will be added to the output schema. " +
            "If left empty, the output record will contains only the result of the HTTP " +
            "query and the input record will be lost." +
            "If a field reused from inputSchema has the same name as a field in the output schema, " +
            "use the \"Rename Reused Input Fields\" to rename the field.")
    @Macro
    protected String reusedInputs;

    @Nullable
    @Name(PROPERTY_RENAME_REUSED_INPUTS)
    @Description("Rename a reused field from input schema. This should be used when a reused field " +
            "from imput schema have the same name as a field from output schema.")
    @Macro
    protected String renameReusedInputs;

    @Nullable
    @Name(PROPERTY_MAX_CALL_PER_SECONDS)
    @Description("The maximum number of call made per seconds. 0 = throttling disabled")
    @Macro
    protected int maxCallPerSeconds;


    // processedURL is static so that it does not appears in plugin configuration panel
    private static String processedURL;

    public String getBaseUrl() {
        return url;
    }
    @Override
    public String getUrl() {
        return processedURL;
    }

    public void setProcessedURL(String processedURL) {
        this.processedURL = processedURL;
    }

    protected DynamicHttpTransformConfig(String referenceName) {
        super(referenceName);
    }

    public void validate(Schema inputSchema) {
        super.validate(false, false);

        // Check that the needed fields exists in input schema
        Map<String, String> urlVariableMap = getUrlVariablesMap();
        for (String value: urlVariableMap.values()) {
            if (inputSchema.getField(value) == null) {
                throw new IllegalArgumentException("Field " + value + " is required in input data schema " +
                        "but wasn't found. Current input schema is : " + inputSchema);
            }
        }

        Map<String, String> reusedInputsNameMap = getReusedInputsNameMap();
        HashSet<String> inputFields = new HashSet<>();
        HashSet<String> outputFields = new HashSet<>();
        for (Map.Entry<String, String> e: reusedInputsNameMap.entrySet()) {
            if (inputSchema.getField(e.getKey()) == null) {
                throw new IllegalArgumentException("Input field " + e.getKey() + " is configured to be renamed " +
                        "but is not present in the inputSchema");
            }
            if (getSchema().getField(e.getValue()) != null) {
                throw new IllegalArgumentException("Input field " + e.getKey() + " is configured to be " +
                        "renamed as " + e.getValue() + " but this field is already present in the outputSchema");
            }

            if (inputFields.add(e.getKey()) == false) {
                throw new IllegalArgumentException("Input field " + e.getKey() +
                        " is configured multiple times to be renamed");
            }
            if (outputFields.add(e.getValue()) == false) {
                throw new IllegalArgumentException("Multiple fields configured to be renamed " + e.getValue());
            }
        }



    }

    public boolean throttlingEnabled() {
        return maxCallPerSeconds > 0;
    }

    public Map<String, String> getQueryParametersMap() {
        return getMapFromKeyValueString(queryParameters, ",", ":");
    }


    public Map<String, String> getUrlVariablesMap() {
        return getMapFromKeyValueString(urlVariables, ",", ":");
    }


    public Map<String, String> getReusedInputsNameMap() {
        return getMapFromKeyValueString(renameReusedInputs, ",", ":");
    }

    public List<String> getReusedInputs() {
        List<String> uniqueFieldList = new ArrayList<>();
        if (!Strings.isNullOrEmpty(reusedInputs)) {
            for (String field : Splitter.on(',').trimResults().split(reusedInputs)) {
                uniqueFieldList.add(field);
            }
        }
        return uniqueFieldList;
    }

    @Override
    /**
     * Return the data schema.
     * In case no fields are reused from the input schema, this function is equals to getOutput()
     * Otherwise, this function return the data schema without the reused fields.
     */
    public Schema getSchema() {
        Schema schema = getOutputSchema();
        List<String> reusedInput = getReusedInputs();
        if (reusedInput.size() > 0) {
            Map<String, String> reusedInputsNameMap = getReusedInputsNameMap();

            List<Schema.Field> fields = new ArrayList<>();
            for (Schema.Field f : schema.getFields()) {
                if (!Util.isReusedField(f.getName(), reusedInput, reusedInputsNameMap)) {
                    fields.add(f);
                }
            }
            schema = Schema.recordOf("record", fields);
        }
        return schema;
    }


    public Schema getOutputSchema() {
        return super.getSchema();
    }
}
