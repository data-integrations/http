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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Provides all the configurations required for configuring the plugin.
 */
public class DynamicHttpTransformConfig extends BaseHttpSourceConfig {
    public static final String PROPERTY_URL_VARIABLES = "urlVariables";
    public static final String PROPERTY_QUERY_PARAMETERS = "queryParameters";
    public static final String PROPERTY_MAX_CALL_PER_SECONDS = "maxCallPerSeconds";

    @Name(PROPERTY_URL_VARIABLES)
    @Nullable
    @Description("Variables used to dynamically construct the URL.")
    @Macro
    protected String urlVariables;

    @Nullable
    @Name(PROPERTY_QUERY_PARAMETERS)
    @Description("Query parameters")
    @Macro
    protected String queryParameters;

    @Nullable
    @Name(PROPERTY_MAX_CALL_PER_SECONDS)
    @Description("The maximum number of call made per seconds. 0 = throttling disabled")
    @Macro
    protected int maxCallPerSeconds;

    protected DynamicHttpTransformConfig(String referenceName) {
        super(referenceName);
    }

    public void validate(Schema inputSchema) {
        super.validate(false);

        // Check that the needed fields exists in input schema
        Map<String,String> urlVariableMap = getUrlVariablesMap();
        for(String value: urlVariableMap.values()){
            if (inputSchema.getField(value) == null) {
                throw new IllegalArgumentException("Field " + value + " is required in input data schema but wasn't found. Current input schema is : " + inputSchema);
            }
        }

    }

    public boolean throttlingEnabled(){
        return maxCallPerSeconds>0;
    }

    public Map<String, String> getQueryParametersMap() {
        return getMapFromKeyValueString(queryParameters);
    }


    public Map<String, String> getUrlVariablesMap() {
        return getMapFromKeyValueString(urlVariables);
    }

}
