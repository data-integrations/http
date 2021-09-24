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

import java.util.List;
import java.util.Map;

/**
 * Utilitary class
 */
public class Util {
    /**
     * Return true if the field (in output schema) is a reused field (coming from the input schema), false else
     * @param fieldName the name of the field
     * @param reusedInputs the list of reused input fields
     * @param reusedInputsMapping the mapping of reused fields
     * @return is reused
     */
    public static boolean isReusedField(
            String fieldName,
            List<String> reusedInputs,
            Map<String, String> reusedInputsMapping) {
        return reusedInputsMapping.containsValue(fieldName) ||
                (reusedInputs.contains(fieldName) && !reusedInputsMapping.containsKey(fieldName));
    }
}
