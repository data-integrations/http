/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.http.source.common;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

/**
 * Unit tests for HttpBatchSourceConfig
 */
public class HttpBatchSourceConfigTest {

    @Test (expected = IllegalArgumentException.class)
    public void testMissingKeyValue() {
        FailureCollector collector = new MockFailureCollector();
        HttpBatchSourceConfig config = HttpBatchSourceConfig.builder()
                .setReferenceName("test").setUrl("http://localhost").setHttpMethod("GET").setHeaders("Auth:")
                .setFormat("JSON").setAuthType("none").setErrorHandling(StringUtils.EMPTY)
                .setRetryPolicy(StringUtils.EMPTY).setMaxRetryDuration(600L).setConnectTimeout(120)
                .setReadTimeout(120).setPaginationType("NONE").setVerifyHttps("true").build();
        config.validate(collector);
    }
}
