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

package io.cdap.plugin.http.source.streaming;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

import javax.annotation.Nullable;

/**
 * Provides all the configurations required for configuring the {@link HttpStreamingSource} plugin.
 */
public class HttpStreamingSourceConfig extends BaseHttpSourceConfig {
  public static final String MAX_PAGES_PER_FETCH = "maxPagesPerFetch";

  @Name(MAX_PAGES_PER_FETCH)
  @Nullable
  @Description("Maximum number of pages put to RDD in one blocking reading. Empty value means that the\n" +
    "maximum is not enforced.")
  @Macro
  protected Long maxPagesPerFetch;

  protected HttpStreamingSourceConfig(String referenceName, String authType, String oauth2Enabled) {
    super(referenceName, authType, oauth2Enabled);
  }

  public Long getMaxPagesPerFetch() {
    if (maxPagesPerFetch == null) {
      return Long.MAX_VALUE;
    }

    return maxPagesPerFetch;
  }
}
