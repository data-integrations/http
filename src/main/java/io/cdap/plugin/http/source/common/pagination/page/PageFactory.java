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
package io.cdap.plugin.http.source.common.pagination.page;

import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.error.HttpErrorHandlingStrategy;

/**
 * A factory which creates instance of {@BasePage} in accordance to format configured in input config.
 * If erroneous page is being handled, the records should not be iterated so {@WholeTextPage} is returned.
 */
public class PageFactory {
  public static BasePage createInstance(BaseHttpSourceConfig config, String responseBody,
                                        HttpErrorHandlingStrategy postRetryStrategy) {
    throw new IllegalStateException("Not yet implemented"); // TODO: implement this
  }
}
