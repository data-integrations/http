/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.format.delimited.common.DataTypeDetectorStatusKeeper;
import io.cdap.plugin.format.delimited.common.DataTypeDetectorUtils;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;

import java.util.HashMap;
import java.util.List;

/**
 * Class that detects the schema of the delimited file.
 */
public class DelimitedSchemaDetector {
  public static Schema detectSchema(HttpBatchSourceConfig config, String delimiter,
                                    RawStringPerLine rawStringPerLine, FailureCollector failureCollector) {
    DataTypeDetectorStatusKeeper dataTypeDetectorStatusKeeper = new DataTypeDetectorStatusKeeper();
    String line;
    String[] columnNames = null;
    String[] rowValue;
    long sampleSize = config.getSampleSize();
    try {
      for (int rowIndex = 0; rowIndex < sampleSize && rawStringPerLine.hasNext(); rowIndex++) {
        line = rawStringPerLine.next();
        rowValue = line.split(delimiter, -1);
        if (rowIndex == 0) {
          columnNames = DataTypeDetectorUtils.setColumnNames(line, config.getCsvSkipFirstRow(),
                  config.getEnableQuotesValues(), delimiter);
          if (config.getCsvSkipFirstRow()) {
            continue;
          }
        }
        DataTypeDetectorUtils.detectDataTypeOfRowValues(new HashMap<>(), dataTypeDetectorStatusKeeper, columnNames,
                rowValue);
      }
      dataTypeDetectorStatusKeeper.validateDataTypeDetector();
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Error while reading the file to infer the schema. Error: %s",
                      e.getMessage()), null)
              .withStacktrace(e.getStackTrace());
      return null;
    }
    List<Schema.Field> fields = DataTypeDetectorUtils.detectDataTypeOfEachDatasetColumn(
            new HashMap<>(), columnNames, dataTypeDetectorStatusKeeper);
    return Schema.recordOf("text", fields);
  }
}
