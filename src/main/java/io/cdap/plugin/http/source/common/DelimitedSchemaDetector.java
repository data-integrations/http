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
import io.cdap.plugin.format.delimited.input.SplitQuotesIterator;
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
        rowValue = getRowValues(line, config.getEnableQuotesValues(), delimiter);
        if (rowIndex == 0) {
          columnNames = DataTypeDetectorUtils.setColumnNames(line, config.getCsvSkipFirstRow(),
                  config.getEnableQuotesValues(), delimiter);
          if (Boolean.TRUE.equals(config.getCsvSkipFirstRow())) {
            continue;
          }
        }
        DataTypeDetectorUtils.detectDataTypeOfRowValues(
          new HashMap<>(), dataTypeDetectorStatusKeeper, columnNames, rowValue);
      }
      dataTypeDetectorStatusKeeper.validateDataTypeDetector();
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Error while reading the file to infer the schema. Error: %s",
                      e.getMessage()), null)
              .withStacktrace(e.getStackTrace());
      return null;
    }
    List<Schema.Field> fields = DataTypeDetectorUtils.detectDataTypeOfEachDatasetColumn(new HashMap<>(),
      columnNames, dataTypeDetectorStatusKeeper);
    return Schema.recordOf("text", fields);
  }

  /**
   * @param rawLine            line to parse and find out the exact number of columns in a row.
   * @param enableQuotedValues flag whether file can contain Quoted values.
   * @param delimiter          delimiter for the file
   * @return Array of all the column values within the provided row.
   */
  public static String[] getRowValues(String rawLine, boolean enableQuotedValues, String delimiter) {
    if (!enableQuotedValues) {
      return rawLine.split(delimiter, -1);
    } else {
      Iterator<String> splitsIterator = new SplitQuotesIterator(rawLine, delimiter, null, false);
      List<String> rowValues = new ArrayList<>();
      while (splitsIterator.hasNext()) {
        rowValues.add(splitsIterator.next());
      }
      return rowValues.toArray(new String[rowValues.size()]);
    }
  }
}
