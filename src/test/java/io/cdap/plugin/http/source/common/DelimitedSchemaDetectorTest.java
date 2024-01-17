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
import io.cdap.plugin.http.source.batch.HttpBatchSourceConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Unit test for {@link DelimitedSchemaDetector}
 */
public class DelimitedSchemaDetectorTest {
  @Mock
  RawStringPerLine rawStringPerLineIterator;
  HttpBatchSourceConfig configSkipHeaderTrue;
  HttpBatchSourceConfig configSkipHeaderFalse;
  String csvDelimiter = ",";
  String tsvDelimiter = "\t";

  Schema expectedSchemaWithoutHeaders;
  Schema expectedSchemaWithHeaders;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    configSkipHeaderTrue = HttpBatchSourceConfig.builder().setCsvSkipFirstRow("true").build();
    configSkipHeaderFalse = HttpBatchSourceConfig.builder().setCsvSkipFirstRow("false").build();
    expectedSchemaWithHeaders = Schema.recordOf("text",
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("age", Schema.of(Schema.Type.INT)),
            Schema.Field.of("isIndian", Schema.of(Schema.Type.BOOLEAN)),
            Schema.Field.of("country", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
    expectedSchemaWithoutHeaders = Schema.recordOf("text",
            Schema.Field.of("body_0", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("body_1", Schema.of(Schema.Type.INT)),
            Schema.Field.of("body_2", Schema.of(Schema.Type.BOOLEAN)),
            Schema.Field.of("body_3", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
  }

  @Test
  public void testDetectSchemaCsvNoHeader() throws IOException {
    String[] lines = new String[] {"raj,29,true,india", "rahul,30,false,"};
    Mockito.when(rawStringPerLineIterator.hasNext()).thenReturn(true, true, false);
    Mockito.when(rawStringPerLineIterator.next()).thenReturn(lines[0], lines[1]);
    Schema schema = DelimitedSchemaDetector.detectSchema(
            configSkipHeaderFalse, csvDelimiter, rawStringPerLineIterator, null);
    Assert.assertEquals(expectedSchemaWithoutHeaders, schema);
  }

  @Test
  public void testDetectSchemaTsvNoHeader() throws IOException {
    String[] lines = new String[] {"raj\t29\ttrue\tindia", "rahul\t30\tfalse\t"};
    Mockito.when(rawStringPerLineIterator.hasNext()).thenReturn(true, true, false);
    Mockito.when(rawStringPerLineIterator.next()).thenReturn(lines[0], lines[1]);
    Schema schema = DelimitedSchemaDetector.detectSchema(
            configSkipHeaderFalse, tsvDelimiter, rawStringPerLineIterator, null);
    Assert.assertEquals(expectedSchemaWithoutHeaders, schema);
  }


  @Test
  public void testDetectSchemaCsvHeader() throws IOException {
    String[] lines = new String[] {"name,age,isIndian,country", "raj,29,true,india", "rahul,30,false,"};
    Mockito.when(rawStringPerLineIterator.hasNext()).thenReturn(true, true, true, false);
    Mockito.when(rawStringPerLineIterator.next()).thenReturn(lines[0], lines[1], lines[2]);
    Schema schema = DelimitedSchemaDetector.detectSchema(
            configSkipHeaderTrue, csvDelimiter, rawStringPerLineIterator, null);
    Assert.assertEquals(expectedSchemaWithHeaders, schema);
  }

  @Test
  public void testDetectSchemaTsvHeader() throws IOException {
    String[] lines = new String[] {"name\tage\tisIndian\tcountry", "raj\t29\ttrue\tindia", "rahul\t30\tfalse\t"};
    Mockito.when(rawStringPerLineIterator.hasNext()).thenReturn(true, true, true, false);
    Mockito.when(rawStringPerLineIterator.next()).thenReturn(lines[0], lines[1], lines[2]);
    Schema schema = DelimitedSchemaDetector.detectSchema(
            configSkipHeaderTrue, tsvDelimiter, rawStringPerLineIterator, null);
    Assert.assertEquals(expectedSchemaWithHeaders, schema);
  }

}
