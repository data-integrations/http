package io.cdap.plugin.http.transform;

import com.google.common.base.Joiner;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.plugin.http.source.common.http.HttpClient;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicHttpTransformTest {
    private void tmp_test(){
    }

    // The input schema
    private static final Schema INPUT_SCHEMA = Schema.recordOf("input",
            Schema.Field.of("firstName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of("lastName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of("mail", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("_id", Schema.of(Schema.Type.STRING))
    );

    private static final String OUTPUT_SCHEMA = Schema.recordOf("input",
            Schema.Field.of("firstName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of("lastName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
            Schema.Field.of("mail", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("_id", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("assignedPrograms", Schema.nullableOf(Schema.of(Schema.Type.INT))),
            Schema.Field.of("averageScore", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
            Schema.Field.of("paths", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING))))
    ).toString();

    private StructuredRecord generateData(String idValue) {
        StructuredRecord record = StructuredRecord.builder(INPUT_SCHEMA)
                .set("firstName", "toto")
                .set("mail", "toto.tata@tutu.com")
                .set("_id", idValue)
                .build();
        return record;
    }

    static class BaseTestConfigHttp extends DynamicHttpTransformConfig {
        BaseTestConfigHttp(String referenceName, String url, String queryParameters, String urlVariables, int maxCallPerSeconds) {
            super(referenceName);

            this.schema = OUTPUT_SCHEMA;
            this.url = url;
            this.queryParameters = queryParameters;
            this.urlVariables = urlVariables;
            this.maxCallPerSeconds = maxCallPerSeconds;
            this.httpMethod = "GET";
            this.oauth2Enabled = "false";
            this.httpErrorsHandling = "2..:Success,.*:Fail";
            this.errorHandling = "stopOnError";
            this.retryPolicy = "linear";
            this.maxRetryDuration = 10L;
            this.linearRetryInterval = 1L;
            this.waitTimeBetweenPages = 0L;
            this.connectTimeout = 60;
            this.readTimeout = 120;
            this.format = "text";
            this.keystoreType = "Java KeyStore (JKS)";
            this.trustStoreType = "Java KeyStore (JKS)";
            this.transportProtocols = "TLSv1.2";
            this.format = "json";
        }
    }

    @Test
    public void testHttpDynamicTransformNominal() throws Exception {
        List<StructuredRecord> outputRecords = testHttpDynamicTransform("user.json");

        Assert.assertTrue(outputRecords.size() == 1);
        StructuredRecord outputRecord = outputRecords.get(0);
        Assert.assertEquals("toto", outputRecord.get("firstName"));
        Assert.assertEquals("tata", outputRecord.get("lastName"));
        Assert.assertEquals("toto.tata@tutu.com", outputRecord.get("mail"));
        Assert.assertEquals("the_id_value", outputRecord.get("_id"));
        Assert.assertEquals(Arrays.asList("RATkiller 🐭", "Bienvenue 🙋", "Pro amiante"), outputRecord.get("paths"));
    }

    public List<StructuredRecord> testHttpDynamicTransform(String filepath)throws Exception {
        HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
        CloseableHttpResponse mockHttpResponse = Mockito.mock(CloseableHttpResponse.class);
        HttpEntity mockEntity = Mockito.mock(HttpEntity.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);

        String baseURL = "myfakeurl.com/{id}";
        Map<String, String> urlParameters = new HashMap<>();
        urlParameters.put("company", "xx");
        urlParameters.put("apiKey", "XX");
        Map<String, String> urlVariables = new HashMap<>();
        urlVariables.put("id", "_id");
        String idValue = "the_id_value";
        String targetURL = "myfakeurl.com/the_id_value?apiKey=XX&company=xx&";
        Mockito.when(mockHttpClient.executeHTTP(targetURL/*Mockito.any()*/)).thenReturn(mockHttpResponse);
        Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        Mockito.when(mockEntity.getContent()).thenReturn(getClass().getClassLoader().getResourceAsStream(filepath));

        BaseTestConfigHttp config = new BaseTestConfigHttp(
                "HttpDynamicTransform-transform",
                baseURL,
                Joiner.on(",").withKeyValueSeparator(":").join(urlParameters),
                Joiner.on(",").withKeyValueSeparator(":").join(urlVariables),
                10);

        Transform<StructuredRecord, StructuredRecord> transform
                = new DynamicHttpTransform(config, mockHttpClient);

        transform.initialize(null);

        MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

        transform.transform(generateData(idValue), emitter);
        transform.destroy();

        return emitter.getEmitted();
    }

}
