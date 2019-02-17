package com.tom.spark;

import com.google.common.base.Joiner;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.inject.internal.Join;
import org.elasticsearch.common.xcontent.XContentType;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

public class EsClientUtil {

    /**
     * 高阶Rest Client
     */
    private static RestHighLevelClient client;
    /**
     * 低阶Rest Client
     */
    private static RestClient restClient;

    private static BulkProcessor bulkProcessor = null;


    static {
        if (restClient == null) {
            restClient = RestClient.builder(
                    new HttpHost("10.0.0.211", 9200, "http"),
                    new HttpHost("10.0.0.212", 9200, "http"),
                    new HttpHost("10.0.0.213", 9200, "http"),
                    new HttpHost("10.0.0.214", 9200, "http")
            ).build();
        }
    }

    static {
        if (client == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("10.0.0.212", 9200, "http"),
                            new HttpHost("10.0.0.213", 9200, "http"),
                            new HttpHost("10.0.0.214", 9200, "http"),
                            new HttpHost("10.0.0.211", 9200, "http")
                    )
            );
        }
    }


    static {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

            }

            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

            }
        };
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("10.0.0.211", 9200, "http"),
                new HttpHost("10.0.0.212", 9200, "http"),
                new HttpHost("10.0.0.213", 9200, "http"),
                new HttpHost("10.0.0.214", 9200, "http"),
                new HttpHost("10.0.0.215", 9200, "http")
//                new HttpHost("127.0.0.1", 9200, "http")
        ));

        bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener)
//                .setBulkActions(10)
//                .setBulkSize(new ByteSizeValue(50, ByteSizeUnit.MB))
//                .setFlushInterval(TimeValue.timeValueSeconds(5))
//                .setConcurrentRequests(5)
//                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100),3))
                .build();

    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public RestClient getRestClient() {
        return restClient;
    }

    public void addDetail (String preIndex, String jsonStr) throws IOException {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
        String daytime = sdf.format(date);

        JsonObject obj = new JsonParser().parse(jsonStr).getAsJsonObject();
//        ThreadPoolFactory.getExecutorService().execute(() -> {
//            try {
//                String index = preIndex + "-"+daytime;
//                HttpEntity entity = new NStringEntity(jsonStr, ContentType.APPLICATION_JSON);
//                String traceId = obj.get("traceId").getAsString();
//                IndexRequest request = new IndexRequest(
//                        index,       // index name
//                        "doc",  // type
//                        traceId);    // doc id
//
//                request.source(jsonStr, XContentType.JSON);
//                client.index(request);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });

    }

    public static String parseChainTime (String str) {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String timeStr1 = sdf1.format(new Date(Long.parseLong(String.valueOf(str.substring(0, str.length()-3)))));
        return timeStr1;
    }

    public static void bulkAdd(IndexRequest requests) {
        bulkProcessor.add(requests);
    }


    /**
     * 将值存入es中
     *
     * @param json
     */
    public static void bulkListAdd(String json) {

//        JsonObject obj = new JsonParser().parse(json).getAsJsonObject();
//        String traceId = obj.get("traceId").getAsString();
//        String timestamp = obj.get("timestamp").getAsString();
//        String day = getDay(timestamp);
//        String index = "hsq-zipkin-list";//Config.get("hsqzipkin.list-index");
//        IndexRequest request = new IndexRequest(
//                index+"-"+day,
//                "doc",
//                traceId);
////        System.out.println("---->"+json);
//        request.source(json, XContentType.JSON);
//        bulkProcessor.add(request);

    }

    public static String getDay(String timestamp) {
        String timeStr = timestamp.substring(0, timestamp.length()-3);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd");
        long lt = Long.valueOf(timeStr);
        Date dateTime = new Date(lt);
        return simpleDateFormat.format(dateTime);
    }



    public static void bulkDailyAdd(String json) {

        JsonObject obj = new JsonParser().parse(json).getAsJsonObject();
        String day = obj.get("day").getAsString();
        String uuid = md5(obj.get("host").getAsString()+obj.get("api")+day);

//        String index = "hsq-zipkin-";//Config.get("hsqzipkin.daily-index");

        IndexRequest request = new IndexRequest(
                "hsq-zipkin-dailystats",
                "doc",
                uuid);
        request.source(json, XContentType.JSON);
        bulkProcessor.add(request);
    }

    public void closeClient() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String md5(String value){
        String result = null;
        MessageDigest md5 = null;
        try{
            md5 = MessageDigest.getInstance("MD5");
            md5.update((value).getBytes("UTF-8"));
        }catch (NoSuchAlgorithmException error){
            error.printStackTrace();
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        byte b[] = md5.digest();
        int i;
        StringBuffer buf = new StringBuffer("");

        for(int offset=0; offset<b.length; offset++){
            i = b[offset];
            if(i<0){
                i+=256;
            }
            if(i<16){
                buf.append("0");
            }
            buf.append(Integer.toHexString(i));
        }

        result = buf.toString();
        return result;
    }

    public static String getDailyData () throws IOException {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
        String daytime = sdf.format(date);
        String dsl = "{\"size\":10000,\"query\":{\"match\":{\"day\":\""+daytime+"\"}}}";
        HttpEntity entity = new NStringEntity(dsl, ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest("POST", "/hsq-zipkin-dailystats/_search", Collections.<String, String>emptyMap(), entity);
//        System.out.println("date:"+date+"dsl:"+dsl);
        String responseStr = EntityUtils.toString(response.getEntity());
        JsonArray hitsArray = new JsonParser().parse(responseStr).getAsJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();
        List<String> statusList = new ArrayList<>();
        for (int i = 0; i< hitsArray.size(); i++) {
            JsonObject obj = hitsArray.get(i).getAsJsonObject().get("_source").getAsJsonObject();
            String host = obj.get("host").toString();
            String str = obj.toString();
//            Tuple2<String, String> tup = new Tuple2<>(host, str);
            statusList.add(str);
        }

        return Joiner.on("##").join(statusList);
    }
}
