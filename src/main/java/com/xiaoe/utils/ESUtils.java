package com.xiaoe.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * @author DoubleHu
 * @Description: ES通用工具类
 * @date 2018/10/30 14:11
 */
public class ESUtils implements Serializable {
    private final static Logger log = LoggerFactory.getLogger(ESUtils.class);

    public static RestHighLevelClient getClient(Config config) {
        String hostnames = config.getString("es.hostnames");
        String username = config.getString("es.username");
        String password = config.getString("es.password");
        int port = config.getInt("es.port");
        int timeout = config.getInt("es.timeout");
        String scheme = config.getString("es.scheme");
        log.warn("esconfig=hostnames:{},port:{},scheme:{}", hostnames, port, scheme);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        String[] hosts = hostnames.split(",");
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            HttpHost node = new HttpHost(hosts[i], port, scheme);
            httpHosts[i] = node;
        }
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHosts).setMaxRetryTimeoutMillis(timeout)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
        );
        return client;
    }



    public static JSONObject get(RestHighLevelClient client, String index, String type, String id) {
        GetRequest getRequest = new GetRequest(index, type, id);
        try {
            String[] includes = new String[]{"latest_days_visited_at", "visited_products"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            getRequest.fetchSourceContext(fetchSourceContext);
            GetResponse getResponse = client.get(getRequest);
            if (getResponse.isExists()) {
                return JSON.parseObject(getResponse.getSourceAsString());
            }
        } catch (IOException e) {
            log.error("ESUtil.get() execute fail :{}", e.getMessage());
        }
        return null;
    }

    public static void upsert(RestHighLevelClient client, String index, String type, String id, String jsonString) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index, type, id).source(jsonString, XContentType.JSON);
        UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(jsonString, XContentType.JSON).upsert(indexRequest);
        try {
            client.update(updateRequest);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ESUtil.upsert() execute fail :{}", e.getMessage());
        }
    }



    /**
     * 批量更新
     *
     * @param client
     * @param index
     * @param type
     * @param newDocMap
     * @Depercated 已经弃用
     */
    public static void bulkUpsert(RestHighLevelClient client, String index, String type, Map<String, JSONObject> newDocMap, int batchLimit) {

        log.warn("upsert to es");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("3m");
        int limitCounter = 0;
        for (String id : newDocMap.keySet()) {
            limitCounter++;
            UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(newDocMap.get(id).toJSONString(), XContentType.JSON).upsert(newDocMap.get(id).toJSONString(), XContentType.JSON);
            bulkRequest.add(updateRequest);
            if (limitCounter == batchLimit) {
                limitCounter = 0;
                log.warn("limitCounter={}", limitCounter);
                try {
                    BulkResponse bulkResponse = client.bulk(bulkRequest);
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) {
                            log.warn("upsert es fail {}", bulkItemResponse.getFailureMessage());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    log.warn("upsert fail{},client:{}", e.getMessage(), client.toString());
                }
            }
        }
        log.warn("limitCounter={}", limitCounter);
        try {
            if (limitCounter == 0) return;
            BulkResponse bulkResponse = client.bulk(bulkRequest);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    log.warn("upsert es fail {}", bulkItemResponse.getFailureMessage());
                }
            }
        } catch (Exception e) {
            log.warn("upsert fail{}", e.getMessage());
        }
    }


    /**
     * 批量请求
     *
     * @param client
     * @param multiGetRequest
     * @return response
     */
    public static HashMap<String, JSONObject> multiGet(RestHighLevelClient client, MultiGetRequest multiGetRequest) throws InterruptedException {
        HashMap<String, JSONObject> responseMap = new HashMap<>();

        MultiGetResponse multiGetResponse = null;
        try {
            multiGetResponse = client.multiGet(multiGetRequest);
        } catch (IOException e) {
            Thread.sleep(3000);
            //设置一处重试机制
            int counter = 3;
            while (true) {
                try {
                    counter = counter - 1;
                    multiGetResponse = client.multiGet(multiGetRequest);
                } catch (IOException e1) {

                    if (counter == 0) {
                        log.warn("multiGet fail! message:{}", e1.getMessage());
                        break;
                    } else {
                        log.warn("multiGet retry{}", counter);
                        Thread.sleep(3000);
                        continue;
                    }
                }
                break;
            }
        }
        if (multiGetResponse == null) return responseMap;
        MultiGetItemResponse[] responses = multiGetResponse.getResponses();
        if (responses == null) {
            log.warn("responses == null:{}", responses);
            return responseMap;
        }
        for (int i = 0; i < responses.length; i++) {
            GetResponse response = responses[i].getResponse();
            if (response == null) continue;
            String id = responses[i].getId();
            if (response.isExists()) {
                responseMap.put(id, JSON.parseObject(response.getSourceAsString()));
            }
        }
        return responseMap;
    }


    /**
     * 批量更新
     * @param client
     * @param index
     * @param type
     * @param newDocMap
     * @return
     * @vserion v1.1
     */

    public static boolean batchUpsert(RestHighLevelClient client, String index, String type, Map<String, JSONObject> newDocMap) throws InterruptedException {
        BulkProcessor bulkProcessor = makeBulkProcessor(client);
        for (String id : newDocMap.keySet()) {
            UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(newDocMap.get(id).toJSONString(), XContentType.JSON).upsert(newDocMap.get(id).toJSONString(), XContentType.JSON);
            bulkProcessor.add(updateRequest);
        }

        return bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);
    }

    /**
     * ES批量处理器
     * @param client
     * @return
     */
    public static BulkProcessor makeBulkProcessor(RestHighLevelClient client) {
        BulkProcessor processor = BulkProcessor.builder(client::bulkAsync, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    log.warn(bulkResponse.buildFailureMessage());
                } else {
                    log.warn("Bulk [{}] completed in {} milliseconds,bulk size {},bulk actions {}",
                            l, bulkResponse.getTook().getMillis(),bulkRequest.estimatedSizeInBytes(),bulkRequest.numberOfActions());
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                log.error("cann't execute batch upsert!");
            }
        }).setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setConcurrentRequests(1)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        return processor;
    }
}
