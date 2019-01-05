package com.pt.elasticsearch;

import com.google.common.base.Strings;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.util.*;

/**
 * ES 2.4
 * 批量插入的时候，可能会有部分失败的情况；解决方案：指定ID，如果有部分失败，则全部重新写入。
 * 对于jar包冲突的情况，使用shade插件完成冲突类的重命名
 * <relocations>
 * -       <relocation>
 * -           <pattern>com.google.common</pattern>
 * -           <shadedPattern>com.google.common.shade</shadedPattern>
 * -       </relocation>
 * </relocations>
 */
public class IndexData {
    private static String indexName = "test";
    private static Settings settings = Settings.settingsBuilder()
            .put("cluster.name", "cluster-name")
            .put("client.transport.sniff", true)
            .put("processors", 30)
            .build();

    private static TransportClient client;

    static {
        try {
            client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.38.161.138"), 9300));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("delete：" + deleteIndex(indexName));
        Map<String, Object> mappings = new HashMap<>();
        mappings.put("properties", new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("type", "string");
                put("index", "not_analyzed");
            }});
        }});
        Map<String, Object> settings = new HashMap<String, Object>() {{
            put("index.number_of_replicas", "0");
            put("index.number_of_shards", "3");
            put("index.codec", "best_compression");
        }};
        System.out.println("create:" + createIndex(indexName, settings, mappings));
        Map<String, Object> map = new HashMap<>();
        map.put("name", "luckyPt");
        map.put("age", 20);
        map.put("isMan", true);
        map.put("birthday", "1999-12-09");
        System.out.println("index one data:" + indexData(map));//单条插入

        List<Map<String, Object>> maps = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> tmp = new HashMap<>();
            tmp.put("name", "luckyPt");
            tmp.put("age", 21 + i);
            tmp.put("isMan", i % 2 == 0);
            tmp.put("birthday", "1999-12-09");
            maps.add(tmp);
        }
        System.out.println("index many data:" + indexData(maps));//批量插入
    }

    /**
     * 索引单个数据，如果可以的话，尽量使用批量索引
     *
     * @param map 数据
     * @return 是否索引成功
     */
    public static boolean indexData(Map<String, Object> map) {
        IndexResponse response = client.prepareIndex(indexName, indexName)
                .setSource(map)
                .get();
        return !Strings.isNullOrEmpty(response.getId());
    }

    /**
     * 批量请求可以增加吞吐量
     *
     * @param dataMap 数据
     * @return 是否索引成功
     */
    public static boolean indexData(Collection<Map<String, Object>> dataMap) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Map<String, Object> aDataMap : dataMap) {
            bulkRequest.add(client.prepareIndex(indexName, indexName).setSource(aDataMap));
        }
        BulkResponse responses = bulkRequest.get();
        return !responses.hasFailures();
    }

    public static boolean deleteIndex(String indexName) {
        if (exists(indexName)) {
            DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName)
                    .execute().actionGet();
            return dResponse.isAcknowledged();
        } else {
            return true;
        }
    }

    public static boolean createIndex(String indexName, Map<String, Object> settings, Map<String, Object> mappings) throws Exception {
        CreateIndexRequestBuilder request = client.admin().indices().prepareCreate(indexName);
        request.setSettings(settings);
        request.addMapping(indexName, mappings);
        return request.get().isAcknowledged();
    }

    public static boolean exists(String indexName) {
        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);
        IndicesExistsResponse inExistsResponse = client.admin().indices()
                .exists(inExistsRequest).actionGet();
        return inExistsResponse.isExists();
    }

}
