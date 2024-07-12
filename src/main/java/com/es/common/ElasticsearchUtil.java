package com.es.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.es.entity.FileObj;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

/**
 * author: 阿杰
 */
@Component
@Slf4j
public class ElasticsearchUtil {


    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private  static  RestHighLevelClient levelClient;

    @PostConstruct
    public void initClient() {
        levelClient = this.restHighLevelClient;
    }

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) throws IOException {
        if (!isIndexExist(index)) {
            log.info("Index is not exists!");
        }

        CreateIndexRequest request = new CreateIndexRequest(index);
        // 添加 IK 分词器设置   ik分词器一定要存在 不然检索会报错
//            request.settings(Settings.builder()
//                    .put("index.analysis.analyzer.default.type", "ik_max_word")
//                    .put("index.analysis.analyzer.default.use_smart", "true")
//            );
        //
//        // 添加 IK 分词器设置 ik_smart 分词会更细一点
//        request.settings(Settings.builder()
//                .put("index.analysis.analyzer.default.type", "ik_smart")
//        );
        CreateIndexResponse response = levelClient.indices().create(request, RequestOptions.DEFAULT);
        log.info("执行建立成功？" + response.isAcknowledged());
        return response.isAcknowledged();

    }

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    public static boolean deleteIndex(String index) throws IOException {
        if (!isIndexExist(index)) {
            log.info("Index is not exists!");
        }
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse response = levelClient.indices().delete(request, RequestOptions.DEFAULT);
        if (response.isAcknowledged()) {
            log.info("delete index " + index + " successfully!");
        } else {
            log.info("Fail to delete index " + index);
        }
        return response.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        boolean exists = levelClient.indices().exists(request, RequestOptions.DEFAULT);
        if (exists) {
            log.info("Index [" + index + "] exists!");
        } else {
            log.info("Index [" + index + "] does not exist!");
        }
        return exists;
    }

    /**
     * 判断index下指定type是否存在
     */
//    public boolean isTypeExist(String index, String type) throws IOException {
//        if (isIndexExist(index)) {
//            TypesExistsRequest request = new TypesExistsRequest(new String[]{index}, new String[]{type});
//            return levelClient.indices().existsType(request, RequestOptions.DEFAULT);
//        } else {
//            return false;
//        }
//    }

    /**
     * 数据添加，指定ID
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type, String id) throws IOException {
        IndexRequest request = new IndexRequest(index, type, id).source(jsonObject);
        IndexResponse response = levelClient.index(request, RequestOptions.DEFAULT);
        log.info("addData response status:{}, id:{}", response.status().getStatus(), response.getId());
        return response.getId();
    }

    /**
     * 数据添加
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type) throws IOException {
        return addData(jsonObject, index, type, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    public static void deleteDataById(String index, String type, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, type, id);
        DeleteResponse response = levelClient.delete(request, RequestOptions.DEFAULT);
        log.info("deleteDataById response status:{}, id:{}", response.status().getStatus(), response.getId());
    }

    /**
     * 通过ID更新数据
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static void updateDataById(JSONObject jsonObject, String index, String type, String id) throws IOException {
        UpdateRequest request = new UpdateRequest(index, type, id).doc(jsonObject);
        levelClient.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 通过ID获取数据
     *
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    public static Map<String, Object> searchDataById(String index, String type, String id, String fields) throws IOException {
        GetRequest request = new GetRequest(index, type, id);
        if (StringUtils.isNotEmpty(fields)) {
            request.fetchSourceContext(new FetchSourceContext(true, fields.split(","), null));
        }
        GetResponse response = levelClient.get(request, RequestOptions.DEFAULT);
        return response.getSourceAsMap();
    }


    /**
     * 使用分词查询,并分页
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param startPage      当前页
     * @param pageSize       每页显示条数
     * @param query          查询条件
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param highlightField 高亮字段
     * @return
     */
    public static EsPage searchDataPage(String index, String type, int startPage, int pageSize, QueryBuilder query, String fields, String sortField, String highlightField) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

// 设置需要显示的字段
        if (StringUtils.isNotEmpty(fields)) {
            sourceBuilder.fetchSource(fields.split(","), null);
        }

// 设置排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            sourceBuilder.sort(sortField, SortOrder.DESC);
        }

// 设置高亮字段
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field highlightFieldBuilder = new HighlightBuilder.Field(highlightField);
            highlightBuilder.field(highlightFieldBuilder);
            sourceBuilder.highlighter(highlightBuilder);
        }

// 设置查询条件
        sourceBuilder.query(query);

// 设置分页
        sourceBuilder.from(startPage);
        sourceBuilder.size(pageSize);

// 是否按查询匹配度进行排序
        sourceBuilder.explain(true);

        searchRequest.source(sourceBuilder);

// 执行搜索
        SearchResponse searchResponse = levelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits().value;
        SearchHit[] searchHits = hits.getHits();

        List<Map<String, Object>> sourceList = new ArrayList<>();

        for (SearchHit hit : searchHits) {
            Map<String, Object> source = hit.getSourceAsMap();

            // 解析高亮字段
            if (StringUtils.isNotEmpty(highlightField)) {
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get(highlightField);
                if (highlight != null) {
                    Text[] fragments = highlight.fragments();
                    StringBuilder stringBuilder = new StringBuilder();
                    for (Text text : fragments) {
                        stringBuilder.append(text);
                    }
                    source.put(highlightField, stringBuilder.toString());
                }
            }

            sourceList.add(source);
        }

        return new EsPage(startPage, pageSize, (int) totalHits, sourceList);

    }


    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param query          查询条件
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param highlightField 高亮字段
     * @return
     */
    public static List<Map<String, Object>> searchListData(
            String index, String type, QueryBuilder query, Integer size,
            String fields, String sortField, String highlightField) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

// 设置需要显示的字段
        if (StringUtils.isNotEmpty(fields)) {
            sourceBuilder.fetchSource(fields.split(","), null);
        }

// 设置排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            sourceBuilder.sort(sortField, SortOrder.DESC);
        }

// 设置高亮字段
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.field(highlightField);
            sourceBuilder.highlighter(highlightBuilder);
        }

// 设置查询条件
        sourceBuilder.query(query);

// 设置文档大小限制
        if (size != null && size > 0) {
            sourceBuilder.size(size);
        }

        searchRequest.source(sourceBuilder);

// 执行搜索
        SearchResponse searchResponse = levelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits().value;
        SearchHit[] searchHits = hits.getHits();

        List<Map<String, Object>> sourceList = new ArrayList<>();

        for (SearchHit hit : searchHits) {
            Map<String, Object> source = hit.getSourceAsMap();

            // 解析高亮字段
            if (StringUtils.isNotEmpty(highlightField)) {
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get(highlightField);
                if (highlight != null) {
                    Text[] fragments = highlight.fragments();
                    StringBuilder stringBuilder = new StringBuilder();
                    for (Text text : fragments) {
                        stringBuilder.append(text);
                    }
                    source.put(highlightField, stringBuilder.toString());
                }
            }

            sourceList.add(source);
        }

        return sourceList;

    }


    /**
     * 高亮结果集 特殊处理
     *
     * @param searchResponse
     * @param highlightField
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        StringBuffer stringBuffer = new StringBuffer();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {

                System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    //遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }
        return sourceList;
    }

    /**
     * 查看索引的分词器
     * @param indexName
     * @return
     * @throws IOException
     */
    public static Map<String, Object> getIndexAnalyzer(String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        GetIndexResponse response = levelClient.indices().get(request, RequestOptions.DEFAULT);
        Settings settings = response.getSettings().get(indexName);
        String analyzerType = settings.get("index.analysis.analyzer.default.type");

        System.out.println("Analyzer type: " + analyzerType);

        // 返回分词器类型
        Map<String, Object> result = new HashMap<>();
        result.put("analyzerType", analyzerType);

        return result;
    }


    /**
     * 创建索引并插入数据
     * @param file
     * @param indexName
     * @return
     * @throws IOException
     */
    public static IndexResponse upload(FileObj file,String indexName) throws IOException {
        // TODO 创建前需要判断当前文档是否已经存在
        if (!isIndexExist(indexName)) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            // 添加 IK 分词器设置  ik_max_word
//            request.settings(Settings.builder()
//                    .put("index.analysis.analyzer.default.type", "ik_max_word")
//                    .put("index.analysis.analyzer.default.use_smart", "true")
//            );

            // 添加 IK 分词器设置 ik_smart
            request.settings(Settings.builder()
                    .put("index.analysis.analyzer.default.type", "ik_smart")
            );
            CreateIndexResponse response = levelClient.indices().create(request, RequestOptions.DEFAULT);
            log.info("执行建立成功？" + response.isAcknowledged());
        }
        IndexRequest indexRequest = new IndexRequest(indexName);
        //上传同时，使用attachment pipline进行提取文件
        indexRequest.source(JSON.toJSONString(file), XContentType.JSON);
        indexRequest.setPipeline("attachment");
        IndexResponse indexResponse= levelClient.index(indexRequest,RequestOptions.DEFAULT);
        System.out.println(indexResponse);
        return indexResponse;
    }

    /**
     * 根据关键词，搜索对应的文件信息
     * 查询文件中的文本内容
     * @param keyword
     * @throws IOException
     */
    public static  List<Map<String, Object>>  search(String keyword,String indexName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);

        //默认会search出所有的东西来
        //SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);


        SearchSourceBuilder srb = new SearchSourceBuilder();
        //多条件查询？
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.matchQuery("name",keyword))
                //使用lk分词器查询，会把插入的字段分词，然后进行处理
                .should(QueryBuilders.matchQuery("attachment.content", keyword).analyzer("ik_smart"));
        srb.query(boolQuery);

        //设置highlighting
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightContent = new HighlightBuilder.Field("attachment.content");
        highlightContent.highlighterType();
        highlightBuilder.field(highlightContent);
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");

        //highlighting会自动返回匹配到的文本，所以就不需要再次返回文本了
        String[] includeFields = new String[]{"id","name"};
        String[] excludeFields = new String[]{"attachment.content"};
        srb.fetchSource(includeFields, excludeFields);

        //把刚才设置的值导入进去
        //srb.highlighter(highlightBuilder);
        searchRequest.source(srb);
        SearchResponse res = levelClient.search(searchRequest,RequestOptions.DEFAULT);

        //获取hits，这样就可以获取查询到的记录了
        SearchHits hits = res.getHits();

        //hits是一个迭代器，所以需要迭代返回每一个hits
        Iterator<SearchHit> iterator = hits.iterator();
        int count = 0;
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            //获取返回的字段
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            //System.out.println(hit.getSourceAsString());
            //统计找到了几条
            count++;
            //这个就会把匹配到的文本返回，而且只返回匹配到的部分文本
           // Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //System.out.println(highlightFields);
           // Map<String, Object> params = hit.getSourceAsMap();
            sourceAsMap.put("_id",hit.getId());
            sourceList.add(sourceAsMap);
        }
        System.out.println("查询到" + count + "条记录");
        // TODO 可以分页展示 new EsPage(startPage, pageSize, (int) totalHits, sourceList)
        return sourceList;
    }
}
