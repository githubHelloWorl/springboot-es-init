package com.es.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.es.common.ElasticsearchUtil;
import com.es.common.EsPage;
import com.es.common.RandomNameGenerator;
import com.es.entity.FileObj;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * author: 阿杰
 */
@RestController
@RequestMapping("/es")
@Api(value = "es检索",tags = {"es检索"})
public class EsController {


    /**
     * 类型
     */
    private String esType = "_doc";

    /**
     * 创建索引
     * @return
     */
    @GetMapping("/createIndex")
    @ApiOperation(value="创建索引-只创建索引", notes="创建索引-只创建索引")
    public String createIndex(@RequestParam("indexName") String indexName) throws IOException {
        if (!ElasticsearchUtil.isIndexExist(indexName)) {
            ElasticsearchUtil.createIndex(indexName);
        } else {
            return "索引已经存在";
        }
        return "索引创建成功";
    }

    /**
     * 创建索引
     * @return
     */
    @GetMapping("/queryIndexType")
    @ApiOperation(value="查询指定索引的分词器", notes="查询指定索引的分词器")
    public Object queryIndexType(@RequestParam("indexName") String indexName) throws IOException {
       return ElasticsearchUtil.getIndexAnalyzer(indexName);
    }

    /**
     * 插入记录
     *
     * @return
     */
    @GetMapping("/insertJson")
    @ApiOperation(value="创建索引-并且定义字段信息", notes="创建索引-并且定义字段信息")
    public String insertJson(@RequestParam("indexName")String indexName) throws IOException {
        if (StringUtils.isEmpty(indexName))
            return "索引名称不能为null";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", System.currentTimeMillis());
        jsonObject.put("age", 25);
        jsonObject.put("name", "测试数据");
        String str="文件base64";
        jsonObject.put("content",  Base64.getEncoder().encodeToString(str.getBytes()));
        jsonObject.put("date", new Date());
        String id = ElasticsearchUtil.addData(jsonObject, indexName, esType, jsonObject.getString("id"));
        return id;
    }


    /**
     * 文档内容检索
     * @param context
     * @param indexName
     * @return
     */
    @GetMapping("/getFile")
    @ApiOperation(value="检索ES-传入检索索引名称-检索内容", notes="检索ES-传入检索索引名称-检索内容")
    public Object getFileData(String context,@RequestParam("indexName")String indexName){
        try {
            if (StringUtils.isEmpty(context) || StringUtils.isEmpty(indexName))
                return "索引名称和检索内容都不能为null";
           return ElasticsearchUtil.search(context,indexName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将文件 文档信息储存到数据中
     * @param file
     * @return
     */
    @PostMapping("/insertFile")
    @ApiOperation(value="创建索引ES-传入ES索引-传入文件", notes="创建索引ES-传入ES索引-传入文件")
    public IndexResponse insertFile(@RequestAttribute("file") MultipartFile file,@RequestParam("indexName")String indexName){
        FileObj fileObj = new FileObj();
        fileObj.setId(String.valueOf(System.currentTimeMillis()));
        fileObj.setName(file.getOriginalFilename());
        fileObj.setType(file.getName().substring(file.getName().lastIndexOf(".") + 1));
        fileObj.setCreateBy(RandomNameGenerator.generateRandomName());
        fileObj.setCreateTime(String.valueOf(System.currentTimeMillis()));
        fileObj.setAge(RandomNameGenerator.getAge());
        fileObj.setMoney(RandomNameGenerator.getMoney());
        // 文件转base64
        byte[] bytes = new byte[0];
        try {
            bytes = file.getBytes();
            //将文件内容转化为base64编码
            String base64 = Base64.getEncoder().encodeToString(bytes);
            fileObj.setContent(base64);

           IndexResponse indexResponse=  ElasticsearchUtil.upload(fileObj,indexName);
            if (0==indexResponse.status().getStatus()){
                // 索引创建并插入数据成功
                System.out.println("索引创建并插入数据成功");
            }
            return indexResponse;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除记录
     *
     * @return
     */
    @GetMapping("/delete")
    public String delete(String id,String indexName) throws IOException {
        if (StringUtils.isNotBlank(id)) {
            ElasticsearchUtil.deleteDataById(indexName, esType, id);
            return "删除id=" + id;
        } else {
            return "id为空";
        }
    }

    /**
     * 更新数据
     *
     * @return
     */
    @GetMapping("/update")
    public String update(String id,String indexName) throws IOException {
        if (StringUtils.isNotBlank(id)) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", id);
            jsonObject.put("age", 31);
            jsonObject.put("name", "修改");
            jsonObject.put("date", new Date());
            ElasticsearchUtil.updateDataById(jsonObject, indexName, esType, id);
            return "id=" + id;
        } else {
            return "id为空";
        }
    }

    /**
     * 获取数据
     * http://127.0.0.1:8080/es/getData?id=2018-04-25%2016:33:44
     *
     * @param id
     * @return
     */
    @GetMapping("/getData")
    public String getData(String id,String indexName) throws IOException {
        if (StringUtils.isNotBlank(id)) {
            Map<String, Object> map = ElasticsearchUtil.searchDataById(indexName, esType, id, null);
            return JSONObject.toJSONString(map);
        } else {
            return "id为空";
        }
    }

    /**
     * 查询数据
     * 模糊查询
     *
     * @return
     */
    @GetMapping("/queryMatchData")
    public String queryMatchData(@RequestParam("indexName")String indexName) throws IOException {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolean matchPhrase = false;
        if (matchPhrase == Boolean.TRUE) {
            //不进行分词搜索
            boolQuery.must(QueryBuilders.matchPhraseQuery("first_name", "cici"));
        } else {
            boolQuery.must(QueryBuilders.matchQuery("last_name", "cici"));
        }
        List<Map<String, Object>> list = ElasticsearchUtil.
                searchListData(indexName, esType, boolQuery, 10, "first_name", null, "last_name");
        return JSONObject.toJSONString(list);
    }

    /**
     * 通配符查询数据
     * 通配符查询 ?用来匹配1个任意字符，*用来匹配零个或者多个字符
     *
     * @return
     */
    @GetMapping("/queryWildcardData")
    public String queryWildcardData(String indexName) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("first_name.keyword", "cici");
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, queryBuilder, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 正则查询
     *
     * @return
     */
    @GetMapping("/queryRegexpData")
    public String queryRegexpData(String indexName) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.regexpQuery("first_name.keyword", "m--[0-9]{1,11}");
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, queryBuilder, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 查询数字范围数据
     *
     * @return
     */
    @GetMapping("/queryIntRangeData")
    public String queryIntRangeData(String indexName) throws IOException {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("age").from(24)
                .to(25));
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, boolQuery, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 查询日期范围数据
     *
     * @return
     */
    @GetMapping("/queryDateRangeData")
    public String queryDateRangeData(String indexName) throws IOException {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("age").from("20")
                .to("50"));
        List<Map<String, Object>> list = ElasticsearchUtil.searchListData(indexName, esType, boolQuery, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }

    /**
     * 查询分页
     *
     * @param startPage 第几条记录开始
     *                  从0开始
     *                  第1页 ：http://127.0.0.1:8080/es/queryPage?startPage=0&pageSize=2
     *                  第2页 ：http://127.0.0.1:8080/es/queryPage?startPage=2&pageSize=2
     * @param pageSize  每页大小
     * @return
     */
    @GetMapping("/queryPage")
    public String queryPage(String startPage, String pageSize,String indexName) throws IOException {
        if (StringUtils.isNotBlank(startPage) && StringUtils.isNotBlank(pageSize)) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            boolQuery.must(QueryBuilders.rangeQuery("age").from("20")
                    .to("100"));
            EsPage list = ElasticsearchUtil.searchDataPage(indexName, esType, Integer.parseInt(startPage), Integer.parseInt(pageSize), boolQuery, null, null, null);
            return JSONObject.toJSONString(list);
        } else {
            return "startPage或者pageSize缺失";
        }
    }
}
