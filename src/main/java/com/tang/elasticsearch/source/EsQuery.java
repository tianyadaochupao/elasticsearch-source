package com.tang.elasticsearch.source;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsQuery {
    private static String hosts="http://192.168.32.61:39200;http://192.168.32.62:39200;http://192.168.32.63:39200";
    private static String username="elastic";
    private static String password="elastic";
    private static String index="test_t_sms_send_20210801";
    static RestClient client;
    private static Integer fetch_size;
    private static int subTaskIndex = 0;
    private static int parallelNum = 0;
    private static Long total;
    private static Long totalPage;

    public static void main(String[] args) throws Exception{
        if (fetch_size == null) {
            fetch_size = 1000;
        }
        initClient();
        // 数据源获取
        try {
            // 修改索引配置 可以查询超10000
            Request settingRequest = new Request("PUT","/"+index+"/_settings");
            String json = "{\"max_result_window\":\"2147483647\"}";
            settingRequest.setJsonEntity(json);
            client.performRequest(settingRequest);
            // 初始化分页
            initPage();
            int from = 0;
            int size = fetch_size;
            Request request = new Request("GET","/"+index+"/_search");
            for (int page = 0; page < totalPage; page++) {
                from = (page + 1) * size;
                String queryJson = "{\n" +
                        "  \"query\": {\n" +
                        "    \"match_all\": {}\n" +
                        "  },\n" +
                        "  \"from\":"+from+",\n" +
                        "  \"size\":"+size+",\n" +
                        "  \"track_total_hits\":true\n" +
                        "}";
                request.setJsonEntity(queryJson);
                Response response = client.performRequest(request);
                String responseBody = EntityUtils.toString(response.getEntity());
                Map<String,JSONObject> map = JSONObject.parseObject(responseBody, Map.class);
                JSONObject jsonObject = map.get("hits");
                if(null==jsonObject){
                    return;
                }
                JSONArray array = jsonObject.getJSONArray("hits");
                List<String> searchList = array.toJavaList(String.class);
                searchList.stream().forEach(sourceAsString->{
                    JSONObject jsonObject1 = JSONObject.parseObject(sourceAsString);
                    Object msg_id = jsonObject1.getJSONObject("_source").get("msg_id");
                    Object table_time = jsonObject1.getJSONObject("_source").get("table_time");
                    System.out.println(sourceAsString);
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getStackTrace(e));
        } finally {
        }
    }
    /**
     * @Description: 获取总数
     * @author tang
     * @date 2021/10/31 16:09
     */
    public static void initPage() throws IOException {
        // 查询索引总数据
        Request request = new Request("GET","/"+index+"/_search");
        String queryJson = "{\n" +
                "  \"query\": {\n" +
                "    \"match_all\": {}\n" +
                "  },\n" +
                "  \"track_total_hits\":true\n" +
                "}";
        request.setJsonEntity(queryJson);
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String,JSONObject> map = JSONObject.parseObject(responseBody, Map.class);
        JSONObject jsonObject = map.get("hits");
        if(null==jsonObject){
            total = 0L;
            return;
        }
        total = jsonObject.getJSONObject("total").getLong("value");
        totalPage = total % fetch_size == 0 ? total / fetch_size : (total / fetch_size) + 1;
    }

    /**
     * @Description: 初始化客户端
     * @author tang
     * @date 2021/10/31 16:08
     */
    public static void initClient() {
        String[] split = hosts.split(";");
        HttpHost[] hosts = new HttpHost[split.length];
        for(int i=0;i<split.length;i++){
            String url = split[i];
            String[] s = url.split(":");
            String host = s[1].replaceAll("/", "");
            int port = Integer.parseInt(s[2]);
            String scheme = s[0];
            HttpHost httpHost = new HttpHost(host,port,scheme);
            hosts[i] = httpHost;
        }
        //设置密码
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        //设置超时
        RestClientBuilder builder = RestClient.builder(hosts).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(-1);
                requestConfigBuilder.setSocketTimeout(-1);
                requestConfigBuilder.setConnectionRequestTimeout(-1);
                return requestConfigBuilder;
            }
        }).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        client = builder.build();
    }
}
