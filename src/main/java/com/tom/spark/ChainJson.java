package com.tom.spark;
//
import java.net.*;
import java.io.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.text.SimpleDateFormat;
import java.util.*;
//
public class ChainJson {
//
//
//    public static String dealCommonMsgJson(String msgStr) {
//        String listStr = "";
//        String resStr = "";
//        List<Map<String, Object>> resList = new ArrayList<>();
//        try {
//            com.alibaba.fastjson.JSONArray jsonArray = JSON.parseArray(msgStr);
//            Integer maxDuration = 0;
//            String traceId = "";
//            String beginTimestamp = "";
//            Integer spanTotal = jsonArray.size();
//
//            HashSet orderIdsSet = new HashSet();
//            HashSet userIdsSet = new HashSet();
//
//            HashMap<String, JsonObject> serviceMaps = new HashMap<>();
////            Gson gson = new Gson();
//            Map<String, Object> chainMap = new HashMap<>();
//
//            List<Map<String, Object>> dailyList = new ArrayList<>();
//            for (int i = 0; i < spanTotal; i++) {
//                JSONObject jsonObject = (JSONObject) jsonArray.get(i);
//                String jsonStr = jsonArray.get(i).toString();
//
//                Integer duration = (Integer) jsonObject.get("duration");
////                System.out.println("=======>"+EsClient.parseChainTime(jsonObject.get("timestamp").toString()));
//                if (i == 0) {
//                    beginTimestamp = jsonObject.get("timestamp").toString();
//                    traceId = jsonObject.get("traceId").toString();
//                }
//                String timestamp = jsonObject.get("timestamp").toString();
//                maxDuration = duration > maxDuration ? duration : maxDuration;
//                String binStr = jsonObject.get("binaryAnnotations").toString();
//                JsonArray binArr = new JsonParser().parse(binStr).getAsJsonArray();
//
//                Integer num = binArr.size();
//                String serviceName = "";
//                Integer subMaxDuration = duration;
//
//                JsonObject chainSubObj = new JsonParser().parse(jsonStr).getAsJsonObject();
//
//                String host = "", path = "";
//                Integer status = 200;
//
//                for (int n = 0; n < num; n++) {
//                    JsonObject jsonObj = binArr.get(n).getAsJsonObject();
//                    if (n == 0) {
//                        Integer subSpanTotal = 1;
//                        HashMap<String, Integer> subMap = new HashMap<>();
//                        serviceName = jsonObj.get("endpoint").getAsJsonObject().get("serviceName").getAsString();
//
//                        if (serviceMaps.containsKey(serviceName)) {
//                            subMaxDuration = serviceMaps.get(serviceName).get("subMaxDuration").getAsInt() > duration ? serviceMaps.get(serviceName).get("subMaxDuration").getAsInt() : duration;
//                            subSpanTotal = serviceMaps.get(serviceName).get("subSpanTotal").getAsInt() + 1;
//                        }
//                        subMap.put("subMaxDuration", subMaxDuration);
//                        subMap.put("subSpanTotal", subSpanTotal);
//                        serviceMaps.put(serviceName, new JsonParser().parse(gson.toJson(subMap)).getAsJsonObject());
////                        System.out.println("serviceName"+serviceName+","+subMaxDuration+","+subSpanTotal);
//                        chainMap.put("serverSpan", serviceMaps);
//                    }
//
//
//                    if (jsonObj.has("key"))  {
//                        String key = jsonObj.get("key").getAsString();
//                        if (key.equals("http.url")) {
//                            String urlstr = jsonObj.get("value").getAsString();
//                            URL url = new URL(urlstr);
//                            host = url.getHost();
//                            path = url.getPath();
//                            URLParser urlParser = URLParser.fromURL(urlstr).compile();
//
//                            try {
//                                String userId = urlParser.getParameter("userId");
//                                if (userId != null) {
//                                    userIdsSet.add(userId);
//                                }
//                                String userIds = urlParser.getParameter("userIds");
//                                if (userIds != null) {
//                                    String[] userArr = userIds.split(",");
//                                    HashSet tmpUserIdSet = new HashSet(Arrays.asList(userArr));
//                                    userIdsSet.addAll(tmpUserIdSet);
//                                }
//                                String orderId = urlParser.getParameter("orderId");
//                                if (orderId != null) {
//                                    orderIdsSet.add(orderId);
//                                }
//                                String orderIds = urlParser.getParameter("orderIds");
//                                if (orderIds != null) {
//                                    String[] orderIdArr = orderIds.split(",");
//                                    HashSet tmpOrderIdSet = new HashSet(Arrays.asList(orderIdArr));
//                                    orderIdsSet.addAll(tmpOrderIdSet);
//                                }
//                            } catch (Exception ex) {
//                                ex.printStackTrace();
//                            }
//                        }
//
//                        if (key.equals("http.status")) {
//                            status = jsonObj.get("value").getAsInt();
//                        }
//                        String day = getDay(timestamp);
//
//                        if (host.length() > 0){
//                            Map<String, Object> dailyMap = new HashMap<>();
//                            dailyMap.put("traceId", traceId);
//                            dailyMap.put("key", host+path);
//                            dailyMap.put("host", host);
//                            dailyMap.put("api", path);
//                            dailyMap.put("duration", duration);
//                            dailyMap.put("status",status);
//                            dailyMap.put("day",day);
//                            dailyList.add(dailyMap);
//                        }
//                    }
//
//                }
//
//                if (host.length() > 0 && status >= 300) {
//                    if (path.length() > 1 && path.substring(0,2).equals("//")) {
//                        path = path.substring(1, path.length());
//                    }
//                    chainSubObj.addProperty("serverCenter", host+path);
//                    chainSubObj.addProperty("status", status);
//                }
//
//                String day = getDay(timestamp);
//                String detailIndex = "hsq-zipkin-detail"; //Config.get("hsqzipkin.detail-index");
//
//                IndexRequest indexRequest = new IndexRequest(detailIndex+"-"+day, "doc");
//
//                EsClientUtil.bulkAdd(indexRequest.source(chainSubObj.toString(), XContentType.JSON));
//            }
//
//            String extStr = "";
//            if (!userIdsSet.isEmpty() || !orderIdsSet.isEmpty()) {
//                if (!userIdsSet.isEmpty()) {
////                    System.out.println("================>"+userIdsSet);
//
//                    chainMap.put("userIds", userIdsSet);
//                }
//                if (!orderIdsSet.isEmpty()) {
//                    chainMap.put("orderIds", orderIdsSet);
//                }
//            }
////            String serverNmaeStr = ",\"serverSpan\":" + gson.toJson(serviceMaps);
//
//
//            chainMap.put("traceId", traceId);
//            chainMap.put("maxDuration", maxDuration);
//            chainMap.put("timestamp", beginTimestamp);
//            chainMap.put("spanTotal", spanTotal);
////            serviceMaps.clear();
//
////            listStr = "{\"traceId\":\"" + traceId + "\",\"maxDuration\":" + maxDuration + ",\"timestamp\":" + beginTimestamp + ",\"spanTotal\":" + spanTotal + extStr + serverNmaeStr + "}";
////            resStr = "{\"daily\":"+gson.toJson(dailyList)+",\"chain\":"+listStr+"}";
//            Map<String, Object> map = new HashMap<>();
//            map.put("daily", dailyList);
//            map.put("chain", chainMap);
//
//            resList.add(map);
//            resStr = gson.toJson(map);
//            userIdsSet.clear();
//            orderIdsSet.clear();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//
////       System.out.println("==========>dealJson:"+ resStr);
////        JKafkaUtil.Producer("hsq-zipkin-list", listStr);
//        return resStr;
//    }
//
//
//
//    /**
//     * 很重要的代码
//     * @param msgStr
//     * @return
//     */
//    public static String dealMsgJson(String msgStr) {
//        String listStr = "";
//        try {
//            com.alibaba.fastjson.JSONArray jsonArray = JSON.parseArray(msgStr);
////            System.out.println("fastJson转换后的数组格式"+jsonArray);
//            Integer maxDuration = 0;
//            String traceId = "";
//            String beginTimestamp = "";
//            Integer spanTotal = jsonArray.size();
//
//            HashSet orderIdsSet = new HashSet();
//            HashSet userIdsSet = new HashSet();
//
//            HashMap<String, JsonObject> serviceMaps = new HashMap<>();
//            Gson gson = new Gson();
//
//            for (int i = 0; i < spanTotal; i++) {
//                JSONObject jsonObject = (JSONObject) jsonArray.get(i);
//                String jsonStr = jsonArray.get(i).toString();
////                System.out.println("从jsonArray上获取的json字符串");
//                Integer duration = (Integer) jsonObject.get("duration");
////                System.out.println("=======>"+EsClient.parseChainTime(jsonObject.get("timestamp").toString()));
//                if (i == 0) {
//                    // 如果i==0的话，获取时间戳和商务id
//                    beginTimestamp = jsonObject.get("timestamp").toString();
//                    traceId = jsonObject.get("traceId").toString();
//                }
//                // 获取最大的时间
//                maxDuration = duration > maxDuration ? duration : maxDuration;
//
//                // 后面处理的主要是binaryAnnotations解析出来的字符串进行处理
//                String binStr = jsonObject.get("binaryAnnotations").toString();
//                JsonArray binArr = new JsonParser().parse(binStr).getAsJsonArray();
//
//                Integer num = binArr.size();
//                String serviceName = "";
//                Integer subMaxDuration = duration;
//
//                JsonObject chainSubObj = new JsonParser().parse(jsonStr).getAsJsonObject();
//
//                String host = "", path = "";
//                Integer status = 200;
//                for (int n = 0; n < num; n++) {
//                    if (n == 0) {
//                        Integer subSpanTotal = 1;
//                        HashMap<String, Integer> subMap = new HashMap<>();
//                        serviceName = binArr.get(n).getAsJsonObject().get("endpoint").getAsJsonObject().get("serviceName").getAsString();
//
//                        if (serviceMaps.containsKey(serviceName)) {
//                            subMaxDuration = serviceMaps.get(serviceName).get("subMaxDuration").getAsInt() > duration ? serviceMaps.get(serviceName).get("subMaxDuration").getAsInt() : duration;
//                            subSpanTotal = serviceMaps.get(serviceName).get("subSpanTotal").getAsInt() + 1;
//                        }
//
//                        subMap.put("subMaxDuration", subMaxDuration);
//                        subMap.put("subSpanTotal", subSpanTotal);
//                        serviceMaps.put(serviceName, new JsonParser().parse(gson.toJson(subMap)).getAsJsonObject());
//                        subMap.clear();
//                    }
//
//                    if (binArr.get(n).getAsJsonObject().has("key"))  {
//                        String key = binArr.get(n).getAsJsonObject().get("key").getAsString();
//                        if (key.equals("http.url")) {
//                            String urlstr = binArr.get(n).getAsJsonObject().get("value").getAsString();
//                            URL url = new URL(urlstr);
//                            host = url.getHost();
//                            path = url.getPath();
//                            URLParser urlParser = URLParser.fromURL(urlstr).compile();
//
//                            try {
//                                String userId = urlParser.getParameter("userId");
//                                if (userId != null) {
//                                    userIdsSet.add(userId);
//                                }
//                                String userIds = urlParser.getParameter("userIds");
//                                if (userIds != null) {
//                                    String[] userArr = userIds.split(",");
//                                    HashSet tmpUserIdSet = new HashSet(Arrays.asList(userArr));
//                                    userIdsSet.addAll(tmpUserIdSet);
//                                }
//                                String orderId = urlParser.getParameter("orderId");
//                                if (orderId != null) {
//                                    orderIdsSet.add(orderId);
//                                }
//                                String orderIds = urlParser.getParameter("orderIds");
//                                if (orderIds != null) {
//                                    String[] orderIdArr = orderIds.split(",");
//                                    HashSet tmpOrderIdSet = new HashSet(Arrays.asList(orderIdArr));
//                                    orderIdsSet.addAll(tmpOrderIdSet);
//                                }
//                            } catch (Exception ex) {
//                                ex.printStackTrace();
//                            }
//                        }
//
//                        if (key.equals("http.status")) {
//                            status = binArr.get(n).getAsJsonObject().get("value").getAsInt();
//                        }
//
//                    }
//                }
//
//                if (host.length() > 0 && status >= 300) {
//                    if (path.length() > 1 && path.substring(0,2).equals("//")) {
//                        path = path.substring(1, path.length());
//                    }
//                    chainSubObj.addProperty("serverCenter", host+path);
//                    chainSubObj.addProperty("status", status);
//                }
//
//                String timestamp = jsonObject.get("timestamp").toString();
//                String id = jsonObject.get("id").toString();
//                String day = getDay(timestamp);
//                IndexRequest indexRequest = new IndexRequest("hsq-zipkin-detail-"+day, "doc", id);
//
//                EsClientUtil.bulkAdd(indexRequest.source(chainSubObj.toString(), XContentType.JSON));
//            }
//
//
//            String extStr = "";
//            if (!userIdsSet.isEmpty() || !orderIdsSet.isEmpty()) {
//
//                String userIdsJson = gson.toJson(userIdsSet);
//                String orderIdsJson = gson.toJson(orderIdsSet);
//                if (!userIdsJson.isEmpty()) {
//                    extStr += ",\"userIds\":" + userIdsJson;
//                }
//                if (!orderIdsJson.isEmpty()) {
//                    extStr += ",\"orderIds\":" + orderIdsJson;
//                }
//            }
//            String serverNmaeStr = ",\"serverSpan\":" + gson.toJson(serviceMaps);
//            serviceMaps.clear();
//            listStr = "{\"traceId\":\"" + traceId + "\",\"maxDuration\":" + maxDuration + ",\"timestamp\":" + beginTimestamp + ",\"spanTotal\":" + spanTotal + extStr + serverNmaeStr + "}";
//
//            userIdsSet.clear();
//            orderIdsSet.clear();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//
////        System.out.println("--------->dealJson:"+ listStr);
////        JKafkaUtil.Producer("hsq-zipkin-list", listStr);
//        return listStr;
//    }
//
//
//    public static String flatMapJson(String msgStr) {
//        String resStr = "";
//        List<String> resList = new ArrayList<>();
//
//        try {
//            com.alibaba.fastjson.JSONArray jsonArray = JSON.parseArray(msgStr);
//
//            Integer spanTotal = jsonArray.size();
//            for (int i = 0; i < spanTotal; i++) {
//                JSONObject jsonObject = (JSONObject) jsonArray.get(i);
//                String jsonStr = jsonArray.get(i).toString();
//                JsonArray binArr = new JsonParser().parse(jsonStr).getAsJsonObject().get("binaryAnnotations").getAsJsonArray();
//                String  traceId  = jsonObject.get("traceId").toString();
//                Integer duration = (Integer) jsonObject.get("duration");
//                Integer status = 200;
//                String  host = "";
//                String  path = "";
//                String timestamp = jsonObject.get("timestamp").toString();
//                String day = getDay(timestamp);
//
//                Integer num = binArr.size();
//                for (int n = 0; n < num; n++) {
//                    if (binArr.get(n).getAsJsonObject().has("key"))  {
//                        if (binArr.get(n).getAsJsonObject().get("key").getAsString().equals("http.url")){
//                            String urlStr = binArr.get(n).getAsJsonObject().get("value").getAsString();
//                            URL url = new URL(urlStr);
//                            host = url.getHost();
//                            path = url.getPath();
////                            System.out.println("===================>"+path.substring(0,2)+">>"+path);
//                            if (path.length() > 1 && path.substring(0,2).equals("//")) {
//                                path = path.substring(1, path.length());
//                            }
//                        }
//                        if (binArr.get(n).getAsJsonObject().get("key").getAsString().equals("http.status")){
//                            status = binArr.get(n).getAsJsonObject().get("value").getAsInt();
//                        }
//                    }
//                }
//
//                if (host.length() > 0){
//                    resList.add("{\"traceId\":\"" + traceId + "\",\"key\":\""+host+path+day+"\",\"host\":\"" + host + "\",\"api\":\"" + path + "\",\"duration\":\""+duration+"\",\"status\":\""+status+"\",\"day\":\""+day+"\"}");
//                }
//            }
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//
////        System.out.println("==========>dealJson:"+ resList);
////        JKafkaUtil.Producer("hsq-zipkin-list", listStr);
//        return Joiner.on("##").join(resList);
//    }
//
//    /**
//     * 第一次聚合，consumer kafka
//     * @param chainStr
//     * @return
//     */
//    public static String dealKafkaJson(String chainStr, String type) {
////        long dealKafkaJsonStartTime = System.currentTimeMillis();
//        String resStr = "";
//
//        if (chainStr != null && !chainStr.isEmpty()) {
////            System.out.println("chainStr原始数据"+chainStr);
////            System.out.println("chainStr切分后的数据："+chainStr.substring(0,1));
//            if (chainStr.substring(0,1).equals("{")) {
//                JsonObject obj = new JsonParser().parse(chainStr).getAsJsonObject();
////                System.out.println("JsonObject解析后的数据："+obj);
//                if (obj.has("message")) {
//                    String msg = obj.get("message").toString();
////                    System.out.println("message切割后的："+msg);
//                    if (msg.substring(0,1).equals("[")){
//                        resStr =  type.equals("chain") ? dealMsgJson(msg) : flatMapJson(msg);
////                        resStr = dealCommonMsgJson(msg);
//                    }
//                }
//            }else if (chainStr.substring(0,1).equals("[")) {
//                resStr = type.equals("chain") ? dealMsgJson(chainStr) : flatMapJson(chainStr);
////                resStr = dealCommonMsgJson(chainStr);
//            }else {
//                System.out.println("============++++++>"+ chainStr);
//            }
//        }
////        long dealKafkaJsonEndTime = System.currentTimeMillis();
////        System.out.println("dealKafkaJsonEndTime的时间间隔为："+(dealKafkaJsonEndTime-dealKafkaJsonStartTime)+"现在时间为："+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date));
//
//        return resStr;
//    }
//
//    public static String dealKafkaCommonJson(String chainStr) {
//        String resStr = "";
//
//        if (chainStr != null && !chainStr.isEmpty()) {
//            if (chainStr.substring(0,1).equals("{")) {
//                JsonObject obj = new JsonParser().parse(chainStr).getAsJsonObject();
//                if (obj.has("message")) {
//                    String msg = obj.get("message").toString();
//                    if (msg.substring(0,1).equals("[")){
//                        resStr = dealCommonMsgJson(msg);
//                    }
//                }
//            }else if (chainStr.substring(0,1).equals("[")) {
//                resStr = dealCommonMsgJson(chainStr);
//            }else {
//                System.out.println("============++++++>"+ chainStr);
//            }
//        }
//        return resStr;
//    }
//
//    public static String dealKafkaJson2(String chainStr) {
//        String span = "";
////        System.out.println(chainStr);
//        if (chainStr != null && !chainStr.isEmpty()) {
//            if (chainStr.substring(0,1).equals("{")) {
//                JsonObject obj = new JsonParser().parse(chainStr).getAsJsonObject();
//                if (obj.has("message")) {
//                    String msg = obj.get("message").toString();
//                    if (msg.substring(0,1).equals("[")){
//                        span = flatMapJson(msg);
//                    }
//                }
//            }else if (chainStr.substring(0,1).equals("[")) {
//                span =  flatMapJson(chainStr);
//            }else {
//                System.out.println("============++++++>"+ chainStr);
//            }
//        }
//        return span;
//    }
//
//    public static String dealKafkaJson3(String chainStr) {
//        String span = "";
////        System.out.println(chainStr);
//        if (chainStr != null && !chainStr.isEmpty()) {
//            if (chainStr.substring(0,1).equals("{")) {
//                JsonObject obj = new JsonParser().parse(chainStr).getAsJsonObject();
//                if (obj.has("message")) {
//                    String msg = obj.get("message").toString();
//                    if (msg.substring(0,1).equals("[")){
//                        span = msg;
//                    }
//                }
//            }else if (chainStr.substring(0,1).equals("[")) {
//                span =  chainStr;
//            }else {
//                System.out.println("============++++++>"+ chainStr);
//            }
//        }
//        return span;
//    }
//
//
//    public static Map<String, Integer> getServerSpanStr(JsonObject obj, String key) {
//
//        Map<String, Integer> map = new HashMap();
//        if (obj.has(key)) {
//            Integer nowNum = obj.get(key).getAsJsonObject().get("subSpanTotal").getAsInt();
//            Integer nowDurationNum = obj.get(key).getAsJsonObject().get("subMaxDuration").getAsInt();
//            map.put("subSpanTotal", nowNum);
//            map.put("subMaxDuration", nowDurationNum);
//        }
//
//        return map;
//    }
//
//    public static JsonArray getJsonArryByKey(JsonObject obj, String key) {
//        return  obj.has(key) ? obj.get(key).getAsJsonArray() : new JsonArray();
//    }
//
//
//    /**
//     * 调用链全局状态更新
//     * @param preVal
//     * @param nowVal
//     * @return
//     */
//    public static String upsertJson(String preVal, String nowVal) {
//        if (preVal.isEmpty()) {
//            preVal = "{\"traceId\":\"0\",\"maxDuration\":0,\"timestamp\":0, \"spanTotal\":0, \"userIds\":[],\"orderIds\":[],\"serverSpan\":{}}";
//        }
//        Gson gson = new Gson();
//
//        JsonObject preObj = (JsonObject) new JsonParser().parse(preVal);
//        System.out.println("preObj数据："+preObj.toString());
//       // JsonArray preUserIds  = getJsonArryByKey(preObj,"userIds");
//        //JsonArray preOrderIds = getJsonArryByKey(preObj,"orderIds");
//
//        JsonObject nowObj     = (JsonObject) new JsonParser().parse(nowVal);
//        JsonArray nowUserIds  = getJsonArryByKey(nowObj,"userIds");
//        JsonArray nowOrderIds = getJsonArryByKey(nowObj,"orderIds");
//
//        Integer preDuration = preObj.get("maxDuration").getAsInt();
//        Integer nowDuration = nowObj.get("maxDuration").getAsInt();
//        Integer maxDuration = nowDuration > preDuration ? nowDuration : preDuration;
//
//        Integer spanTotal = preObj.get("spanTotal").getAsInt() + nowObj.get("spanTotal").getAsInt();
//
//        //userIds
//        String userIdsStr = "";
////        nowUserIds.addAll(nowUserIds);
//
//        Set userIdsSet = new HashSet();
//        for (int i=0; i < nowUserIds.size(); i++) {
//            userIdsSet.add(nowUserIds.get(i));
//        }
//
//        if (!userIdsSet.isEmpty()) {
//            userIdsStr = ",\"userIds\":" + gson.toJson(userIdsSet);
//        }
//
//        //orderIds
//        String orderIdsStr = "";
////        preOrderIds.addAll(nowOrderIds);
//        Set orderIdsSet = new HashSet();
//        for (int i=0; i< nowOrderIds.size(); i++) {
//            orderIdsSet.add(nowOrderIds.get(i));
//        }
//
//        if (!orderIdsSet.isEmpty()) {
//            orderIdsStr = ",\"orderIds\":" + gson.toJson(orderIdsSet);
//        }
//
//        //serverSpan
//        String serverSpanStr = "";
////        JsonObject preServerSpanObj = preObj.getAsJsonObject("serverSpan");
////        String preServerSpanObjStr = preServerSpanObj.toString();
////        Set preKeys = JSON.parseObject(preServerSpanObjStr).keySet();
//
//        JsonObject nowServerSpanObj = nowObj.getAsJsonObject("serverSpan");
//        String nowServerSpanObjStr = nowServerSpanObj.toString();
//        // 取所有的key放到一个set集合里面
//        Set nowKeys = JSON.parseObject(nowServerSpanObjStr).keySet();
//
//        Set resKeys = new HashSet();
//        resKeys.clear();
////        resKeys.addAll(preKeys);
//        resKeys.addAll(nowKeys);
//        Map<String, Map<String, Integer>> serverSpan = new HashMap<>();
//        for (Object k : resKeys) {
//            String key = k.toString();
//
//            if (nowServerSpanObj.has(key)) {
//                String SpanStr = "";
//                Map<String, Integer> map1 = new HashMap();
//                Integer preSpanNum = 0; //preServerSpanObj.get(key).getAsJsonObject().get("subSpanTotal").getAsInt();
//                Integer nowSpanNum = nowServerSpanObj.get(key).getAsJsonObject().get("subSpanTotal").getAsInt();
//                Integer maxSpanNum = preSpanNum > nowSpanNum ? preSpanNum : nowSpanNum;
//
//                Integer preDurationNum = 10; //preServerSpanObj.get(key).getAsJsonObject().get("subMaxDuration").getAsInt();
//                Integer nowDurationNum = nowServerSpanObj.get(key).getAsJsonObject().get("subMaxDuration").getAsInt();
//                Integer maxDurationNum = preDurationNum > nowDurationNum ? preDurationNum : nowDurationNum;
//                map1.put("subSpanTotal", maxSpanNum);
//                map1.put("subMaxDuration", maxDurationNum);
//                serverSpan.put(key, map1);
//            }
//
////            if (!nowServerSpanObj.has(key)) {
////                Map<String, Integer> map2 = getServerSpanStr(preServerSpanObj, key);
////                if(!map2.isEmpty()){
////                    serverSpan.put(key, map2);
////                }
////            }
//
//            if (nowServerSpanObj.has(key)) {
//                Map<String, Integer> map3 = getServerSpanStr(nowServerSpanObj, key);
//                if(!map3.isEmpty()){
//                    serverSpan.put(key, map3);
//                }
//            }
//        }
//
//        serverSpanStr = ",\"serverSpan\":"+gson.toJson(serverSpan);
//
//
//        String resStr = "{\"traceId\":"+nowObj.get("traceId").toString()+",\"maxDuration\":"+maxDuration+"," +
//                "\"timestamp\":"+nowObj.get("timestamp").toString()+",\"spanTotal\":"+spanTotal+userIdsStr+orderIdsStr+serverSpanStr+"}";
//        return resStr;
//    }
//
//
//
//    /**
//     * 每日统计 ，用于streamingDailyStats
//     * @param preVal
//     * @param nowVal
//     * @return
//     */
//    public static String upsertDailyJson(String preVal, String nowVal) {
//
//        JsonObject preObj = (JsonObject) new JsonParser().parse(preVal);
//        JsonObject nowObj = (JsonObject) new JsonParser().parse(nowVal);
//
//        Long access = preObj.get("access").getAsLong()+1;
//
//        Long maxDuration = preObj.get("maxDuration").getAsLong();
//        Long nowDuration = nowObj.get("duration").getAsLong();
//        Long duration      = nowDuration > maxDuration ? nowDuration : maxDuration;
//        Long sumDuration = preObj.get("sumDuration").getAsLong() + nowDuration;
////        if(nowObj.get("api").getAsString().equals("/delivery/getdeliverycheckrecordlist")){
////            JKafkaUtil.Producer("hsq-consumer-log", "sumDuration:"+sumDuration.toString()+">>>>,access:"+access+",>>>>>avg:"+(sumDuration/access));
////        }
//
//        Long success = preObj.get("success").getAsLong();
//        Long fail = preObj.get("fail").getAsLong();
//        String nowStatus = nowObj.get("status").getAsString();
//
//        if(nowStatus.substring(0, 1).equals("2")){
//            success+= 1;
//        }else {
//            fail+= 1;
////            System.out.println("=======>fail:"+nowObj.get("host")+">>"+nowObj.get("api")+">>"+nowStatus+">>fail:"+fail);
//        }
//
//        String daytime = nowObj.get("day").getAsString();
//
//        String resStr = "{\"host\":"+nowObj.get("host")+",\"api\":"+nowObj.get("api")+",\"access\":"+access+",\"sumDuration\":"+sumDuration+",\"maxDuration\":"+duration+"," +"\"success\":"+success+",\"fail\":"+fail+",\"day\":\""+daytime+"\"}";
////        SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
////        String daytime2 = df.format(date);
//
//        return resStr;
//    }
//
//
//
//
//    //TODO 从redis 批量获取数据，处理后批量更新
//    public static String upsertByRedis(String jsonStr, String key) {
//
//        String state = "";
//        if (RedisUtil.getJedis().exists("hsq_list_" +key)) {
//             state = RedisUtil.getJedis().get("hsq_list_" +key);
//        }
//        return upsertJson(state, jsonStr);
//    }
//
//    public static String getParam(String url, String name) {
//        String params = url.substring(url.indexOf("?") + 1, url.length());
//        Map<String, String> split = Splitter.on("&").withKeyValueSeparator("=").split(params);
//        return split.get(name);
//    }
//
    public static String getDay(String timestamp) {
        String timeStr = timestamp.substring(0, timestamp.length()-3);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd");
        long lt = Long.valueOf(timeStr);
        Date dateTime = new Date(lt);
        return simpleDateFormat.format(dateTime);
    }
}
