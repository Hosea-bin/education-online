package com.atguigu.qzpoint.util;

import com.alibaba.fastjson.JSONObject;

//解析json格式的
public class ParseJsonData {

    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }
}
