package Hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    // 传入的line，和json的key
    public  String evaluate(String line, String key) {
        // 切割
        String[] log = line.split("\\|");

        // 合法性判断, 长度不对，和json中是空值
        if (log.length != 2 || StringUtils.isBlank(log[1].trim())) {
            return "";
        }

        // 去除json对象来解析
        JSONObject jsonObject = new JSONObject(log[1].trim());

        String result = "";
        // 根据传入的key来取值
        if ("st".equals(key)) {
            // 返回服务器时间
            return log[0].trim();
        }else if ("et".equals(key)) {
            if (jsonObject.has("et")) {
                result = jsonObject.getString("et");
            }
        }else {
            // 获取cm对应的value值
            JSONObject cm = jsonObject.getJSONObject("cm");

            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }
        return "";
    }
}
