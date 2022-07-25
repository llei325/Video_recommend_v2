package com.imooc.controller;

import com.alibaba.fastjson.JSONObject;
import com.imooc.utils.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * 数据接口V1.0
 * Created by xuwei
 */
@RestController//控制器类
@RequestMapping("/v1")//映射路径
public class DataController {
    private static final Logger logger = LoggerFactory.getLogger(DataController.class);
    /**
     * 测试接口
     * @param name
     * @return
     */
    @RequestMapping(value="/t1",method = RequestMethod.GET)
    public String test(@RequestParam("name") String name) {

        return "hello,"+name;
    }

    /**
     * 根据主播uid查询三度关系推荐列表数据
     *
     * 返回数据格式：
     * {"flag":"success/error","msg":"错误信息","rec_uids":["1005","1004"]}
     * @param uid
     * @return
     */
    @RequestMapping(value="/get_recommend_list",method = RequestMethod.GET)
    public JSONObject getRecommendList(@RequestParam("uid") String uid) {
        JSONObject resobj = new JSONObject();
        String flag = "success";
        String msg = "ok";
        try{
            Jedis jedis = RedisUtils.getJedis();
            //获取待推荐列表数据
            List<String> uidList = jedis.lrange("l_rec_" + uid, 0, -1);
            String[] uidArr = uidList.toArray(new String[0]);
            resobj.put("rec_uids",uidArr);
        }catch (Exception e){
            flag = "error";
            msg = e.getMessage();
            logger.error(msg);
        }
        resobj.put("flag",flag);
        resobj.put("msg",msg);

        return resobj;
    }





}
