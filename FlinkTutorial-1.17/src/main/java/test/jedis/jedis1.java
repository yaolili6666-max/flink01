package test.jedis;

import redis.clients.jedis.Jedis;

import java.util.Set;

public class jedis1 {
    /**
     * 主方法：连接Redis并获取各种类型的数据
     *
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        // 创建Jedis客户端连接到hadoop102:6379的Redis服务
        Jedis jedis = new Jedis("hadoop102",6379);
    
        // 获取string类型的值
//        String value = jedis.get("k1");
//        System.out.println("k1 的值为: " + value);
        
        //获取hash类型u1的所有字段和值
//        java.util.Map<String, String> u1Map = jedis.hgetAll("u1");
//        System.out.println("u1 的值为: " + u1Map);
        
        // 获取set类型的所有成员
        Set<String> setMembers = jedis.smembers("k3");
        System.out.println("set类型的成员为: " + setMembers);
            
        // 关闭Redis连接释放资源
        jedis.close();
    }
}


