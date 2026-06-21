package test.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 使用Jedis连接池操作Redis的示例
 *
 * @author 黑大帅
 * @create 2026/06/21
 */
public class jedis2 {
    
    private static final JedisPool jedisPool;
    
    static {
        // 创建连接池配置
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // 设置最大连接数
        poolConfig.setMaxTotal(10);
        // 设置最大空闲连接数
        poolConfig.setMaxIdle(5);
        // 设置最小空闲连接数
        poolConfig.setMinIdle(5);
        // 设置获取连接时的最大等待时间（毫秒）
        poolConfig.setMaxWaitMillis(2000);
        // 连接耗尽时是否等待
        poolConfig.setBlockWhenExhausted(true);
        // 取连接的时候进行一下有效性检查
        poolConfig.setTestOnBorrow(true);

        // 创建Jedis连接池
        jedisPool = new JedisPool(poolConfig, "hadoop102", 6379);
    }
    
    /**
     * 主方法：演示使用连接池获取Redis数据
     *
     * @param args 命令行参数（未使用）
     */
    public static void main(String[] args) {
        try (Jedis jedis = jedisPool.getResource()) {
            // 从连接池获取Jedis实例


            // 获取set类型的所有成员
            java.util.Set<String> setMembers = jedis.smembers("k3");
            System.out.println("set类型的成员为: " + setMembers);

        } catch (Exception e) {
            System.err.println("Redis操作失败: " + e.getMessage());
            e.printStackTrace();
        }
        // 将连接归还到连接池（不是关闭连接）
    }
}
