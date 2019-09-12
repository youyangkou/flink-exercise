package com.kouyy.flink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class RedisUtil {

    // 添加线程局部变量
    private static ThreadLocal<JedisCluster> local = new ThreadLocal<JedisCluster>();
    private static ThreadLocal<Jedis> writeJedis_local = new ThreadLocal<Jedis>();
    private static ThreadLocal<Jedis> readJedis_local = new ThreadLocal<Jedis>();

    /**
     * 得到jedisCluster对象
     * @return
     */
    public static JedisCluster getJedisCluster()  {
        // 从本地线程变量ThreadLocal中获取连接
        JedisCluster jedisCluster = local.get();
        // 是第一次获取
        if (jedisCluster == null) {
            // 从连接池获得一个连接
            jedisCluster = RedisClusterPool.getJedisClusterObject();
            // 将连接添加到本地线程变量中共享
            local.set(jedisCluster);
        }
        return jedisCluster;
    }

    /**
     * 得到writeJedis对象
     * @return
     */
    public static Jedis getWriteJedis()  {
        // 从本地线程变量ThreadLocal中获取连接
        Jedis jedis = writeJedis_local.get();
        // 是第一次获取
        if (jedis == null) {
            // 从连接池获得一个连接
            jedis = MyJedisPool.getWriteJedisObject();
            // 将连接添加到本地线程变量中共享
            writeJedis_local.set(jedis);
        }
        return jedis;
    }

    /**
     * 得到readJedis对象
     * @return
     */
    public static Jedis getReadJedis()  {
        // 从本地线程变量ThreadLocal中获取连接
        Jedis jedis = readJedis_local.get();
        // 是第一次获取
        if (jedis == null) {
            // 从连接池获得一个连接
            jedis = MyJedisPool.getReadJedisObject();
            // 将连接添加到本地线程变量中共享
            readJedis_local.set(jedis);
        }
        return jedis;
    }


    /**
     * 单机redis
     * 向set中装载数据，并设置过期时间为1天
     * @param pushKey
     * @param userid_luid
     */
    public static void setPushKeyvalue(String pushKey,String userid_luid){
        Jedis jedis=null;
        try{
            jedis = MyJedisPool.getWriteJedisObject();
            Boolean res = jedis.exists(pushKey);
            if(!res){
                jedis.sadd(pushKey,userid_luid);
                jedis.expire(pushKey, 2*24 * 60 * 60);
            }else{
                jedis.sadd(pushKey,userid_luid);
            }
        }catch(Exception e){
            System.err.println("redis操作异常"+e.getMessage());
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
    }

    /**
     * 单机redis
     * 判断set中是否存在某个值
     * @param pushKey    redis_key
     * @param userid_luid    set中某一个值
     * @return
     */
    public static Boolean isExistsPushKeyvalue(String pushKey,String userid_luid){
        Boolean res=false;
        Jedis jedis=null;
        try{
            jedis = MyJedisPool.getReadJedisObject();
             res = jedis.sismember(pushKey, userid_luid);
        }catch(Exception e){
            System.err.println("redis操作异常"+e.getMessage());
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return res;
    }


    /**
     * 单机redis
     * jedis设置list类型数据
     */
    public static Long setListLeftKV(String name,int second,String value){
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getWriteJedisObject();
            Long res = jedis.lpush(name, value);
//           30 * 60  30分钟
            jedis.expire(name, second);
            return res;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }


    /**
     * 单机redis
     * 从redis list中获取index索引的值
     */
    public static String getListIndexKV(String name,long index){
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getReadJedisObject();
            return jedis.lindex(name,index);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }



    /**
     * 单机redis
     * 获取hash表中所有key
     * @param name
     * @return
     */
    public static Set<String> getHashAllKey(String name){
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getReadJedisObject();
            return jedis.hkeys(name);
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }

    /**
     * 单机redis
     * 从redis hash表中获取
     * @param hashName
     * @param key
     * @return
     */
    public static String getHashKV(String hashName,String key){
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getReadJedisObject();
            return jedis.hget(hashName, key);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }

    /**
     * 单机redis
     * 删除hash表的键值对
     * @param hashName
     * @param key
     */
    public static Long delHashKV(String hashName,String key){
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getWriteJedisObject();
            return jedis.hdel(hashName,key);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }

    /**
     * 单机redis
     * 存放hash表键值对
     * @param hashName
     * @param key
     * @param value
     */
    public static Long setHashKV(String hashName,String key,String value){
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getWriteJedisObject();
            return jedis.hset(hashName,key,value);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }

    /**
     * 单机redis
     * 删除键值对
     * @param k
     * @return
     */
    public static Long delKV(String k){
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getWriteJedisObject();
            return jedis.del(k);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }

    /**
     * 单机redis
     * 放键值对
     * 永久
     * @param k
     * @param v
     */
    public static String setKV(String k, String v)
    {
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getWriteJedisObject();
            return jedis.set(k, v);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }


    /**
     * 放键值对
     * 单机redis
     * @param k
     * @param v
     */
    public static String setKV(String k,int second, String v)
    {
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getWriteJedisObject();
            return jedis.setex(k,second, v);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }

    /**
     * 根据key取value
     * 单机redis
     * @param k
     * @return
     */
    public static String getKV(String k)
    {
        Jedis jedis = null;
        try {
            jedis = MyJedisPool.getReadJedisObject();
            return jedis.get(k);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            MyJedisPool.returnJedisOjbect(jedis);
        }
        return null;
    }

}

/**
 * 单机redis
 */
class MyJedisPool {

    private final static Logger logger = LoggerFactory.getLogger(MyJedisPool.class);
    private static JedisPool readPool = null;
    private static JedisPool writePool = null;
    //静态代码初始化池配置
    static {
        try{
            Properties props = new Properties();
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream in = classloader.getResourceAsStream("redis.properties");
            props.load(in);
            //创建jedis池配置实例
            JedisPoolConfig config = new JedisPoolConfig();
            //设置池配置项值
            config.setMaxTotal(Integer.valueOf(props.getProperty("jedis.pool.maxTotal")));
            config.setMaxIdle(Integer.valueOf(props.getProperty("jedis.pool.maxIdle")));
            config.setMaxWaitMillis(Long.valueOf(props.getProperty("jedis.pool.maxWait")));
            config.setTestOnBorrow(Boolean.valueOf(props.getProperty("jedis.pool.testOnBorrow")));
            config.setTestOnReturn(Boolean.valueOf(props.getProperty("jedis.pool.testOnReturn")));
            //根据配置实例化jedis池
            //需要设置密码时用
//            readPool = new JedisPool(config, props.getProperty("redisReadURL"), Integer.valueOf(props.getProperty("redisReadPort")),60000,props.getProperty("redisPassWord"));
//            writePool = new JedisPool(config, props.getProperty("redisWriteURL"), Integer.valueOf(props.getProperty("redisWritePort")),60000,props.getProperty("redisPassWord"));
            //不需要设置密码时用
            readPool = new JedisPool(config, props.getProperty("redisReadURL"), Integer.valueOf(props.getProperty("redisReadPort")),60000);
            writePool = new JedisPool(config, props.getProperty("redisWriteURL"), Integer.valueOf(props.getProperty("redisWritePort")),60000);
        }catch (IOException e) {
            logger.info("redis连接池异常",e);
        }
    }

    /**
     * 单机redis
     * 释放jedis资源
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            MyJedisPool.returnJedisOjbect(jedis);
        }
    }

    /**获得jedis对象*/
    public static Jedis getReadJedisObject(){
        return readPool.getResource();
    }

    /**获得jedis对象*/
    public static Jedis getWriteJedisObject(){
        return writePool.getResource();
    }


    /**归还jedis对象*/
    public static void returnJedisOjbect(Jedis jedis){
        if (jedis != null) {
            jedis.close();
        }
    }
}

/**
 * JedisCluster
 */
class RedisClusterPool{
    private final static Logger logger = LoggerFactory.getLogger(RedisClusterPool.class);
    private static Set<HostAndPort> jedisClusterNode=null;
    private static JedisCluster jedisCluster=null;
    static {
        try{
            //创建jedisCluster池配置实例
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10000);//设置最大连接数
            config.setMaxIdle(100); //设置最大空闲数
            config.setMaxWaitMillis(3000);//设置超时时间
            config.setTestOnBorrow(true);
            // 线上环境集群结点
            jedisClusterNode = new HashSet<HostAndPort>();
            jedisClusterNode.add(new HostAndPort("redis-bdpsearch-rediscluster-n0.idc.xiaozhu.com", Integer.parseInt("7000")));
            jedisClusterNode.add(new HostAndPort("redis-bdpsearch-rediscluster-n0.idc.xiaozhu.com", Integer.parseInt("7001")));
            jedisClusterNode.add(new HostAndPort("redis-bdpsearch-rediscluster-n1.idc.xiaozhu.com", Integer.parseInt("7000")));
            jedisClusterNode.add(new HostAndPort("redis-bdpsearch-rediscluster-n1.idc.xiaozhu.com", Integer.parseInt("7001")));
            jedisClusterNode.add(new HostAndPort("redis-bdpsearch-rediscluster-n2.idc.xiaozhu.com", Integer.parseInt("7000")));
            jedisClusterNode.add(new HostAndPort("redis-bdpsearch-rediscluster-n2.idc.xiaozhu.com", Integer.parseInt("7001")));
            //测试环境
//            jedisClusterNode.add(new HostAndPort("10.3.0.114", Integer.parseInt("7000")));
//            jedisClusterNode.add(new HostAndPort("10.3.0.114", Integer.parseInt("7001")));
//            jedisClusterNode.add(new HostAndPort("10.3.0.115", Integer.parseInt("7000")));
//            jedisClusterNode.add(new HostAndPort("10.3.0.115", Integer.parseInt("7001")));
//            jedisClusterNode.add(new HostAndPort("10.3.0.116", Integer.parseInt("7000")));
//            jedisClusterNode.add(new HostAndPort("10.3.0.116", Integer.parseInt("7001")));
            jedisCluster = new JedisCluster(jedisClusterNode,3000,2000,5,"1074b870afbe95db",config);

        }catch (Exception e) {
            logger.info("redisCluster连接池异常",e);
        }
    }

    /**获得jedisCluster对象*/
    public static JedisCluster getJedisClusterObject(){
        return jedisCluster;
    }

}


