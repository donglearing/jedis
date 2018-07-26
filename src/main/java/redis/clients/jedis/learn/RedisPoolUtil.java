package redis.clients.jedis.learn;

import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Set;


/**
 * Created by dongpengfei
 * Date 2018/6/29
 * Time 上午11:06
 */

public class RedisPoolUtil {

    private static final int DEFAULT_TIMEOUT = 2000;
    private static final int DEFAULT_REDIRECTIONS = 5;
    private static final JedisPoolConfig DEFAULT_CONFIG = new JedisPoolConfig();

//    private static String redisUrl = "192.168.1.250:7001;192.168.1.249:7002;192.168.1.244:7003";

//    private static String password = "m2TYeW1";
    private static String redisUrl = "127.0.0.1:7001;127.0.0.1:7002;127.0.0.1:7003";

    private static String password = "m9Gy7Hbw";

//    private static String redisUrl = "127.0.0.1:6379";

//    private static String redisUrl = "192.168.1.242:6379";

//    private static String password = "";

    //
    public static  JedisCluster jc = null;

    public  static JedisPool pool = null;

    static{
        init();
    }

    /**
     * hadoop内部用的是JAVA 1.7 不支持lamda表达式 用rxjava
     */
    public static void init(){

        String[] strs = redisUrl.split(";");
        final Set<HostAndPort> jedisClusterNodeSet = new HashSet<HostAndPort>();

        Observable.fromArray(strs).subscribe(new Consumer<String>() {
            @Override
            public void accept(String p) throws Exception {
                String[] str = p.split(":");
                jedisClusterNodeSet.add(new HostAndPort(str[0], Integer.valueOf(str[1])));
            }
        });
        if(RedisPoolUtil.password.length() > 1){
            jc = new JedisCluster(jedisClusterNodeSet, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, DEFAULT_REDIRECTIONS, password, DEFAULT_CONFIG);
        }else{
            String[] str = redisUrl.split(":");
            pool = new JedisPool(new JedisPoolConfig(), str[0], Integer.valueOf(str[1]), DEFAULT_TIMEOUT);
        }

    }

    public static void returnResource(Jedis jedis){
        jedis.close();
    }

    public static void redisSet(String key, String value){
        if(jc != null){
            jc.set(key, value);
        }else{
            Jedis jedis = RedisPoolUtil.getResouce();
            jedis.set(key, value);
            RedisPoolUtil.returnResource(jedis);
        }
    }

    public static void redisSet(byte[] key, byte[] value){
        if(jc != null){
            jc.set(key, value);
        }else{
            Jedis jedis = RedisPoolUtil.getResouce();
            jedis.set(key, value);
            RedisPoolUtil.returnResource(jedis);
        }
    }

    /**
     * @Author pengfei.dong
     * @Description // redis 集合操作 添加数据  不可添加重复数据
     * @Date 2018/7/26
     * @time 下午5:51
     * @Param  * @param key
     * @param value
     * @return void
     **/
    public static void redisSadd(String key, String value){

        if(jc != null){
            jc.sadd(key, value);
        }else{
            Jedis jedis = RedisPoolUtil.getResouce();
            jedis.sadd(key, value);
            RedisPoolUtil.returnResource(jedis);
        }
    }

    /**
     * @Author pengfei.dong
     * @Description //TODO
     * @Date 2018/7/26
     * @time 下午5:56
     * @Param  * @param key
     * @return java.util.Set<java.lang.String>
     **/
    public static Set<String> redisGetMember(String key){
        Set<String> sets ;
        if(jc != null){
            sets = jc.smembers(key);
        }else{
            Jedis jedis = RedisPoolUtil.getResouce();
            sets = jedis.smembers(key);
            RedisPoolUtil.returnResource(jedis);
        }
        return sets;
    }


    public static Jedis getResouce(){

        return pool.getResource();
    }

    public static String redisGet(String key){
        if(jc != null){
            return jc.get(key);
        }else{
            Jedis jedis = null;
            try {
                jedis = RedisPoolUtil.getResouce();
                return jedis.get(key);
            }finally {
                RedisPoolUtil.returnResource(jedis);
            }
        }
    }

    public static byte[] redisGet(byte[] key){
        if(jc != null){
            return jc.get(key);
        }else{
            Jedis jedis = null;
            try {
                jedis = RedisPoolUtil.getResouce();
                return jedis.get(key);
            }finally {
                RedisPoolUtil.returnResource(jedis);
            }
        }
    }

    public static void main(String[] args) {
//        RedisPoolUtil.redisSet("dongpf0000CC", "ccccsss1");
//        System.out.println(RedisPoolUtil.redisGet("dongpf0000CC"));
        RedisPoolUtil.init();
//       Jedis jedis =  RedisPoolUtil.jc.getResource();
//       jc.sadd("dongpf","1");
//        jc.sadd("dongpf","2");
//        jc.sadd("dongpf","3");
//        jc.sadd("dongpf","4");
//        jc.sadd("dongpf","5");
//        jc.sadd("dongpf","6");
//        jc.sadd("dongpf","1");
//        jc.sadd("dongpf","1");
//        jc.sadd("dongpf","1");
//        jc.srem("dongpf", "1");

         RedisPoolUtil.redisSadd("dongpf0000CC", "ccccsss1");
         RedisPoolUtil.redisSadd("dongpf0000CC", "ccccsss2");
         RedisPoolUtil.redisSadd("dongpf0000CC", "ccccsss3");
         RedisPoolUtil.redisSadd("dongpf0000CC", "ccccsss4");
         RedisPoolUtil.redisSadd("dongpf0000CC", "ccccsss5");


        Set<String> strings = RedisPoolUtil.redisGetMember("dongpf0000CC");



        strings.stream().forEach(System.out::println);


    }


}
