package com.lagou.redis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * 安装部署流程详见: install.txt
 */
public class RedisDemo {

    private JedisCluster jedisCluster;

    @Before
    public void init() {
        Set<HostAndPort> hostAndPortSet = new HashSet();
        hostAndPortSet.add(new HostAndPort("linux121", 7001));
        hostAndPortSet.add(new HostAndPort("linux121", 7002));
        hostAndPortSet.add(new HostAndPort("linux122", 7003));
        hostAndPortSet.add(new HostAndPort("linux121", 7007));
        hostAndPortSet.add(new HostAndPort("linux121", 7008));
        hostAndPortSet.add(new HostAndPort("linux123", 7005));
        hostAndPortSet.add(new HostAndPort("linux122", 7004));
        hostAndPortSet.add(new HostAndPort("linux123", 7006));
        jedisCluster = new JedisCluster(hostAndPortSet, 2000);
    }

    @Test
    public void testConn(){
        Jedis redis = new Jedis("linux121", 7001);
        redis.set("jedis:cluster-name:1","new-create-redis-cluster");
        System.out.println(redis.get("jedis:cluster-name:1"));
    }

}
