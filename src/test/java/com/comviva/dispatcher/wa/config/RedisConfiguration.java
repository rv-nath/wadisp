package com.comviva.dispatcher.wa.config;
import redis.embedded.RedisServer;

public class RedisConfiguration{
	public static RedisServer redisServer;
	static int port = Integer.parseInt(System.getenv().getOrDefault("redisTestPort","6911"));
	static String host = System.getenv().getOrDefault("redisTestHost","localhost");

   public static void serverStart() throws Exception {
		if (redisServer == null || !redisServer.isActive()) {
			redisServer = new RedisServer(port);
			redisServer.start();
			System.out.println("server started");
		}
	}
	
	public static void serverStop() throws Exception {
		redisServer.stop();
		System.out.println("server stopped");

	}

}
