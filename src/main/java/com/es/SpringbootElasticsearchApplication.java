package com.es;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.env.Environment;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * author: 阿杰
 */
@SpringBootApplication
@EnableSwagger2
@Slf4j
public class SpringbootElasticsearchApplication {

	public static void main(String[] args) throws UnknownHostException {
//		SpringApplication.run(SpringbootElasticsearchApplication.class, args);

		System.out.println(" ......................我佛慈悲......................");
		System.out.println("                       _oo0oo_                      ");
		System.out.println("                      o8888888o                     ");
		System.out.println("                      88\" . \"88                     ");
		System.out.println("                      (| -_- |)                     ");
		System.out.println("                      0\\  =  /0                     ");
		System.out.println("                    ___/‘---’\\___                   ");
		System.out.println("                  .' \\|       |/ '.                 ");
		System.out.println("                 / \\\\|||  :  |||// \\                ");
		System.out.println("                / _||||| -卍-|||||_ \\               ");
		System.out.println("               |   | \\\\\\  -  /// |   |              ");
		System.out.println("               | \\_|  ''\\---/''  |_/ |              ");
		System.out.println("               \\  .-\\__  '-'  ___/-. /              ");
		System.out.println("             ___'. .'  /--.--\\  '. .'___            ");
		System.out.println("          .\"\" ‘<  ‘.___\\_<|>_/___.’ >’ \"\".          ");
		System.out.println("         | | :  ‘- \\‘.;‘\\ _ /’;.’/ - ’ : | |        ");
		System.out.println("         \\  \\ ‘_.   \\_ __\\ /__ _/   .-’ /  /        ");
		System.out.println("     =====‘-.____‘.___ \\_____/___.-’___.-’=====     ");
		System.out.println("                       ‘=---=’                      ");
		System.out.println("                                                    ");
		System.out.println("....................佛祖开光 ,永无BUG...................");

		ConfigurableApplicationContext application = SpringApplication.run(SpringbootElasticsearchApplication.class, args);
		Environment env = application.getEnvironment();
		String ip = InetAddress.getLocalHost().getHostAddress();
		String port = env.getProperty("server.port");
		String path = "";
		log.info("\n----------------------------------------------------------\n\t" +
				"SpringbootElasticsearchApplication is running! Access URLs:\n\t" +
				"Local: \t\thttp://localhost:" + port + path + "/\n\t" +
				"External: \thttp://" + ip + ":" + port + path + "/\n\t" +
				"swagger-ui: \thttp://" + ip + ":" + port + path + "/swagger-ui.html\n\t" +
				"----------------------------------------------------------");
	}
}
