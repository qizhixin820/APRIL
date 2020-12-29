package com.hit.cs.db.april;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = {})
public class AprilBackendApplication {

	public static void main(String[] args) {
		SpringApplication.run(AprilBackendApplication.class, args);
	}
	
//	@Bean
//	  public TomcatServletWebServerFactory webServerFactory() {
//	     TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
//	     factory.addConnectorCustomizers(new TomcatConnectorCustomizer() {
//	              @Override
//	              public void customize(Connector connector) {
//	                  connector.setProperty("relaxedPathChars", "\"<>[\\]^`{|}/=#");
//	                  connector.setProperty("relaxedQueryChars", "\"<>[\\]^`{|}/=#");
//	               }
//	      });
//	      return factory;
//	  }

}
