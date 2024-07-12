package com.es.config;

import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.MultipartConfigElement;

/**
 * author: 阿杰
 */
@Configuration
public class FileUploadConfiuration {
    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        //单个文件大小100mb
        factory.setMaxFileSize(104857600);
        //设置总上传数据大小100mb
        factory.setMaxRequestSize(104857600);

        return factory.createMultipartConfig();
    }
}
