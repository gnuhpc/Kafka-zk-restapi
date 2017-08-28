package org.gnuhpc.bigdata.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Created by gnuhpc on 2017/7/22.
 */
@Configuration
public class WebMvcConfig extends WebMvcConfigurerAdapter {
    public void configurePathMatch(PathMatchConfigurer configurer) {
        configurer.setUseSuffixPatternMatch(false);
    }
}
