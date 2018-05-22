package org.gnuhpc.bigdata.config;

import org.gnuhpc.bigdata.service.UserDetailsServiceImp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {
  @Autowired
  private BasicAuthenticationPoint basicAuthenticationPoint;

  @Bean
  public UserDetailsService userDetailsService() {
    return new UserDetailsServiceImp();
  };

  @Bean
  public BCryptPasswordEncoder passwordEncoder() {
    return new BCryptPasswordEncoder();
  };

  @Value("${server.security}")
  private boolean security;

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.csrf().disable();
    if (security) {
      http.authorizeRequests().antMatchers("/api", "/swagger-ui.html", "/webjars/**", "/swagger-resources/**", "/v2/**").permitAll()
              .anyRequest().authenticated();
      http.httpBasic().authenticationEntryPoint(basicAuthenticationPoint);
      http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);
    } else {
      http.authorizeRequests().antMatchers("/**").permitAll()
              .anyRequest().authenticated();
    }
  }

  @Autowired
  public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
    auth.userDetailsService(userDetailsService()).passwordEncoder(passwordEncoder());
  }
}
