package org.gnuhpc.bigdata.config;

import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonValueProcessor;
import org.gnuhpc.bigdata.exception.RestErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class BasicAuthenticationPoint extends BasicAuthenticationEntryPoint {
  @Override
  public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authEx)
          throws IOException, ServletException {
    response.addHeader("WWW-Authenticate", "Basic realm=" +getRealmName());
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    String error = "Authenciation Error:" + authEx.getClass().getCanonicalName();
    RestErrorResponse restAuthenticationError = new RestErrorResponse(HttpStatus.UNAUTHORIZED, error, authEx);
    /**
     * Translate field LocalDateTime to uniform the response format.
     */
    JsonConfig jsonConfig = new JsonConfig();
    jsonConfig.registerJsonValueProcessor(LocalDateTime.class, new JsonValueProcessor() {
      DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      @Override
      public Object processObjectValue(String propertyName, Object date,JsonConfig config) {
        return df.format((LocalDateTime)date);
      }

      @Override
      public Object processArrayValue(Object date, JsonConfig config) {
        return df.format((LocalDateTime)date);
      }
    });

    response.getWriter().print(JSONObject.fromObject(restAuthenticationError, jsonConfig).toString());
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    setRealmName("Contact Big Data Infrastructure Team to get available accounts.");
    super.afterPropertiesSet();
  }
}
