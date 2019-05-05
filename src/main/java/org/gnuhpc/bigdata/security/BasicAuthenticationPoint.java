package org.gnuhpc.bigdata.security;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NoArgsConstructor;
import org.gnuhpc.bigdata.exception.RestErrorResponse;
import org.springframework.boot.jackson.JsonComponent;
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
    ObjectMapper mapper = new ObjectMapper();
    JavaTimeModule javaTimeModule = new JavaTimeModule();
    javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer());
    mapper.registerModule(javaTimeModule);
    response.getWriter().print(mapper.writeValueAsString(restAuthenticationError));
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    setRealmName("Contact Big Data Infrastructure Team to get available accounts.");
    super.afterPropertiesSet();
  }

  @JsonComponent
  @NoArgsConstructor
  private class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {
    @Override
    public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider sp) throws IOException{
      gen.writeString(value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }
  }
}
