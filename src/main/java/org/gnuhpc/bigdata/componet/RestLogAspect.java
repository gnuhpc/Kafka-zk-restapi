package org.gnuhpc.bigdata.componet;

import java.util.Arrays;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.log4j.Log4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Aspect
@Component
@Log4j
public class RestLogAspect {

  ThreadLocal<Long> startTime = new ThreadLocal<>();

  @Pointcut("execution(public * org.gnuhpc.bigdata.controller..*.*(..))")
  public void restServiceLog() {}

  @Before("restServiceLog()")
  public void doBefore(JoinPoint joinPoint) throws Throwable {
    startTime.set(System.currentTimeMillis());
    // 接收到请求，记录请求内容
    ServletRequestAttributes attributes =
        (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
    HttpServletRequest request = attributes.getRequest();
    // 记录下请求内容
    log.info("===================== Controller Request ====================");
    log.info("URL : " + request.getRequestURL().toString());
    log.info("HTTP_METHOD : " + request.getMethod());
    log.info("IP : " + request.getRemoteAddr());
    log.info(
        "CLASS_METHOD : "
            + joinPoint.getSignature().getDeclaringTypeName()
            + "."
            + joinPoint.getSignature().getName());
    log.info("ARGS : " + Arrays.toString(joinPoint.getArgs()));
  }

  @AfterReturning(returning = "ret", pointcut = "restServiceLog()")
  public void doAfterReturning(Object ret) throws Throwable {
    // 处理完请求，返回内容
    log.info("===================== Controller Response =================");
    log.info("SPEND TIME : " + (System.currentTimeMillis() - startTime.get()));
    log.info("RESPONSE : " + ret);
  }
}
