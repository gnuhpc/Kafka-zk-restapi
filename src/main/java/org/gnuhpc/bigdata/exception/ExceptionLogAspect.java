package org.gnuhpc.bigdata.exception;

import lombok.extern.log4j.Log4j2;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Log4j2
public class ExceptionLogAspect {

  @Pointcut("execution(public * org.gnuhpc.bigdata.exception..*.*(..))")
  public void exceptionLog() {}

  @Before("exceptionLog()")
  public void doBefore(JoinPoint joinPoint) throws Throwable {
    // 接收到请求，记录请求内容
    log.info("Exception happened!", (Exception) joinPoint.getArgs()[0]);
  }
}
