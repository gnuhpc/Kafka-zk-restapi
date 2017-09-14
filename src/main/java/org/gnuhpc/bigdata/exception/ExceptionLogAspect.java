package org.gnuhpc.bigdata.exception;


import lombok.extern.log4j.Log4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;

@Aspect
@Component
@Log4j
public class ExceptionLogAspect {
    @Pointcut("execution(public * org.gnuhpc.bigdata.exception..*.*(..))")
    public void exceptionLog() {
    }

    @Before("exceptionLog()")
    public void doBefore(JoinPoint joinPoint) throws Throwable {
        // 接收到请求，记录请求内容
        log.info("Exception happened!", (Exception) joinPoint.getArgs()[0]);
    }
}
