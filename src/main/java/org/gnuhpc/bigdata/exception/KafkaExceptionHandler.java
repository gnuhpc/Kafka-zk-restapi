package org.gnuhpc.bigdata.exception;

import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.errors.ApiException;
import org.springframework.boot.context.config.ResourceNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.Set;

@Log4j
@RestControllerAdvice
public class KafkaExceptionHandler {
    @ExceptionHandler(ApiException.class)
    public RestErrorResponse kafkaApiException(ApiException ex) {
        RestErrorResponse.Builder responseBuilder = new RestErrorResponse.Builder();
        return  responseBuilder.setStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                .setCode(KafkaErrorCode.UNKNOWN.ordinal())
                .setMessage("Api Exception happened!")
                .setDeveloperMessage(ex.getMessage())
                .build();
    }
}
