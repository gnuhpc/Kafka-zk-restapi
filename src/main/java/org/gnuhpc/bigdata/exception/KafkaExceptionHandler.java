package org.gnuhpc.bigdata.exception;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.errors.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Log4j2
@RestControllerAdvice
public class KafkaExceptionHandler {

  @ExceptionHandler(ApiException.class)
  public RestErrorResponse kafkaApiException(ApiException ex) {
    RestErrorResponse.Builder responseBuilder = new RestErrorResponse.Builder();
    return responseBuilder
        .setStatus(HttpStatus.INTERNAL_SERVER_ERROR)
        .setCode(KafkaErrorCode.UNKNOWN.ordinal())
        .setMessage("Api Exception happened!")
        .setDeveloperMessage(ex.getMessage())
        .build();
  }
}
