package org.gnuhpc.bigdata.exception;

import org.apache.kafka.common.errors.ApiException;
import org.springframework.boot.context.config.ResourceNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.Set;

@RestControllerAdvice
public class ExceptionHandlingController {
    @ExceptionHandler(Exception.class)
    public RestErrorResponse handleException(Exception ex){
        RestErrorResponse.Builder builder = new RestErrorResponse.Builder();
        RestErrorResponse response = builder
                .setCode(KafkaErrorCode.UNKNOWN.ordinal())
                .setMessage("Default Exception happened!")
                .setDeveloperMessage(ex.getMessage())
                .setStatus(HttpStatus.SERVICE_UNAVAILABLE).build();
        return response;
    }

    @ExceptionHandler(ApiException.class)
    public RestErrorResponse kafkaApiException(ApiException ex) {
        RestErrorResponse.Builder responseBuilder = new RestErrorResponse.Builder();
        return  responseBuilder.setStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                .setCode(KafkaErrorCode.UNKNOWN.ordinal())
                .setMessage("Api Exception happened!")
                .setDeveloperMessage(ex.getMessage())
                .build();
    }

    @ExceptionHandler(RuntimeException.class)
    public RestErrorResponse runtimeException(RuntimeException ex){
        RestErrorResponse.Builder responseBuilder = new RestErrorResponse.Builder();
        return responseBuilder.setStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .setCode(KafkaErrorCode.UNKNOWN.ordinal())
                .setMessage("Runtime Exception happened!")
                .setDeveloperMessage(ex.getMessage())
                .build();
    }


    @ExceptionHandler(ConstraintViolationException.class)
    public RestErrorResponse constraintViolationException(ConstraintViolationException ex){
        StringBuilder message = new StringBuilder();
        Set<ConstraintViolation<?>> violations = ex.getConstraintViolations();
        for (ConstraintViolation<?> violation : violations) {
            message.append(violation.getMessage().concat(";"));
        }

        RestErrorResponse.Builder responseBuilder = new RestErrorResponse.Builder();
        return responseBuilder.setStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .setCode(KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION.ordinal())
                .setMessage("Constraint  Violation Exception happened!")
                .setMessage(message.toString().substring(0,message.length()-1))
                .setDeveloperMessage(ex.getMessage())
                .build();
    }


    @ExceptionHandler(ResourceNotFoundException.class)
    public RestErrorResponse serviceNotAvailableException(ServiceNotAvailableException ex){
        RestErrorResponse.Builder responseBuilder = new RestErrorResponse.Builder();
        return responseBuilder.setStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .setCode(KafkaErrorCode.SERVICE_DOWN.ordinal())
                .setMessage("Service not Available happened: " + ex)
                .setDeveloperMessage(ex.getMessage())
                .build();
    }
}
