package org.gnuhpc.bigdata.exception;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.validator.internal.engine.path.PathImpl;
import org.springframework.http.HttpStatus;
import org.springframework.util.ObjectUtils;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;

@Data
public class RestErrorResponse {

  private HttpStatus status;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
  private LocalDateTime timestamp;

  private int code;
  private String message;
  private String developerMessage;
  private String moreInfoUrl;
  private List<RestSubError> subErrorList;

  public RestErrorResponse() {
    // this.timestamp = new Date();
    this.timestamp = LocalDateTime.now();
  }

  public RestErrorResponse(HttpStatus status, String message, Throwable ex) {
    this();
    this.status = status;
    this.code = status.value();
    this.message = message;
    this.developerMessage = ex.getLocalizedMessage();
  }

  public RestErrorResponse(HttpStatus status, String message, String moreInfoUrl, Throwable ex) {
    this();
    this.status = status;
    this.code = status.value();
    this.message = message;
    this.developerMessage = ex.getLocalizedMessage();
    this.moreInfoUrl = moreInfoUrl;
  }

  public RestErrorResponse(
      HttpStatus status, int code, String message, String developerMessage, String moreInfoUrl) {
    this();
    if (status == null) {
      throw new NullPointerException("HttpStatus argument cannot be null.");
    }
    this.status = status;
    this.code = code;
    this.message = message;
    this.developerMessage = developerMessage;
    this.moreInfoUrl = moreInfoUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof RestErrorResponse) {
      RestErrorResponse re = (RestErrorResponse) o;
      return ObjectUtils.nullSafeEquals(getStatus(), re.getStatus())
          && getCode() == re.getCode()
          && ObjectUtils.nullSafeEquals(getMessage(), re.getMessage())
          && ObjectUtils.nullSafeEquals(getDeveloperMessage(), re.getDeveloperMessage())
          && ObjectUtils.nullSafeEquals(getMoreInfoUrl(), re.getMoreInfoUrl());
    }

    return false;
  }

  @Override
  public int hashCode() {
    //noinspection ThrowableResultOfMethodCallIgnored
    return ObjectUtils.nullSafeHashCode(
        new Object[] {
          getStatus(), getCode(), getMessage(), getDeveloperMessage(), getMoreInfoUrl()
        });
  }

  public String toString() {
    //noinspection StringBufferReplaceableByString
    return new StringBuilder()
        .append(getStatus().value())
        .append(" (")
        .append(getStatus().getReasonPhrase())
        .append(" )")
        .toString();
  }

  private void addSubError(RestSubError subError) {
    if (subErrorList == null) {
      subErrorList = new ArrayList<>();
    }
    subErrorList.add(subError);
  }

  private void addValidationError(
      String object, String field, Object rejectedValue, String message) {
    addSubError(new RestValidationError(object, field, rejectedValue, message));
  }

  private void addValidationError(String object, String message) {
    addSubError(new RestValidationError(object, message));
  }

  private void addValidationError(FieldError fieldError) {
    this.addValidationError(
        fieldError.getObjectName(),
        fieldError.getField(),
        fieldError.getRejectedValue(),
        fieldError.getDefaultMessage());
  }

  private void addValidationError(ObjectError objectError) {
    this.addValidationError(objectError.getObjectName(), objectError.getDefaultMessage());
  }

  void addValidationError(List<ObjectError> globalErrors) {
    globalErrors.forEach(this::addValidationError);
  }

  /**
   * Utility method for adding error of ConstraintViolation. Usually when a @Validated validation
   * fails.
   *
   * @param cv the ConstraintViolation
   */
  private void addValidationError(ConstraintViolation<?> cv) {
    this.addValidationError(
        cv.getRootBeanClass().getSimpleName(),
        ((PathImpl) cv.getPropertyPath()).getLeafNode().asString(),
        cv.getInvalidValue(),
        cv.getMessage());
  }

  void addValidationErrors(List<FieldError> fieldErrors) {
    fieldErrors.forEach(this::addValidationError);
  }

  void addValidationErrors(Set<ConstraintViolation<?>> constraintViolations) {
    constraintViolations.forEach(this::addValidationError);
  }

  public static class Builder {

    private HttpStatus status;
    private int code;
    private String message;
    private String developerMessage;
    private String moreInfoUrl;

    public Builder() {}

    public Builder setStatus(int statusCode) {
      this.status = HttpStatus.valueOf(statusCode);
      return this;
    }

    public Builder setStatus(HttpStatus status) {
      this.status = status;
      return this;
    }

    public Builder setCode(int code) {
      this.code = code;
      return this;
    }

    public Builder setMessage(String message) {
      this.message = message;
      return this;
    }

    public Builder setDeveloperMessage(String developerMessage) {
      this.developerMessage = developerMessage;
      return this;
    }

    public Builder setMoreInfoUrl(String moreInfoUrl) {
      this.moreInfoUrl = moreInfoUrl;
      return this;
    }

    public RestErrorResponse build() {
      if (this.status == null) {
        this.status = HttpStatus.INTERNAL_SERVER_ERROR;
      }
      return new RestErrorResponse(
          this.status, this.code, this.message, this.developerMessage, this.moreInfoUrl);
    }
  }

  abstract class RestSubError {}

  @Data
  @EqualsAndHashCode(callSuper = false)
  @AllArgsConstructor
  class RestValidationError extends RestSubError {

    private String object;
    private String field;
    private Object rejectedValue;
    private String message;

    RestValidationError(String object, String message) {
      this.object = object;
      this.message = message;
    }
  }
}
