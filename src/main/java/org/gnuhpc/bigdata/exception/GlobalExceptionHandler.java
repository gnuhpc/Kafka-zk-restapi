package org.gnuhpc.bigdata.exception;

import java.util.List;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.util.CollectionUtils;
import org.springframework.validation.BindException;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingPathVariableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.util.WebUtils;

@Log4j
@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

  /**
   * A single place to customize the response body of all Exception types.
   *
   * <p>The default implementation sets the {@link WebUtils#ERROR_EXCEPTION_ATTRIBUTE} request
   * attribute and creates a {@link ResponseEntity} from the given body, headers, and status.
   *
   * @param ex the exception
   * @param body the body for the response
   * @param headers the headers for the response
   * @param status the response status
   * @param request the current request
   */
  @Override
  protected ResponseEntity<Object> handleExceptionInternal(
      Exception ex, Object body, HttpHeaders headers, HttpStatus status, WebRequest request) {

    if (HttpStatus.INTERNAL_SERVER_ERROR.equals(status)) {
      request.setAttribute(WebUtils.ERROR_EXCEPTION_ATTRIBUTE, ex, WebRequest.SCOPE_REQUEST);
    }
    String error = "Internal Server Error";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, error, ex));
  }

  /**
   * Customize the response for HttpRequestMethodNotSupportedException.
   *
   * <p>This method logs a warning, sets the "Allow" header.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param webRequest the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleHttpRequestMethodNotSupported(
      HttpRequestMethodNotSupportedException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest webRequest) {
    pageNotFoundLogger.warn(ex.getMessage());

    ServletWebRequest servletRequest = (ServletWebRequest) webRequest;
    HttpServletRequest request = servletRequest.getNativeRequest(HttpServletRequest.class);
    StringBuilder builder = new StringBuilder();
    builder.append(
        "Request method: " + request.getMethod() + " is not supported. Supported Methods: ");
    Set<HttpMethod> supportedMethods = ex.getSupportedHttpMethods();
    supportedMethods.forEach(m -> builder.append(m).append(", "));

    if (!CollectionUtils.isEmpty(supportedMethods)) {
      headers.setAllow(supportedMethods);
    }
    return buildResponseEntity(
        new RestErrorResponse(
            HttpStatus.METHOD_NOT_ALLOWED, builder.substring(0, builder.length() - 2), ex));
  }

  /**
   * Customize the response for HttpMediaTypeNotSupportedException.
   *
   * <p>This method sets the "Accept" header.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleHttpMediaTypeNotSupported(
      HttpMediaTypeNotSupportedException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    StringBuilder builder = new StringBuilder();
    builder.append(ex.getContentType());
    builder.append(" media type is not supported. Supported media types: ");
    List<MediaType> mediaTypes = ex.getSupportedMediaTypes();
    mediaTypes.forEach(t -> builder.append(t).append(", "));

    if (!CollectionUtils.isEmpty(mediaTypes)) {
      headers.setAccept(mediaTypes);
    }

    return buildResponseEntity(
        new RestErrorResponse(
            HttpStatus.UNSUPPORTED_MEDIA_TYPE, builder.substring(0, builder.length() - 2), ex));
  }

  /**
   * Customize the response for HttpMediaTypeNotAcceptableException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleHttpMediaTypeNotAcceptable(
      HttpMediaTypeNotAcceptableException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    String error = "Media Type not Acceptable";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.NOT_ACCEPTABLE, error, ex));
  }

  /**
   * Customize the response for MissingPathVariableException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   * @since 4.2
   */
  @Override
  protected ResponseEntity<Object> handleMissingPathVariable(
      MissingPathVariableException ex, HttpHeaders headers, HttpStatus status, WebRequest request) {
    String error = "Path Variable : " + ex.getVariableName() + " is missing";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.BAD_REQUEST, error, ex));
  }

  /**
   * Customize the response for MissingServletRequestParameterException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleMissingServletRequestParameter(
      MissingServletRequestParameterException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    String error = ex.getParameterName() + " parameter is missing";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.BAD_REQUEST, error, ex));
  }

  /**
   * Customize the response for ServletRequestBindingException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleServletRequestBindingException(
      ServletRequestBindingException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    String error = "ServletRequest Bind Error";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.BAD_REQUEST, error, ex));
  }

  /**
   * Customize the response for ConversionNotSupportedException.
   *
   * <p>This method delegates to {@link #handleExceptionInternal}.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return a {@code ResponseEntity} instance
   */
  @Override
  protected ResponseEntity<Object> handleConversionNotSupported(
      ConversionNotSupportedException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    return handleExceptionInternal(ex, null, headers, status, request);
  }

  /**
   * Customize the response for TypeMismatchException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleTypeMismatch(
      TypeMismatchException ex, HttpHeaders headers, HttpStatus status, WebRequest request) {
    String error = "Request parameter value type mismatch error. ";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.BAD_REQUEST, error, ex));
  }

  /**
   * Customize the response for HttpMessageNotReadableException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleHttpMessageNotReadable(
      HttpMessageNotReadableException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {
    String error = "Malformed JSON request";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.BAD_REQUEST, error, ex));
  }

  /**
   * Customize the response for HttpMessageNotWritableException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleHttpMessageNotWritable(
      HttpMessageNotWritableException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {

    String error = "Error writing JSON output";
    return buildResponseEntity(new RestErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, error, ex));
  }

  /**
   * Customize the response for MethodArgumentNotValidException.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleMethodArgumentNotValid(
      MethodArgumentNotValidException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {

    String error = "Method Argument Validation Error.";
    RestErrorResponse restErrorResponse = new RestErrorResponse(HttpStatus.BAD_REQUEST, error, ex);
    restErrorResponse.addValidationErrors(ex.getBindingResult().getFieldErrors());
    restErrorResponse.addValidationError(ex.getBindingResult().getGlobalErrors());
    return buildResponseEntity(restErrorResponse);
  }

  @ExceptionHandler(ConstraintViolationException.class)
  public ResponseEntity<Object> handleConstraintViolation(ConstraintViolationException ex) {
    String error = "Constraint Violation Error.";
    RestErrorResponse restErrorResponse = new RestErrorResponse(HttpStatus.BAD_REQUEST, error, ex);
    restErrorResponse.addValidationErrors(ex.getConstraintViolations());
    return buildResponseEntity(restErrorResponse);
  }

  /**
   * Customize the response for MissingServletRequestPartException.
   *
   * <p>This method delegates to {@link #handleExceptionInternal}.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return the RestErrorResponse Object
   */
  @Override
  protected ResponseEntity<Object> handleMissingServletRequestPart(
      MissingServletRequestPartException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest request) {

    return handleExceptionInternal(ex, null, headers, status, request);
  }

  /**
   * Customize the response for BindException.
   *
   * <p>This method delegates to {@link #handleExceptionInternal}.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return a {@code ResponseEntity} instance
   */
  @Override
  protected ResponseEntity<Object> handleBindException(
      BindException ex, HttpHeaders headers, HttpStatus status, WebRequest request) {

    return handleExceptionInternal(ex, null, headers, status, request);
  }

  /**
   * Customize the response for NoHandlerFoundException.
   *
   * <p>This method delegates to {@link #handleExceptionInternal}.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param request the current request
   * @return a {@code ResponseEntity} instance
   * @since 4.0
   */
  @Override
  protected ResponseEntity<Object> handleNoHandlerFoundException(
      NoHandlerFoundException ex, HttpHeaders headers, HttpStatus status, WebRequest request) {

    return handleExceptionInternal(ex, null, headers, status, request);
  }

  /**
   * Customize the response for NoHandlerFoundException.
   *
   * <p>This method delegates to {@link #handleExceptionInternal}.
   *
   * @param ex the exception
   * @param headers the headers to be written to the response
   * @param status the selected response status
   * @param webRequest the current request
   * @return a {@code ResponseEntity} instance
   * @since 4.2.8
   */
  @Override
  protected ResponseEntity<Object> handleAsyncRequestTimeoutException(
      AsyncRequestTimeoutException ex,
      HttpHeaders headers,
      HttpStatus status,
      WebRequest webRequest) {

    if (webRequest instanceof ServletWebRequest) {
      ServletWebRequest servletRequest = (ServletWebRequest) webRequest;
      HttpServletRequest request = servletRequest.getNativeRequest(HttpServletRequest.class);
      HttpServletResponse response = servletRequest.getNativeResponse(HttpServletResponse.class);
      if (response.isCommitted()) {
        if (logger.isErrorEnabled()) {
          logger.error(
              "Async timeout for " + request.getMethod() + " [" + request.getRequestURI() + "]");
        }
        return null;
      }
    }

    return handleExceptionInternal(ex, null, headers, status, webRequest);
  }

  private ResponseEntity<Object> buildResponseEntity(RestErrorResponse restErrorResponse) {
    return new ResponseEntity(restErrorResponse, restErrorResponse.getStatus());
  }
}
