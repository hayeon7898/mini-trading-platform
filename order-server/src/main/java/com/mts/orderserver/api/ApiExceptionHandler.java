package com.mts.orderserver.api;

import com.mts.orderserver.domain.OrderNotCancelableException;
import com.mts.orderserver.service.AccountNotFoundException;
import com.mts.orderserver.service.InvalidPeriodException;
import com.mts.orderserver.service.OrderNotFoundException;
import com.mts.orderserver.service.UnknownReferenceException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 오류 응답은 RFC 9457 ProblemDetail(application/problem+json)로 통일한다.
 * ResponseEntityExceptionHandler를 상속해 Spring MVC 표준 예외(본문 파싱 실패, 타입 불일치,
 * 요청 파라미터 메서드 검증 등)도 같은 형식이 된다.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    public ProblemDetail handleOrderNotFound(OrderNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(AccountNotFoundException.class)
    public ProblemDetail handleAccountNotFound(AccountNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(InvalidPeriodException.class)
    public ProblemDetail handleInvalidPeriod(InvalidPeriodException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(UnknownReferenceException.class)
    public ProblemDetail handleUnknownReference(UnknownReferenceException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(422), e.getMessage());
        pd.setProperty("field", e.getField());
        return pd;
    }

    @ExceptionHandler(OrderNotCancelableException.class)
    public ProblemDetail handleNotCancelable(OrderNotCancelableException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        pd.setProperty("orderStatus", e.getStatus()); // "status"는 ProblemDetail의 HTTP 상태 필드라 피한다
        return pd;
    }

    /** 기본 응답에는 어느 필드가 틀렸는지 없어서, 필드별 오류 목록을 붙인다. */
    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers,
            HttpStatusCode status, WebRequest request) {
        List<Map<String, String>> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> Map.of(
                        "field", fe.getField(),
                        "message", Objects.requireNonNullElse(fe.getDefaultMessage(), "invalid")))
                .toList();
        ProblemDetail body = ex.getBody();
        body.setProperty("errors", errors);
        return handleExceptionInternal(ex, body, headers, status, request);
    }
}