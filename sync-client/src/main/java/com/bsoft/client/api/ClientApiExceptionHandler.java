package com.bsoft.client.api;

/**
 * ClassName: ClientApiExceptionHandler
 * Package:   com.bsoft.client.api
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/17 - 9:49
 */
import com.bsoft.common.http.ApiResponse;
import com.bsoft.common.http.ErrorCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(basePackages = "com.bsoft.client.api")
public class ClientApiExceptionHandler {
    private static final Logger logger = LogManager.getLogger(ClientApiExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse<Void>> handleBadRequest(IllegalArgumentException e) {
        String traceId = java.util.UUID.randomUUID().toString();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.fail(ErrorCode.BAD_REQUEST, e.getMessage(), traceId));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleInternal(Exception e) {
        String traceId = java.util.UUID.randomUUID().toString();
        logger.error("Unhandled exception, traceId={}", traceId, e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.fail(ErrorCode.INTERNAL_ERROR, "系统繁忙，请稍后重试", traceId));
    }
}