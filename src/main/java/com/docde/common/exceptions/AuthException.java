package com.docde.common.exceptions;

public class AuthException extends RuntimeException {

    public AuthException(String message) {
        super(message);
    }

    public AuthException(String message, Throwable cause) {
        super(message, cause);
    }
}

