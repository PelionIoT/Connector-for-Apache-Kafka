/*
 * Copyright 2021 Pelion Ltd.
 *
 * Licensed under the Apache License, PelionConnectorUtils 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.pelion.connect.dm.exception;

import org.apache.kafka.connect.errors.ConnectException;

public class RequestFailedException extends ConnectException {

  private static final long serialVersionUID = 1L;

  private final String path;

  private final String request;

  private final Integer statusCode;

  private final String body;

  private final Exception exception;

  private String message;

  public RequestFailedException(Integer statusCode, String path, String request,
                                String body, Exception exception, String message) {
    super(String.format("HTTP response code:%d, path:%s, request:%s, body:%s, exception:%s , message:%s",
        statusCode,
        path,
        request,
        body,
        exception,
        message));
    this.statusCode = statusCode;
    this.path = path;
    this.request = request;
    this.body = body;
    this.exception = exception;
    this.message = message;
  }

  public String path() {
    return path;
  }

  public String request() {
    return request;
  }

  public Integer statusCode() {
    return statusCode;
  }

  public String body() {
    return body;
  }

  public Exception exception() {
    return exception;
  }

  public String message() {
    return message;
  }
}
