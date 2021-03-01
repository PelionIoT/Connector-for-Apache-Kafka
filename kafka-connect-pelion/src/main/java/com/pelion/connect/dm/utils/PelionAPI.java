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

package com.pelion.connect.dm.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pelion.connect.dm.exception.RequestFailedException;
import com.pelion.connect.dm.source.PelionSourceConnectorConfig;
import com.pelion.connect.dm.source.PelionSourceTaskConfig;
import com.pelion.protobuf.PelionProtos.DeviceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.pelion.connect.dm.utils.PelionConnectorUtils.getFutureDateFromNow;

public class PelionAPI {

  private static final Logger LOG = LoggerFactory.getLogger(PelionAPI.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private final HttpClient client;
  private final String baseURL;
  private final String bearer;

  // default base API host
  public static final String DEFAULT_PELION_API_HOST = "api.us-east-1.mbedcloud.com";
  // default group that all tasks would be bound to
  private static final String DEFAULT_GROUP_NAME = "Administrators";

  private WebSocket webSocket;

  private enum HttpMethod {
    GET, PUT, POST, DELETE
  }

  private enum HttpStatus {
    SUCCESS(200),
    CREATED(201),
    ACCEPTED(202),
    NO_CONTENT(204);

    int code;

    private HttpStatus(int code) {
      this.code = code;
    }
  }

  private static final String SUBSCRIPTION_KEY_ENDPOINT_NAME = "endpoint-name";
  private static final String SUBSCRIPTION_KEY_ENDPOINT_TYPE = "endpoint-type";
  private static final String SUBSCRIPTION_KEY_RESOURCE_PATH = "resource-path";

  public PelionAPI(String bearer) {
    this(DEFAULT_PELION_API_HOST, bearer);
  }

  public PelionAPI(String baseURL, String bearer) {
    this.baseURL = baseURL;
    this.bearer = bearer;

    this.client = HttpClient.newBuilder().build();
  }

  public String getAccountId() {
    String accountId = null;

    try {
      final HttpResponse<String> response = get("v3/accounts/me");

      // parse response
      final JsonNode body = mapper.readTree(response.body());
      final int statusCode = response.statusCode();
      if (statusCode == HttpStatus.SUCCESS.code) {
        accountId = body.get("id").asText();
      } else {
        throw new RuntimeException(String.format("failed to get account-id for application, got response: status:%d body:%s",
            statusCode, body));
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("an error has occurred during retrieving of account-id", e);
    }

    return accountId;
  }

  public String getGroupId(final String accountId) {
    String groupId = null;

    try {
      final HttpResponse<String> response = get(String.format("v3/accounts/%s/policy-groups", accountId));

      // parse response for group-id
      final JsonNode body = mapper.readTree(response.body());
      final int statusCode = response.statusCode();
      if (statusCode != HttpStatus.SUCCESS.code) {
        throw new RuntimeException(String.format("failed to retrieve group-id for account-id '%s', got response: status:%d body:%s",
            accountId, statusCode, body));
      }

      final JsonNode data = body.get("data");
      for (final JsonNode node : data) {
        if (DEFAULT_GROUP_NAME.equals(node.get("name").asText())) {
          groupId = node.get("id").asText();
          break;
        }
      }

    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("an error has occurred during retrieving of group-id", e);
    }

    return groupId;
  }

  public String createApplication(final String accountId, final String groupId, final String name) {
    String appId = null;

    try {
      // first, delete any application with the same name, if exists means we
      // have run at least once. We need to recreate a number of access-keys
      // according to connectors 'maxTasks'.
      // Unfortunately, the Pelion API doesn't return the access-keys in full
      deleteApplication(accountId, name);

      final ObjectNode jsonObj = mapper.createObjectNode();
      jsonObj
          .put("name", name)
          .put("description", String.format("used by Kafka connector '%s'", name))
          .putArray("groups")
          .add(groupId);

      final String path = String.format("v3/accounts/%s/applications", accountId);
      final HttpResponse<String> response = post(path, jsonObj);

      // parse response
      final JsonNode body = mapper.readTree(response.body());
      final int statusCode = response.statusCode();
      if (statusCode == HttpStatus.CREATED.code) {
        // extract application-id
        appId = body.get("id").asText();
        LOG.debug("created application-id '{}' for application with name '{}'", appId, name);
      } else {
        throw new RuntimeException(String.format("failed to create application with name '%s', got response: status:%d body:%s", name,
            statusCode, body));
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(String.format("an error has occurred during creation of application with name '%s'",
          name), e);
    }

    return appId;
  }

  public String[] createAccessKeys(String accountId, String groupId, String name, int count) {
    String[] accessKeys = new String[count];

    try {
      // a pair of Pelion application-id and access-key would be created
      for (int i = 0; i < count; i++) {
        // create application-id
        final String appId = createApplication(accountId, groupId, String.format("%s-task%d-app", name, i));
        // create access-key bound to this application-id
        final ObjectNode jsonObj = mapper.createObjectNode();
        jsonObj
            .put("name", String.format("%s-task%d-key", name, i))
            .put("description", String.format("used by tasks for Kafka connector '%s'", name))
            .put("expiration", getFutureDateFromNow(365));

        final String path = String.format("v3/accounts/%s/applications/%s/access-keys",
            accountId, appId);
        final HttpResponse<String> response = post(path, jsonObj);

        // parse response
        final JsonNode body = mapper.readTree(response.body());
        final int statusCode = response.statusCode();
        if (statusCode == HttpStatus.CREATED.code) {
          // extract access key
          accessKeys[i] = body.get("key").asText();
          LOG.debug("created access-key '{}' for application with name '{}'", accessKeys[i], name);
        } else {
          throw new RuntimeException(
              String.format("failed to create access key for application with name '%s' and id '%s', got response: status:%d body:%s",
                  name, appId, statusCode, body));
        }
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          String.format("an error has occurred during creation of access-key for application with name '%s'",
              name), e);
    }

    return accessKeys;
  }

  public void deleteApplication(String accountId, String name) {
    try {
      String path;
      JsonNode body;
      int statusCode;

      // we need first to determine the application-id for this name (if exists)
      String appId = null;

      // retrieve application list
      path = String.format("v3/accounts/%s/applications", accountId);
      HttpResponse<String> response = get(path);

      // parse response for application-id
      body = mapper.readTree(response.body());
      statusCode = response.statusCode();
      if (statusCode != HttpStatus.SUCCESS.code) {
        throw new RuntimeException(String.format("failed to delete application with name '%s', got response: status:%d body:%s",
            name, statusCode, body));
      }

      final JsonNode data = body.get("data");
      if (data.size() > 0) {
        for (final JsonNode node : data) {
          if (name.equals(node.get("name").asText())) {
            appId = node.get("id").asText();
            break;
          }
        }
      }

      // application does not exist, no need to continue
      if (appId == null) {
        return;
      }

      // ok, we can now delete it
      path = String.format("v3/accounts/%s/applications/%s",
          accountId, appId);
      response = delete(path);
      statusCode = response.statusCode();
      if (statusCode == HttpStatus.NO_CONTENT.code) {
        LOG.debug("deleted application-id with name '{}'", name);
      } else { // if not '204 - Deleted Successfully' an error has occurred, raise it
        // parse error response
        body = mapper.readTree(response.body());
        throw new RuntimeException(String.format("failed to delete application-id: '%s', got response: status:%d body:%s",
            appId, statusCode, body));
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(String.format("an error has occurred during deletion of application-id: '%s'",
          name), e);
    }
  }

  public void createPreSubscriptions(final PelionSourceTaskConfig config) {
    final List<String> aliases = config.getList(PelionSourceConnectorConfig.SUBSCRIPTIONS_CONFIG);

    try {
      final ArrayNode jsonArr = mapper.createArrayNode();
      for (final String sub : aliases) {
        final ObjectNode jsonObj = mapper.createObjectNode();

        final String endpointName = config.getString(
            String.format("subscriptions.%s.%s", sub, SUBSCRIPTION_KEY_ENDPOINT_NAME));
        if (endpointName != null) {
          jsonObj.put(SUBSCRIPTION_KEY_ENDPOINT_NAME, endpointName);
        }
        final String endpointType = config.getString(
            String.format("subscriptions.%s.%s", sub, SUBSCRIPTION_KEY_ENDPOINT_TYPE));
        if (endpointType != null) {
          jsonObj.put(SUBSCRIPTION_KEY_ENDPOINT_TYPE, endpointType);
        }
        final List<String> resourcePathList = config.getList(
            String.format("subscriptions.%s.%s", sub, SUBSCRIPTION_KEY_RESOURCE_PATH));
        if (resourcePathList != null && resourcePathList.size() > 0) {
          ArrayNode jsonArrPaths = jsonObj.putArray(SUBSCRIPTION_KEY_RESOURCE_PATH);
          for (final String path : resourcePathList) {
            jsonArrPaths.add(path);
          }
        }

        jsonArr.add(jsonObj);
      }

      final HttpResponse<String> response = put("v2/subscriptions", jsonArr);
      final int statusCode = response.statusCode();
      if (statusCode == HttpStatus.NO_CONTENT.code) {
        LOG.debug("created pre-subscriptions '{}'", jsonArr);
      } else {
        throw new RuntimeException(String.format("failed to create pre-subscriptions, got response: status:%d body:%s",
            statusCode, response.body()));
      }

    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("an error has occurred during creation of pre-subscriptions", e);
    }
  }

  public WebSocket connectNotificationChannel(WebSocket.Listener listener) {
    // first we need to setup a new ws notification channel
    try {
      final ObjectNode jsonObj = mapper.createObjectNode();
      jsonObj.putObject("serialization")
          .put("type", "v2")
          .put("max_chunk_size", 10000)
          .putObject("cfg")
          .put("deregistrations_as_object", true)
          .put("include_uid", true)
          .put("include_timestamp", true)
          .put("include_original_ep", true);

      final HttpResponse<String> response = put("v2/notification/websocket", jsonObj);

      // parse response
      final JsonNode body = mapper.readTree(response.body());
      final int statusCode = response.statusCode();
      if (statusCode == HttpStatus.CREATED.code
          || statusCode == HttpStatus.SUCCESS.code) {
        LOG.debug("created websocket channel");
      } else {
        throw new RuntimeException(String.format("failed to create web socket channel, got response: status:%d body:%s", statusCode, body));
      }

      // ready to connect
      String wssURI = String.format("wss://%s/v2/notification/websocket-connect", this.baseURL);
      return HttpClient.newBuilder().build().newWebSocketBuilder()
          .subprotocols("wss", String.format("pelion_%s", this.bearer))
          .buildAsync((URI.create(wssURI)), listener).get();

    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException("an error has occurred during creation of websocket channel", e);
    }
  }

  public RequestFailedException executeDeviceRequest(final DeviceRequest request) {
    final ObjectNode jsonObj = mapper.createObjectNode();

    putIfNotDefault(jsonObj, "method", request.getBody().getMethod().name());
    putIfNotDefault(jsonObj, "uri", request.getBody().getUri());
    putIfNotDefault(jsonObj, "accept", request.getBody().getAccept());
    putIfNotDefault(jsonObj, "content-type", request.getBody().getContentType());
    putIfNotDefault(jsonObj, "payload-b64", request.getBody().getPayloadB64());

    String path = null;
    int statusCode = 0;
    JsonNode body = null;

    try {
      path = String.format("v2/device-requests/%s?async-id=%s",
          request.getEp(),
          request.getAsyncId());

      if (request.getRetry() != 0) {
        path += String.format("&retry=%d", request.getRetry());
      }

      if (request.getExpirySeconds() != 0) {
        path += String.format("&expiry-seconds=%d", request.getExpirySeconds());
      }

      final HttpResponse<String> response = post(path, jsonObj);

      // parse response
      body = mapper.readTree(response.body());
      statusCode = response.statusCode();
      if (statusCode != HttpStatus.ACCEPTED.code) {
        return new RequestFailedException(statusCode, path, jsonAsString(jsonObj), jsonAsString(body), null,
            "response code differs from expected 202(Accepted)");
      }
    } catch (IOException | InterruptedException e) {
      return new RequestFailedException(statusCode, path, jsonObj.toString(), "", e, e.getMessage());
    }

    return null;
  }

  // -------------------------------------------
  //          Internal API Methods
  // -------------------------------------------

  private HttpResponse<String> get(final String path) throws IOException, InterruptedException {
    return request(path, HttpMethod.GET);
  }

  private HttpResponse<String> delete(final String path) throws IOException, InterruptedException {
    return request(path, HttpMethod.DELETE);
  }

  private HttpResponse<String> put(final String path, final JsonNode data) throws IOException, InterruptedException {
    return request(path, HttpMethod.PUT, data);
  }

  private HttpResponse<String> post(final String path, final JsonNode data) throws IOException, InterruptedException {
    return request(path, HttpMethod.POST, data);
  }

  private HttpResponse<String> request(final String path, final HttpMethod method) throws IOException, InterruptedException {
    return request(path, method, null);
  }

  private HttpResponse<String> request(final String path, HttpMethod method, final JsonNode reqBody)
      throws IOException, InterruptedException {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(String.format("https://%s/%s", this.baseURL, path)))
        .header("Authorization", "Bearer " + this.bearer)
        .header("Content-Type", "application/json");

    final HttpRequest request;
    switch (method) {
      case PUT:
        request = builder.PUT(HttpRequest.BodyPublishers.ofString(jsonAsString(reqBody))).build();
        break;
      case POST:
        request = builder.POST(HttpRequest.BodyPublishers.ofString(jsonAsString(reqBody))).build();
        break;
      case DELETE:
        request = builder.DELETE().build();
        break;
      case GET:
        request = builder.GET().build();
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + method);
    }

    // send request
    LOG.debug("---> calling: {} ", request.toString());
    //LOG.debug("headers: '{} body: '{}'", request.headers(), jsonAsString(reqBody));
    LOG.debug("body: '{}'", jsonAsString(reqBody));

    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    LOG.debug("<--- response: ");
    LOG.debug("http status: '{}', headers: '{}'", response.statusCode(), response.headers());
    LOG.debug("body: '{}'", response.body());

    return response;
  }

  private String jsonAsString(JsonNode data) throws JsonProcessingException {
    if (data == null) {
      return ""; // used for debug
    }
    return mapper
        //.writerWithDefaultPrettyPrinter()
        .writeValueAsString(data);
  }

  private void putIfNotDefault(ObjectNode node, String name, String value) {
    if (!value.isEmpty()) {
      node.put(name, value);
    }
  }

}
