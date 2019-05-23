/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.clustermanager;

import com.google.gson.JsonObject;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.server.RoutingHandler;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


public class UndertowServer {
  private final static Logger LOGGER = Logger.getLogger(UndertowServer.class.getName());

  public static String start(ContainerProcessManager manager) {

    final HttpHandler ROUTES = new RoutingHandler()
        .post("/container/{containerId}/to/{hostname}", new RequestHandler(manager))
        .get("/containers", new HttpHandler() {
          @Override
          public void handleRequest(HttpServerExchange exchange) throws Exception {
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            String json = new ObjectMapper().writeValueAsString(manager.getCurrentRunningContainers());
            LOGGER.info("List of Running containers is found to be {}" + json);
            exchange.getResponseSender().send(json);
          }});


    int port = 4040;
    /*
     *  "localhost" will ONLY listen on local host.
     *  If you want the server reachable from the outside you need to set "0.0.0.0"
     */
    String host = null;
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
      host = "localhost";
      LOGGER.info("Falling back to localhost");
    }

    /*
     * This web server has a single handler with no routing.
     * ALL urls are handled by the helloWorldHandler.
     */
    Undertow server = Undertow.builder()
        // Add the helloWorldHandler as a method reference.
        .addHttpListener(port, host, ROUTES)
        .build();
    LOGGER.info("starting on http://" + host + ":" + port);
    server.start();

    return "starting on http://" + host + ":" + port;
  }
}


class RequestHandler implements HttpHandler {
  private final static Logger LOGGER = Logger.getLogger(RequestHandler.class.getName());

  ContainerProcessManager manager = null;

  public RequestHandler(ContainerProcessManager manager) {
    this.manager = manager;
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) throws Exception {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
    String containerId = exchange.getQueryParameters().get("containerId").getFirst();
    String hostname = exchange.getQueryParameters().get("hostname").getFirst();
    exchange.getResponseSender().send(String.format("Requesting Container Move for containerId: %s to hostname: %s so sit back and chill", containerId, hostname));
    manager.requestMoveContainer(containerId, hostname);
  }
}
