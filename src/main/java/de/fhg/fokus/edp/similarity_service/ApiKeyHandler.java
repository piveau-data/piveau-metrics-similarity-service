package de.fhg.fokus.edp.similarity_service;

import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;

class ApiKeyHandler {

    private final String apiKey;

    ApiKeyHandler(String api_key) {
        this.apiKey = api_key;
    }

    void checkApiKey(RoutingContext context) {

        final String authorization = context.request().headers().get(HttpHeaders.AUTHORIZATION);

        if (this.apiKey.isEmpty()) {
            context.response().setStatusCode(500);
        } else if (authorization == null || !authorization.equals(this.apiKey)) {
            context.response().setStatusCode(401).end();
        } else {
            context.next();
        }
    }
}
