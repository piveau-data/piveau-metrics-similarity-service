package de.fhg.fokus.edp.similarity_service;

import de.fhg.fokus.edp.similarity_service.model.SimilarityRequest;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static de.fhg.fokus.edp.similarity_service.ApplicationConfig.*;

public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);

    private JsonObject config;
    private ApiKeyHandler apiKeyHandler;

    @Override
    public void start() {
        LOG.info("Launching Dataset Similarity Service...");

        // startup is only successful if no step failed
        Future<Void> steps = loadConfig()
            .compose(handler -> initApiKey())
            .compose(handler -> bootstrapVerticles())
            .compose(handler -> startServer());

        steps.setHandler(handler -> {
            if (handler.succeeded()) {

                LOG.info("Dataset Similarity Service successfully launched");
            } else {
                handler.cause().printStackTrace();
                LOG.error("Failed to launch Dataset Similarity Service: " + handler.cause());
            }
        });
    }

    private Future<Void> loadConfig() {
        Future<Void> future = Future.future();

        ConfigRetriever configRetriever = ConfigRetriever.create(vertx);

        configRetriever.getConfig(handler -> {
            if (handler.succeeded()) {
                config = handler.result();
                LOG.info(config.encodePrettily());
                future.complete();
            } else {
                future.fail("Failed to load config: " + handler.cause());
            }
        });

        configRetriever.listen(change ->
            config = change.getNewConfiguration());

        return future;
    }

    private Future<Void> initApiKey() {
        Future<Void> future = Future.future();

        String apiKey = config.getString(ENV_API_KEY);

        if (apiKey != null && !apiKey.isEmpty()) {
            apiKeyHandler = new ApiKeyHandler(apiKey);
            future.complete();
        } else {
            future.fail("No API key specified");
        }

        return future;
    }

    private CompositeFuture bootstrapVerticles() {
        DeploymentOptions options = new DeploymentOptions()
            .setConfig(config)
            .setWorkerPoolName("extractor-pool")
            .setMaxWorkerExecuteTime(30)
            .setMaxWorkerExecuteTimeUnit(TimeUnit.MINUTES)
            .setWorker(true);

        List<Future> deploymentFutures = new ArrayList<>();
        deploymentFutures.add(startVerticle(options, SimilarityVerticle.class.getName()));
        deploymentFutures.add(startVerticle(options, FingerprintVerticle.class.getName()));

        return CompositeFuture.join(deploymentFutures);
    }

    private Future<Void> startServer() {
        Future<Void> startFuture = Future.future();
        Integer port = config.getInteger(ApplicationConfig.ENV_APPLICATION_PORT, DEFAULT_APPLICATION_PORT);

        OpenAPI3RouterFactory.create(vertx, "webroot/openapi.yaml", handler -> {
            if (handler.succeeded()) {
                OpenAPI3RouterFactory routerFactory = handler.result();
                RouterFactoryOptions options = new RouterFactoryOptions().setMountNotImplementedHandler(true).setMountValidationFailureHandler(true);
                routerFactory.setOptions(options);

                routerFactory.addSecurityHandler("ApiKeyAuth", apiKeyHandler::checkApiKey);

                routerFactory.addHandlerByOperationId("fingerprintLanguages", this::handleFingerprintRequest);
                routerFactory.addHandlerByOperationId("similaritiesForDataset", this::handleSimilarityRequest);

                Router router = routerFactory.getRouter();
                router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedHeader("Access-Control-Allow-Origin: *"));
                router.route("/*").handler(StaticHandler.create());

                HttpServer server = vertx.createHttpServer(new HttpServerOptions().setPort(port));
                server.requestHandler(router).listen();

                LOG.info("Server successfully launched on port [{}]", port);
                startFuture.complete();
            } else {
                // Something went wrong during router factory initialization
                LOG.error("Failed to start server at [{}]: {}", port, handler.cause());
                startFuture.fail(handler.cause());
            }
        });

        return startFuture;
    }

    private void handleFingerprintRequest(RoutingContext context) {
        vertx.eventBus().send(ADDRESS_START_FINGERPRINT, new JsonArray(context.queryParam("language")).encode());
        context.response().setStatusCode(202).end();
    }

    private void handleSimilarityRequest(RoutingContext context) {
        String datasetId = context.pathParam("datasetId");
        List<String> limitList = context.queryParam("limit");

        if (datasetId != null
            && limitList.size() == 1
            && StringUtils.isNumeric(limitList.get(0))) {

            SimilarityRequest request =
                new SimilarityRequest(datasetId, Integer.valueOf(limitList.get(0)));

            vertx.eventBus().send(ADDRESS_GET_SIMILARITY, Json.encode(request), sendHandler -> {
                if (sendHandler.succeeded()) {
                    context.response()
                        .setStatusCode(200)
                        .end((String) sendHandler.result().body());
                } else {
                    context.response().setStatusCode(500).end();
                }
            });
        } else {
            context.response().setStatusCode(400).end();
        }
    }

    private Future<Void> startVerticle(DeploymentOptions options, String className) {
        Future<Void> future = Future.future();

        vertx.deployVerticle(className, options, handler -> {
            if (handler.succeeded()) {
                future.complete();
            } else {
                LOG.error("Failed to deploy verticle [{}] : {}", className, handler.cause());
                future.fail("Failed to deploy [" + className + "] : " + handler.cause());
            }
        });

        return future;
    }
}
