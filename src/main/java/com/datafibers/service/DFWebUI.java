package com.datafibers.service;

import io.vertx.core.AbstractVerticle;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.log4j.Logger;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class DFWebUI extends AbstractVerticle {

    private static final Logger LOG = Logger.getLogger(DFWebUI.class);

    @Override
    public void start() {

        // Create a router object for web ui
        Router routerWeb = Router.router(vertx);

        // Bind web ui
        routerWeb.route("/admin/*").handler(StaticHandler.create("admin").setCachingEnabled(false));
        // Bind api doc
        routerWeb.route("/api/*").handler(StaticHandler.create("apidoc").setCachingEnabled(true));
        // Bind landing page
        routerWeb.route("/*").handler(StaticHandler.create("landing").setCachingEnabled(true));

        // Create the HTTP server to serve the web ui
        vertx.createHttpServer().requestHandler(routerWeb::accept)
                .listen(config().getInteger("http.port.df.processor", 8000));

        try {
            LOG.info("DataFibers Welcome You @ http://" + InetAddress.getLocalHost().getHostAddress() + ":" +
                    config().getInteger("http.port.df.processor", 8000));
        } catch (UnknownHostException e) {
            LOG.error("NetworkHostException", e.getCause());
        }
    }
}
