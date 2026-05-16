package com.alessandra.widecolumn.grpc;

import com.alessandra.widecolumn.config.DatabaseProperties;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class GrpcServerLifecycle implements ApplicationRunner {
    private final DatabaseProperties properties;
    private final WideColumnNodeService service;
    private Server server;

    public GrpcServerLifecycle(DatabaseProperties properties, WideColumnNodeService service) {
        this.properties = properties;
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.server = NettyServerBuilder.forPort(properties.getGrpcPort())
                .addService(service)
                .build()
                .start();
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
