package rabbitmq_reactive;

import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.RpcClient;
import reactor.rabbitmq.Sender;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

public class RPCClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RPCClient.class);

    public static void main(String[] args) throws InterruptedException {
        String queue = "rpc.server.queue";
        Supplier<String> correlationIdSupplier = () -> UUID.randomUUID().toString();
        Sender sender = RabbitFlux.createSender();
        RpcClient rpcClient = sender.rpcClient(
                "", queue, correlationIdSupplier
        );
        Flux.range(1, 10)
                .parallel()
                .runOn(Schedulers.parallel())
                .doOnNext(i -> {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Mono<Delivery> reply = rpcClient.rpc(Mono.just(
                            new RpcClient.RpcRequest(i.toString().getBytes())
                    ));
                    reply.subscribe(r -> {
                        try {
                            String msg = new String(r.getBody(), "UTF-8");
                            LOGGER.info("Reply for " + i + ": " + msg);
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    });
                })
                .subscribe(j -> LOGGER.info("Sent message " + j));
        Thread.sleep(1000);
    }
}
