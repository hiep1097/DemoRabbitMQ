package rabbitmq_reactive;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class RPCServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RPCServer.class);

    public static void main(String[] args) {
        String queue = "rpc.server.queue";
        Receiver receiver = RabbitFlux.createReceiver();
        Flux<Delivery> inboundFlux = receiver
                .consumeNoAck(queue);
        Sender sender = RabbitFlux.createSender();
        inboundFlux.delaySubscription(sender.declareQueue(QueueSpecification.queue(queue)))
                .subscribe(d -> {
                    try {
                        String msg = new String(d.getBody(), "UTF-8");
                        LOGGER.info("Received " + msg);
                        String rep_msg = "Hello " + msg;
                        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                                .Builder()
                                .correlationId(d.getProperties().getCorrelationId())
                                .build();
                        Mono<OutboundMessage> reply =
                                Mono.just(new OutboundMessage("",
                                        d.getProperties().getReplyTo(), replyProps, rep_msg.getBytes()));
                        sender.send(reply).subscribe();
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                });
    }
}
