package rabbitmq_reactive;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Sender;

import java.io.UnsupportedEncodingException;

public class VDReceive {
    private static final Logger LOGGER = LoggerFactory.getLogger(VDReceive.class);
    public static void main(String[] args) {
        Flux<Delivery> inboundFlux = RabbitFlux.createReceiver()
                .consumeNoAck("routing.key");
        Sender sender = RabbitFlux.createSender();
        Mono<AMQP.Queue.DeclareOk> queue = sender.declareQueue(
                QueueSpecification.queue("routing.key")
        );
        inboundFlux.delaySubscription(queue).subscribe(d->{
            try {
                String msg = new String(d.getBody(), "UTF-8");
                LOGGER.info("Received message {}",msg);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });
    }
}
