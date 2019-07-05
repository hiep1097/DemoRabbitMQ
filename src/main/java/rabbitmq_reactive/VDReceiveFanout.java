package rabbitmq_reactive;

import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.ExchangeSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

import java.io.UnsupportedEncodingException;

import static reactor.rabbitmq.ResourcesSpecification.binding;
import static reactor.rabbitmq.ResourcesSpecification.queue;

public class VDReceiveFanout {
    private static final Logger LOGGER = LoggerFactory.getLogger(VDReceiveFanout.class);

    public static void main(String[] args) {
        Sender sender = RabbitFlux.createSender();

        sender.declare(ExchangeSpecification.exchange("ex_fanout").type("fanout"))
                .then(sender.declare(queue("queue2")))
                .then(sender.bind(binding("ex_fanout", "", "queue2")))
                .subscribe(r -> System.out.println("Exchange and queue declared and bound"));
        Receiver receiver = RabbitFlux.createReceiver();
        Flux<Delivery> inboundFlux = receiver
                .consumeNoAck("queue2");
        inboundFlux
                .subscribe(d -> {
                    try {
                        String msg = new String(d.getBody(), "UTF-8");
                        LOGGER.info("Received message {}", msg);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                });
    }
}
