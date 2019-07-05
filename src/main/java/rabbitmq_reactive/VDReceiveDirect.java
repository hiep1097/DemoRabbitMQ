package rabbitmq_reactive;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.*;

import java.io.UnsupportedEncodingException;

import static reactor.rabbitmq.ResourcesSpecification.*;

public class VDReceiveDirect {
    private static final Logger LOGGER = LoggerFactory.getLogger(VDReceiveDirect.class);

    public static void main(String[] args) {
        Sender sender = RabbitFlux.createSender();
        Receiver receiver = RabbitFlux.createReceiver();
        Flux<Delivery> inboundFlux = receiver
                .consumeNoAck("queue2");
        inboundFlux
                .delaySubscription(
                        sender.declare(ExchangeSpecification.exchange("ex").type("direct"))
                                .then(sender.declare(queue("queue2")))
                                .then(sender.bind(binding("ex", "ghi", "queue2"))))
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
