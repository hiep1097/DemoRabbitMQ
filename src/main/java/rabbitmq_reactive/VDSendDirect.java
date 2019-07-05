package rabbitmq_reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

public class VDSendDirect {
    private static final Logger LOGGER = LoggerFactory.getLogger(VDSendDirect.class);
    public static void main(String[] args) {
        Sender sender = RabbitFlux.createSender();

        Flux<OutboundMessage> outboundFlux  =
                Flux.range(1, 10)
                        .map(i -> new OutboundMessage(
                                "ex",
                                "def", ("Message " + i).getBytes()
                        ));
        sender.declare(ExchangeSpecification.exchange("ex").type("direct"))
                .subscribe(r -> System.out.println("Exchange declared"));
        Mono<Void> ss = sender.send(outboundFlux);
        ss.doOnError(e -> LOGGER.error("Send failed", e))
                .doOnSuccess(s-> {
                    LOGGER.info("complete");
                    sender.close();
                })
                .subscribe();


    }
}

