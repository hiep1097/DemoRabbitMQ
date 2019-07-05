package rabbitmq_reactive;

import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
public class SampleSender {

    private static final String QUEUE = "demo-queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleSender.class);

    private final Sender sender;

    public SampleSender() {
        this.sender = RabbitFlux.createSender();
    }

    public void send(String queue, int count, CountDownLatch latch) {
        Flux<OutboundMessageResult> confirmations = sender.sendWithPublishConfirms(Flux.range(1, count)
                .map(i -> new OutboundMessage("", queue, ("Message_" + i).getBytes())));
        sender.declareQueue(QueueSpecification.queue(queue))
                .thenMany(confirmations)
                .doOnError(e -> LOGGER.error("Send failed", e))
                .subscribe(r -> {
                    if (r.isAck()) {
                        LOGGER.info("Message {} sent successfully", new String(r.getOutboundMessage().getBody()));
                        latch.countDown();
                    }
                });
    }

    public void close() {
        this.sender.close();
    }

    public static void main(String[] args) throws Exception {
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);
        SampleSender sender = new SampleSender();
        sender.send(QUEUE, count, latch);
        latch.await(10, TimeUnit.SECONDS);
        sender.close();
    }

}
