package rabbitmq_reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SampleReceiver {

    private static final String QUEUE = "demo-queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleReceiver.class);

    private final Receiver receiver;
    private final Sender sender;

    public SampleReceiver() {
        this.receiver = RabbitFlux.createReceiver();
        this.sender = RabbitFlux.createSender();
    }

    public Disposable consume(String queue, CountDownLatch latch) {
        return receiver.consumeAutoAck(queue)
                .delaySubscription(sender.declareQueue(QueueSpecification.queue(queue)))
                .subscribe(m -> {
                    LOGGER.info("Received message {}", new String(m.getBody()));
                    latch.countDown();
                });
    }

    public void close() {
        this.sender.close();
        this.receiver.close();
    }

    public static void main(String[] args) throws Exception {
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);
        SampleReceiver receiver = new SampleReceiver();
        Disposable disposable = receiver.consume(QUEUE, latch);
        latch.await(10, TimeUnit.SECONDS);
        disposable.dispose();
        receiver.close();
    }

}