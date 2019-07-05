package base;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException {
        String replyQueueName = channel.queueDeclare().getQueue();
        System.out.println(replyQueueName);


        //1
        final String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        //2
        final String corrId1 = UUID.randomUUID().toString();
        AMQP.BasicProperties props1 = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId1)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props1, message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(2);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            } else if (delivery.getProperties().getCorrelationId().equals(corrId1)) {
                response.offer("xxxxxx"+new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        StringBuilder result = new StringBuilder(response.take());
        try {
            result.append(response.take());
        } catch (Exception e){

        }
        channel.basicCancel(ctag);
        return result.toString();
    }

    public void close() throws IOException {
        connection.close();
    }
}