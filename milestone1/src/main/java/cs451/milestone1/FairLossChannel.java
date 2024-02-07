package cs451.milestone1;

import cs451.Host;
import cs451.Forward;
import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;
import cs451.Connection.NetworkSender;
import cs451.Connection.NetworkObserver;
import cs451.Connection.NetworkReceiver;

import java.net.DatagramSocket;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Implements Fair Loss Links using UDP. This ensures that messages are sent
 * with UDP's delivery guarantees, but doesn't provide any additional reliability.
 */
public class FairLossChannel implements Forward, NetworkObserver {
    private final DatagramSocket[] senderSockets;
    private final int maxSenderThreads;
    private final ExecutorService senderThreadPool;

    private final NetworkReceiver networkReceiver;
    private final Forward messageDeliverer;

    private final ConcurrentHashMap<Map.Entry<Integer,Integer>, Boolean> pendingMessageAcknowledgments;


    /**
     * Constructor for FairLossLinks.
     *
     * @param port         The port to listen on for incoming messages.
     * @param forwarder    The Deliverer to delegate message delivery to.
     * @param hostSize     The number of hosts in the environment, used to calculate thread numbers.
     */
    FairLossChannel(int port, Forward forwarder, int hostSize) {
        this.networkReceiver = new NetworkReceiver(port, this);
        this.messageDeliverer = forwarder;

        // Set the maximum number of sender threads to 8 directly
        this.maxSenderThreads = 8;
        //System.out.println("Sender thread count fixed to: " + maxSenderThreads);

        this.senderThreadPool = Executors.newFixedThreadPool(maxSenderThreads);

        // Initialize sockets for sending messages
        senderSockets = new DatagramSocket[maxSenderThreads];
        for (int i = 0; i < maxSenderThreads; i++) {
            try {
                senderSockets[i] = new DatagramSocket();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        pendingMessageAcknowledgments = new ConcurrentHashMap<>();
    }

    /**
     * Sends a MessagePackage to the specified host.
     *
     * @param messageBatch The MessagePackage to send.
     * @param host           The destination Host.
     */
    void send(MessageBatch messageBatch, Host host) {
        // Select a random socket index. This helps in load balancing if there are multiple sender sockets.
        int socketIndex = ThreadLocalRandom.current().nextInt(senderSockets.length);

        // Before sending, mark each message in the batch as pending acknowledgment.
        // This is stored in a map that keeps track of which messages are awaiting acknowledgments.
        for (Message message : messageBatch.getMessages()) {
            // Use Map.entry to create an immutable map entry for the key
            pendingMessageAcknowledgments.put(Map.entry(message.getReceiverId(), message.getId()), true);
        }

        // Create a new NetworkSender task. This encapsulates the message sending operation.
        // The NetworkSender is constructed with the IP and port of the host to send to,
        // the ID of the receiver, the serialized bytes of the MessageBatch, and a reference
        // to the current object (this) to notify upon message send completion.
        NetworkSender networkSenderTask = new NetworkSender(
                host.getIp(),
                host.getPort(),
                host.getId(),
                messageBatch.toBytes(),
                messageBatch.getMessageIds(),
                senderSockets[socketIndex],
                this
        );

        // Submit the NetworkSender task to the thread pool for execution.
        // This allows the message sending to be processed asynchronously.
        senderThreadPool.submit(networkSenderTask);
    }


    /**
     * Starts the receiver thread to listen for incoming messages.
     */
    void start() {
        networkReceiver.start();
    }

    /**
     * Stops the receiver and closes all sender sockets to clean up resources.
     */
    void stop() {
        networkReceiver.haltReceiving();
        for (DatagramSocket socket : senderSockets) {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
        senderThreadPool.shutdown();
    }

    /**
     * Checks if the send queue is full.
     *
     * @return true if the send queue is full, false otherwise.
     */
    boolean isWorkloadAboveThreshold() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) senderThreadPool;
        // Calculate the number of tasks that are awaiting execution
        long pendingTasks = executor.getQueue().size();
        // Add the number of active tasks to get the total number of uncompleted tasks
        long totalUncompletedTasks = pendingTasks + executor.getActiveCount();
        // Check if the total number of uncompleted tasks exceeds some threshold
        return totalUncompletedTasks >= maxSenderThreads * 2;
    }


    /**
     * Checks if a message with a specific ID is in the queue.
     *
     * @param receiverId The receiver's identifier.
     * @param messageId  The message's identifier.
     * @return true if the message is in the queue, false otherwise.
     */
    Boolean isMessageScheduledForSending(int receiverId, int messageId) {
        return pendingMessageAcknowledgments.containsKey(Map.entry(receiverId, messageId));
    }

    /**
     * Delivers a message using the underlying Deliverer implementation.
     *
     * @param message The message to deliver.
     */
    @Override
    public void forward(Message message) {
        messageDeliverer.forward(message);
    }

    /**
     * Callback for when a UDP sender has completed sending a message.
     *
     * @param receiverId The receiver's identifier.
     * @param messageId  The message's identifier.
     */
    @Override
    public void observe(int receiverId, int messageId) {
        pendingMessageAcknowledgments.remove(Map.entry(receiverId, messageId));
    }
}
