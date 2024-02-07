package cs451.milestone1;

import cs451.Host;
import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;
import cs451.CallbackMethods.Forward;
import cs451.Connection.NetworkSender;
import cs451.Connection.NetworkObserver;
import cs451.Connection.NetworkReceiver;

import java.net.DatagramSocket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements Fair Loss Links using UDP sockets. It provides the functionality
 * to send and receive messages reliably over a network using UDP protocol.
 */
public class FairLossChannel implements Forward, NetworkObserver {
    private final int THREAD_NUMBER; // Number of threads in the thread pool
    private final NetworkReceiver receiver; // Receiver to handle incoming Network messages
    private final Forward forwarder; // Forwarder interface to handle message delivery
    private final ExecutorService threadPool; // Thread pool for handling tasks concurrently
    private final AtomicInteger jobCount; // Counter for the number of jobs
    private final ConcurrentHashMap<Message, Boolean> inQueue; // Concurrent map to track incoming messages
    private final DatagramSocket[] messageSockets; // Array of DatagramSockets for sending messages

    /**
     * Constructor for the FairLossLinks class.
     *
     * @param port The port number on which to listen for incoming UDP messages.
     * @param forwarder The Forward interface implementation to handle delivered messages.
     */
    FairLossChannel(int port, Forward forwarder) {
        // Initialize the Network receiver
        this.receiver = new NetworkReceiver(port, this);
        // Set the fowarder for handling message delivery
        this.forwarder = forwarder;
        // A concurrent map to track incoming messages
        this.inQueue = new ConcurrentHashMap<>();
        // Thread count calculation based on processes and host size
        this.THREAD_NUMBER = 2; // Calculated based on the max number of processes per host and thread requirements
        // Initialize the thread pool
        this.threadPool = Executors.newFixedThreadPool(THREAD_NUMBER);
        // Initialize the job counter
        this.jobCount = new AtomicInteger(0);
        // Initialize an array of DatagramSockets for sending messages
        messageSockets = new DatagramSocket[THREAD_NUMBER];
        for (int i = 0; i < THREAD_NUMBER; i++) {
            try {
                messageSockets[i] = new DatagramSocket();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Sends a package of messages to the specified host.
     * A random socket from the available sockets is chosen to send the message.
     *
     * @param messageBatch The package of messages to be sent.
     * @param host The destination host.
     */
    void send(MessageBatch messageBatch, Host host) {
        // Select a random socket for sending the message
        int socketId = ThreadLocalRandom.current().nextInt(messageSockets.length);
        // Increment the job count
        this.jobCount.addAndGet(1);
        // Add each message in the package to the inQueue map
        for (Message message : messageBatch.getMessages()) {
            this.inQueue.put(message, false);
        }
        // Submit a new UDPBulkSender task to the thread pool
        threadPool.submit(new NetworkSender(
                host.getIp(),
                host.getPort(),
                messageBatch.copy(),
                messageSockets[socketId],
                this));
    }

    /**
     * Starts the Network receiver to listen for incoming messages.
     */
    void start() {
        receiver.start();
    }

    /**
     * Stops the UDP receiver and shuts down the thread pool.
     * Also closes all DatagramSockets.
     */
    void stop() {
        // Stop receiving messages
        receiver.haltReceiving();
        // Shut down the thread pool
        threadPool.shutdownNow();
        // Close all sockets
        for (DatagramSocket socket : messageSockets) {
            socket.close();
        }
    }

    /**
     * Checks if the message queue is full.
     *
     * @return True if the queue is full, false otherwise.
     */
    public boolean isQueueFull() {
        // This method currently always returns false. If needed, it can be modified to check the job count.
        return false;
    }

    /**
     * Checks if a given message is in the queue.
     *
     * @param message The message to check.
     * @return True if the message is in the queue, false otherwise.
     */
    public boolean isinQueue(Message message) {
        return this.inQueue.containsKey(message);
    }

    /**
     * Delivers a message using the configured Deliverer.
     *
     * @param message The message to be delivered.
     */
    @Override
    public void forward(Message message) {
        forwarder.forward(message);
    }

    /**
     * Callback method triggered when a single message has been sent.
     * It removes the message from the inQueue map.
     *
     * @param message The message that was sent.
     */
    @Override
    public void onSingleMessageTransmissionComplete(Message message) {
        this.inQueue.remove(message);
    }

    /**
     * Callback method triggered when a message batch has been sent.
     * It decrements the job count.
     */
    @Override
    public void onMessageBatchTransmissionComplete() {
        this.jobCount.decrementAndGet();
    }

}
