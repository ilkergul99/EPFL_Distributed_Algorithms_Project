package cs451.milestone1;

import cs451.Host;
import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;
import cs451.CallbackMethods.Forward;
import cs451.Connection.NetworkSender;
import cs451.Connection.NetworkObserver;
import cs451.Connection.NetworkReceiver;

import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;

/**
 * The FairLossChannel class implements a network channel with fair loss properties.
 * It handles the sending and receiving of messages over the network, ensuring that
 * messages are fairly lost without any bias. This class implements the Forward interface
 * for message delivery and NetworkObserver for observing network events.
 */
public class FairLossChannel implements Forward, NetworkObserver {
    // Number of threads in the thread pool. Determines the level of concurrent message processing.
    private final int availableThreadCount;

    // Number of proposals. Used in the context of lattice agreement or consensus algorithms.
    private final int teklifSize;

    // NetworkReceiver instance to handle incoming network messages.
    // It listens for messages on a specified port and processes them as they arrive.
    private final NetworkReceiver networkReceiver;

    // Forward interface implementation for handling the delivery of received messages.
    // It defines how messages should be forwarded after being received.
    private final Forward forwarder;

    // Thread pool for executing tasks concurrently.
    // Allows multiple network operations and message handling to occur in parallel.
    private final ExecutorService threadPool;

    // Counter for tracking the total number of jobs or tasks handled by this channel.
    // An AtomicInteger is used for thread-safe incrementation and access.
    private final AtomicInteger totalJobs;

    // A concurrent map to track and manage incoming messages.
    // Maps each Message to a Future representing its processing status.
    private final ConcurrentHashMap<Message, Future> inQueue;

    // A concurrent queue to hold NetworkSender tasks that need to be executed.
    // This allows for the efficient scheduling and execution of sending tasks.
    private final ConcurrentLinkedQueue<NetworkSender> toRun;

    // An array of DatagramSockets used for sending messages over the network.
    // Each socket can be used for sending a message to a specific destination.
    private final DatagramSocket[] messageSockets;

    /**
     * Constructor for the FairLossChannel class.
     *
     * @param port       The port number on which to listen for incoming UDP messages.
     * @param forwarder  The Forward interface implementation to handle delivered messages.
     * @param teklifSize The proposal number for Lattice Agreement.
     */
    FairLossChannel(int port, Forward forwarder, int teklifSize) {
        // Define the number of threads available for this channel, based on processing needs.
        this.availableThreadCount = 2;

        // Assign the provided Forward implementation to the forwarder member variable.
        this.forwarder = forwarder;

        // Initialize the inQueue as a ConcurrentHashMap to manage incoming messages concurrently.
        this.inQueue = new ConcurrentHashMap<>();

        // Initialize the messageSockets array with the size equal to availableThreadCount.
        messageSockets = new DatagramSocket[availableThreadCount];

        // Initialize the NetworkReceiver with the specified port, reference to this class, and teklifSize.
        this.networkReceiver = new NetworkReceiver(port, this, teklifSize);

        // Store the provided teklifSize in the corresponding member variable.
        this.teklifSize = teklifSize;

        // Initialize the thread pool with a fixed number of threads (availableThreadCount).
        this.threadPool = Executors.newFixedThreadPool(availableThreadCount);

        // Initialize the toRun queue as a ConcurrentLinkedQueue to manage tasks to be executed.
        this.toRun = new ConcurrentLinkedQueue<>();

        // Initialize the totalJobs counter as an AtomicInteger for thread-safe job counting.
        this.totalJobs = new AtomicInteger(0);

        // Iterate to initialize each DatagramSocket in the messageSockets array.
        for (int i = 0; i < availableThreadCount; i++) {
            try {
                // Create a new DatagramSocket and assign it to the current index in the array.
                messageSockets[i] = new DatagramSocket();
            } catch (Exception exception) {
                // Handle any exceptions during DatagramSocket initialization.
                exception.printStackTrace();
            }
        }
    }

    /**
     * Starts the network receiver. This method is used to initiate the process of receiving
     * messages. It should be called when the system is ready to start handling incoming network traffic.
     */
    void start() {
        // Calling the start method on networkReceiver.
        // This presumably initiates the receiver's thread or process, allowing it to begin
        // listening for and processing incoming network messages.
        networkReceiver.start();
    }

    /**
     * Checks if the specified message is present in the inQueue.
     * This method is used to determine if a message has been queued for processing.
     *
     * @param message The Message object to check in the queue.
     * @return true if the message is present in the inQueue, false otherwise.
     */
    public boolean isinQueue(Message message) {
        // Checking the inQueue (a ConcurrentHashMap) for the presence of the specified message.
        // The containsKey method is used to determine if the message is a key in the map.
        // This method returns true if the message is in the queue, false otherwise.
        return this.inQueue.containsKey(message);
    }


    /**
     * Shuts down network services and resources. This method is used to stop receiving messages,
     * shut down the thread pool, and close all open sockets. It ensures an orderly shutdown
     * of network operations.
     */
    void stop() {
        // Request the network receiver to stop receiving messages.
        networkReceiver.haltReceiving();

        // Shut down the thread pool immediately.
        // This will attempt to stop all actively executing tasks and halt the processing of waiting tasks.
        threadPool.shutdownNow();

        // Iterate through all message sockets to close them.
        for (int i = 0; i < messageSockets.length; i++) {
            // Check if the socket at the current index is not null to avoid NullPointerException.
            if (messageSockets[i] != null) {
                // Close the socket at the current index.
                messageSockets[i].close();
            }
        }
    }

    /**
     * Sends a batch of messages to a specified host. This method selects a socket, serializes the
     * message batch into bytes, and sends it using a NetworkSender task.
     *
     * @param batch The batch of messages to be sent.
     * @param host  The host (destination) to which the messages should be sent.
     */
    void send(MessageBatch batch, Host host) {
        // Randomly select a socket ID from the available message sockets for load balancing.
        int socketId = ThreadLocalRandom.current().nextInt(messageSockets.length);

        try {
            // Serialize the MessageBatch into a byte array.
            byte[] buffer = batch.toBytes(teklifSize);

            // Submit a new NetworkSender task to the thread pool for asynchronous sending.
            // The NetworkSender is responsible for sending the serialized data to the specified host.
            var consensusRounds = threadPool.submit(
                    new NetworkSender(host.getIp(), host.getPort(),
                            messageSockets[socketId], batch.copy(),
                            this, teklifSize
                    )
            );

            // For each message in the batch, associate it with the future consensusRounds.
            // This allows tracking of the message's handling in the network layer.
            for (Message message : batch.getMessages()) {
                this.inQueue.put(message, consensusRounds);
            }
        } catch (Exception e) {
            // Print the stack trace in case of an exception. This is helpful for debugging.
            e.printStackTrace();
        }
    }

    /**
     * Forwards a received message using the specified forwarder.
     * This method is part of the Forward interface implementation.
     *
     * @param message The Message to be forwarded.
     */
    @Override
    public void forward(Message message) {
        // Delegating the forwarding action to the forwarder object.
        // The forwarder is responsible for the actual forwarding logic.
        forwarder.forward(message);
    }

    /**
     * Retrieves the maximum lattice round that can be processed.
     * This method is part of the Forward interface implementation.
     *
     * @return The maximum lattice round number.
     */
    @Override
    public int getMaxLatticeRound() {
        // Delegating the retrieval of the maximum lattice round to the forwarder object.
        return forwarder.getMaxLatticeRound();
    }

    /**
     * Handles the completion of the transmission of a single message.
     * This method is part of the NetworkObserver interface implementation.
     *
     * @param message The Message whose transmission is complete.
     */
    @Override
    public void onSingleMessageTransmissionComplete(Message message) {
        // Removing the message from the inQueue upon completion of its transmission.
        // This indicates that the message no longer needs to be tracked.
        this.inQueue.remove(message);
    }

    /**
     * Handles the completion of the transmission of a batch of messages.
     * This method is part of the NetworkObserver interface implementation.
     */
    @Override
    public void onMessageBatchTransmissionComplete() {
        // Decrementing the total job counter when a batch transmission is complete.
        // This is used to track the number of active or completed jobs.
        this.totalJobs.decrementAndGet();
    }
}
