package cs451.milestone1;

import cs451.CallbackMethods.Forward;
import cs451.Host;
import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The StubbornChannel class is responsible for managing message transmission in a network,
 * ensuring that messages and acknowledgements are sent even in the presence of potential
 * network failures or message loss. It utilizes both FairLossChannel and PerfectChannel
 * to handle different aspects of message delivery and acknowledgement.
 */
public class StubbornChannel implements Forward {
    // Counter to track the number of acknowledgements or other relevant events.
    private int acknowledgmentCounter;

    // FairLossChannel instance used for handling message transmission with fair loss properties.
    private final FairLossChannel fairLossChannel;

    // Map to track pending messages that need to be sent. Each host (identified by a Byte) has its own map of messages.
    private final ConcurrentHashMap<Byte, ConcurrentHashMap<Message, Boolean>> pendingMessages;

    // Map to track pending acknowledgements to be sent, similar to pending messages.
    private final ConcurrentHashMap<Byte, ConcurrentHashMap<Message, Boolean>> pendingAcknowledgements;

    // Total number of hosts in the network. Used to determine the range for iterating over hosts.
    private final int hostSize;

    // Runnable for periodically sending messages. It handles the logic for message transmission.
    private final Runnable messageSender;

    // Runnable for periodically sending acknowledgements. It follows a similar logic to messageSender.
    private final Runnable acknowledgementSender;

    // AtomicBoolean to safely control the running state of messageSender and acknowledgementSender.
    private final AtomicBoolean isRunning;

    // HashMap storing host information, keyed by host ID (Byte). Provides easy access to host data.
    private final HashMap<Byte, Host> hostMapByIdentifier;

    // PerfectChannel instance used for handling message transmission with guaranteed delivery.
    private final PerfectChannel perfectChannel;

    /**
     * Constructs a StubbornChannel instance. This channel manages robust message and acknowledgement
     * transmissions by utilizing a FairLossChannel and a PerfectChannel. It's designed to ensure
     * reliable communication in a network with a specified set of hosts.
     *
     * @param port            The port number on which the FairLossChannel will listen for incoming messages.
     * @param hostMapByIdentifier           A mapping of host IDs to Host objects for the network.
     * @param perfectChannel  An instance of PerfectChannel for handling guaranteed delivery.
     * @param teklifSize The size of the proposal set, relevant for message processing.
     */
    public StubbornChannel(int port, HashMap<Byte, Host> hostMapByIdentifier,
                           PerfectChannel perfectChannel,
                           int teklifSize) {
        // Define the acknowledgement sender thread which will periodically process and send acknowledgements.
        this.acknowledgementSender = () -> runPeriodicTask(this::processCurrentAcknowledgement, 400);

        // Initialize ConcurrentHashMaps to track pending messages and acknowledgements.
        this.pendingMessages = new ConcurrentHashMap<>();
        this.pendingAcknowledgements = new ConcurrentHashMap<>();

        // Initialize the count to track the number of acknowledgements or other relevant events.
        this.acknowledgmentCounter = 0;

        // Initialize FairLossChannel with provided port, this channel as a forwarder, and the proposal set size.
        this.fairLossChannel = new FairLossChannel(port, this, teklifSize);

        // Store the provided mapping of host IDs to Host objects.
        this.hostMapByIdentifier = hostMapByIdentifier;

        // Set the total number of hosts based on the size of the provided hosts map.
        this.hostSize = hostMapByIdentifier.size();

        // Set the isRunning flag to true, indicating that the channel is active and ready to process messages.
        this.isRunning = new AtomicBoolean(true);

        // Define the message sender thread which will periodically process and send messages.
        this.messageSender = () -> runPeriodicTask(this::processCurrentMessage, 800);

        // Assign the provided PerfectChannel instance to handle guaranteed message delivery.
        this.perfectChannel = perfectChannel;

    }


    /**
     * Runs a specified task periodically based on the given sleep duration. The task is executed
     * repeatedly as long as the 'isActive' condition remains true. This method is designed to facilitate
     * the execution of background tasks at regular intervals, such as sending acknowledgments or
     * performing periodic checks.
     *
     * The method ensures that the provided task is executed in a safe manner by catching and logging
     * any exceptions that may occur during its execution. After each execution, the thread is put to sleep
     * for a specified duration to control the rate of task execution.
     *
     * @param task The Runnable task to be executed periodically.
     * @param sleepDuration The duration (in milliseconds) for which the thread should sleep between consecutive task executions.
     */
    private void runPeriodicTask(Runnable task, int sleepDuration) {
        // Continuously run the task while the isActive flag is true.
        while (isRunning.get()) {
            try {
                // Execute the given task.
                task.run();
            } catch (Exception e) {
                // Catch and print any exceptions that occur during task execution.
                e.printStackTrace();
            }
            // Pause execution for a specified duration to control the rate of task execution.
            sleepThread(sleepDuration);
        }
    }

    /**
     * Causes the currently executing thread to sleep for a specified number of milliseconds.
     * If the thread is interrupted while sleeping, the interruption is handled gracefully:
     * the interrupt flag is set again on the thread, and the exception is logged, ensuring
     * that the thread's interrupted status is properly maintained.
     *
     * @param milliseconds The duration for which the thread should sleep, in milliseconds.
     */
    private void sleepThread(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            // Re-interrupt the thread to maintain the interrupted status
            Thread.currentThread().interrupt();
            // Log the interrupted exception
            e.printStackTrace();
        }
    }

    /**
     * Processes and forwards messages in the pendingMessages map to their respective hosts.
     * This method iterates over all hosts and sends any pending messages available for each host.
     */
    private void processCurrentMessage() {
        // Iterating over the range of host IDs.
        for (int i = 0; i < hostSize; i++) {
            // Calculating the host ID, wrapping around using the modulus operator to ensure it stays within range.
            byte hostId = (byte) (i % hostSize);

            // Check if there are any pending messages for the current host ID.
            if (pendingMessages.containsKey(hostId) && !pendingMessages.get(hostId).isEmpty()) {
                // Retrieve a list of pending messages for the current host.
                var messagesForCurrentHost = Collections.list(pendingMessages.get(hostId).keys());

                // Sending the retrieved messages to the corresponding host.
                sendPendingMessage(messagesForCurrentHost, this.hostMapByIdentifier.get(hostId));
            }
        }
    }

    /**
     * Processes and forwards acknowledgements in the pendingAcknowledgements map to their respective hosts.
     * This method iterates over all hosts and sends any pending acknowledgements available for each host.
     */
    private void processCurrentAcknowledgement() {
        // Iterating over the range of host IDs.
        for (int i = 0; i < hostSize; i++) {
            // Calculating the host ID, wrapping around using the modulus operator to ensure it stays within range.
            byte hostId = (byte) (i % hostSize);

            // Check if there are any pending acknowledgements for the current host ID.
            if (pendingAcknowledgements.containsKey(hostId) && !pendingAcknowledgements.get(hostId).isEmpty()) {
                // Retrieve a list of pending acknowledgements for the current host.
                var acknowledgementsForCurrentHost = Collections.list(pendingAcknowledgements.get(hostId).keys());

                // Sending the retrieved acknowledgements to the corresponding host.
                sendPendingAcknowledgementMessage(acknowledgementsForCurrentHost, this.hostMapByIdentifier.get(hostId));
            }
        }
    }

    /**
     * Sends pending messages to a specified host. This method filters out delivered messages
     * and messages already in the queue, and batches them for sending. The messages are batched
     * in groups of up to 8 before being sent.
     *
     * @param messages The list of messages to be potentially sent.
     * @param host     The host to which the messages should be sent.
     */
    private void sendPendingMessage(List<Message> messages, Host host) {
        // Check if the messages list is empty to avoid unnecessary processing.
        if (messages.isEmpty()) {
            return; // No messages to send, exit the method early.
        }

        List<Message> messagesToSend = new ArrayList<>(); // List to hold the batch of messages to be sent.

        for (Message m : messages) {
            // Remove the message if it has already been delivered and recorded by the perfect channel.
            if (!m.isDeliveredMessage() && perfectChannel.isDelivered(m.getLatticeRound())) {
                pendingMessages.get(m.getReceiverId()).remove(m); // Remove from pending since it's delivered.
                continue; // Move to the next message in the loop.
            }

            // Add the message to the batch only if it's not already in the queue to be sent.
            if (!fairLossChannel.isinQueue(m)) {
                messagesToSend.add(m); // Add to the batch.

                // Check if the batch has reached its capacity of 8 messages.
                if (messagesToSend.size() == 8) {
                    fairLossChannel.send(new MessageBatch(messagesToSend), host); // Send the batch of 8 messages.
                    messagesToSend.clear(); // Clear the list for the next batch.
                }
            }
        }

        // After processing all messages, check if there's a remaining batch to send.
        if (!messagesToSend.isEmpty()) {
            fairLossChannel.send(new MessageBatch(messagesToSend), host); // Send the remaining messages.
        }
    }

    /**
     * Sends pending acknowledgement messages to the specified host. This method filters out messages
     * already in the queue and batches them for sending. The messages are batched in groups of up to 8
     * before being sent.
     *
     * @param messages The list of acknowledgement messages to be potentially sent.
     * @param host     The host to which the acknowledgement messages should be sent.
     */
    private void sendPendingAcknowledgementMessage(List<Message> messages, Host host) {
        List<Message> messagesToSend = new ArrayList<>(); // List to hold the batch of acknowledgement messages to be sent.

        // Iterate over each message in the provided list.
        for (Message m : messages) {
            // Add the message to the batch only if it's not already in the queue to be sent.
            if (!fairLossChannel.isinQueue(m)) {
                messagesToSend.add(m); // Add to the batch.
                pendingAcknowledgements.get(m.getReceiverId()).remove(m); // Remove from pending acknowledgements.

                // Check if the batch has reached its capacity of 8 messages.
                if (messagesToSend.size() == 8) {
                    fairLossChannel.send(new MessageBatch(messagesToSend), host); // Send the batch of 8 messages.
                    messagesToSend.clear(); // Clear the list for the next batch.
                }
            }
        }

        // After processing all messages, check if there's a remaining batch to send.
        if (!messagesToSend.isEmpty()) {
            fairLossChannel.send(new MessageBatch(messagesToSend), host); // Send the remaining messages.
        }
    }

    /**
     * Initializes and starts the necessary components and threads.
     * This method activates the fairLossChannel and sets up two threads for message and acknowledgment sending.
     */
    public void start() {
        // Activates the fair loss channel, preparing it for use.
        fairLossChannel.start();

        // Creating and configuring the thread for sending messages.
        Thread messageSenderThread = new Thread(this.messageSender);
        // Setting the priority slightly below the maximum to ensure efficient execution while maintaining system balance.
        messageSenderThread.setPriority(Thread.MAX_PRIORITY - 2);
        // Starting the message sending thread.
        messageSenderThread.start();

        // Creating and configuring the thread for sending acknowledgments.
        Thread acknowledgmentSenderThread = new Thread(this.acknowledgementSender);
        // Setting the priority to the maximum for immediate acknowledgment handling.
        acknowledgmentSenderThread.setPriority(Thread.MAX_PRIORITY);
        // Starting the acknowledgment sending thread.
        acknowledgmentSenderThread.start();
    }

    /**
     * Queues a message for sending to the specified host. The method categorizes the message into
     * acknowledgment messages or regular messages and stores them in appropriate data structures.
     *
     * @param message The Message object to be sent.
     * @param host The Host to which the message is to be sent.
     */
    public void send(Message message, Host host) {
        // Check if the message is an acknowledgment message or a delivered acknowledgment message
        if (message.isAckMessage() || message.isDeliveredAck()) {
            // Ensures there is a mapping for this receiver. If not, it initializes one.
            pendingAcknowledgements.computeIfAbsent(message.getReceiverId(), k -> new ConcurrentHashMap<>());
            // Adds the message to the acknowledgment messages map, marking it for sending.
            pendingAcknowledgements.get(message.getReceiverId()).put(message, true);
        } else {
            // For regular messages, ensures a mapping for this receiver exists.
            pendingMessages.computeIfAbsent(message.getReceiverId(), k -> new ConcurrentHashMap<>());
            // Adds the message to the regular messages map, marking it for sending.
            pendingMessages.get(message.getReceiverId()).put(message, true);
        }
    }


    /**
     * Stops the operation of this component and any associated subcomponents.
     * It signals this component to cease its operations and also stops the underlying
     * FairLossChannel. This method is typically used to gracefully shut down all activities
     * and network communication initiated by this component.
     */
    public void stop() {
        // Atomically sets the isRunning flag to false if it's currently true.
        this.isRunning.compareAndSet(true, false);

        // This ensures that all network activities are also gracefully terminated.
        fairLossChannel.stop();
    }


    /**
     * Stops the message sending operation. This method is used to signal sender threads or processes
     * to stop their execution. It sets the isRunning flag from true to false, indicating that sending
     * operations should cease.
     */
    public void stopSenders() {
        // Atomically sets the isRunning flag to false if it's currently true.
        // This is a thread-safe way to stop sender threads or processes.
        this.isRunning.compareAndSet(true, false);
    }


    /**
     * Forwards a message based on its type. This method checks if the message is a delivered acknowledgment
     * or a standard acknowledgment and handles it accordingly. If the message is neither, it forwards
     * the message through the perfect channel, which is responsible for the actual transmission.
     *
     * @param currentMessage The Message object to be forwarded.
     */
    @Override
    public void forward(Message currentMessage) {
        // Check and handle delivered acknowledgment messages.
        if (currentMessage.isDeliveredAck()) {
            processDeliveredAcknowledgement(currentMessage);
            return;
        }

        // Check and handle acknowledgment messages.
        if (currentMessage.isAckMessage()) {
            processAcknowledgementMessage(currentMessage);
        }

        // Forward the message using the perfect channel.
        // The perfect channel handles the actual transmission of the message.
        perfectChannel.forward(currentMessage);
    }


    /**
     * Handles a delivered acknowledgment message.
     * @param currentMessage The message to be handled.
     */
    private void processDeliveredAcknowledgement(Message currentMessage) {
        // Create a response currentMessage based on the delivered acknowledgment currentMessage.
        Message originalMessage = currentMessage.createResponseMessage();

        // Remove the original currentMessage from the pending messages queue of the receiver.
        // This step acknowledges that the currentMessage has been successfully delivered.
        pendingMessages.get(originalMessage.getReceiverId()).remove(originalMessage);
    }

    /**
     * Handles an acknowledgment message.
     * @param currentMessage The message to be handled.
     */
    private void processAcknowledgementMessage(Message currentMessage) {
        // Create a response currentMessage based on the acknowledgment currentMessage.
        Message originalMessage = currentMessage.createResponseMessage();
        // Remove the original currentMessage from the pending messages queue of the receiver.
        // The return value indicates whether the removal was successful.
        Boolean removal = pendingMessages.get(originalMessage.getReceiverId()).remove(originalMessage);
        // If the currentMessage was successfully removed, increment the count.
        if (removal != null && removal) {
            acknowledgmentCounter += 1;
        }
    }

    /**
     * Retrieves the maximum lattice round that can be processed from the perfect channel.
     *
     * @return The maximum lattice round number.
     */
    @Override
    public int getMaxLatticeRound() {
        // Delegating the retrieval of the maximum lattice round to the perfect channel.
        // The perfect channel is assumed to have the most up-to-date information on the
        // maximum lattice round that has been processed or is currently in processing.
        return perfectChannel.getMaxLatticeRound();
    }

}
