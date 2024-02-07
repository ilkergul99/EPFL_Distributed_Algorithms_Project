package cs451.Connection;

import cs451.CallbackMethods.Forward;
import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NetworkReceiver is a specialized thread that handles the reception of network packets.
 * It manages UDP socket communication, processing incoming data into message batches,
 * and storing them in a thread-safe queue for further handling.
 */
public class NetworkReceiver extends Thread{
    public final int teklifSize; // Size of the proposal data.
    private boolean threadRunning; // Flag to indicate if the process is active.
    private byte[] messageBuffer; // Buffer for data to be sent via DatagramSocket
    private DatagramSocket udpSocket; // Socket for network communication.
    private final Forward forwarder; // Interface for message forwarding logic.
    private final DatagramPacket incomingPacket; // Packet for sending data over the network.
    private final ConcurrentLinkedQueue<MessageBatch> bekleyenMesajlar; // Queue for storing message batches in a thread-safe manner.

    /**
     * Constructor for NetworkReceiver.
     *
     * @param port The port number on which the receiver will listen.
     * @param forwarder The forwarder that will handle received messages.
     * @param teklifSize Size of the proposal data.
     */
    public NetworkReceiver(int port, Forward forwarder, int teklifSize){
        // Sets the size of the proposal data.
        this.teklifSize = teklifSize;

        // Assigns the provided Forwarder object to handle incoming messages.
        this.forwarder = forwarder;

        // Initializes the message buffer to a calculated size based on teklifSize.
        // The buffer size is set to accommodate the structure of the expected messages.
        this.messageBuffer = new byte[8 * (11 + teklifSize * 4)];

        // Queue for holding messages that are waiting to be processed.
        this.bekleyenMesajlar = new ConcurrentLinkedQueue<>();

        // Prepares a DatagramPacket with the message buffer for receiving messages.
        this.incomingPacket = new DatagramPacket(messageBuffer, messageBuffer.length);

        try {
            // Opens a DatagramSocket on the specified port to receive incoming messages.
            this.udpSocket = new DatagramSocket(port);
        }
        catch (SocketException e) {
            // Prints the stack trace of any SocketException that occurs during socket creation.
            e.printStackTrace();
        }
    }


    /**
     * The main run method of the thread.
     * Continuously listens for incoming UDP packets and processes them.
     */
    @Override
    public void run() {
        threadRunning = true;

        // Continue executing as long as 'threadRunning' remains true.
        while (threadRunning) {
            try {
                // Waits to receive a DatagramPacket from the network.
                udpSocket.receive(incomingPacket);

                // Process the received DatagramPacket.
                processPacket(incomingPacket);

                // Process messages from the queue.
                processMessages();

            } catch (SocketException exception) {
                if (!threadRunning) {
                    // If the loop is no longer running, exit the loop gracefully.
                    break;
                }
                exception.printStackTrace();
            } catch (Exception exception) {
                // Log any errors encountered during packet reception or processing.
                exception.printStackTrace();
            }
        }
    }

    /**
     * Receives a DatagramPacket and processes it into a MessageBatch object.
     */
    private void processPacket(DatagramPacket targetPacket) {
        // Transform the received packet data into a MessageBatch object and add to the queue.
        MessageBatch messageBatch = MessageBatch.fromBytes(targetPacket.getData(), this.teklifSize);

        // Adds the newly created MessageBatch to the queue for further processing.
        this.bekleyenMesajlar.add(messageBatch);
    }

    /**
     * Processes all messages in the pending message queue.
     * Iterates through each message batch and processes individual messages.
     */
    private void processMessages() {
        // Continues processing as long as there are messages in the queue.
        while (!bekleyenMesajlar.isEmpty()) {
            // Retrieves and removes the next batch of messages from the queue.
            MessageBatch batchOfMessages = bekleyenMesajlar.poll();

            // Iterates over each message in the retrieved batch.
            for (Message singleMessage : batchOfMessages.getMessages()) {
                // Delegates the processing of each message to the forwarder.
                // The forwarder is responsible for handling the message based on its content and type.
                forwarder.forward(singleMessage);
            }
        }
    }

    /**
     * Halts the receiving of packets.
     * Sets the running flag to false and closes the socket.
     */
    public void haltReceiving() {
        threadRunning = false; // Stop the running loop
        udpSocket.close(); // Close the socket to free the resource
    }
}
