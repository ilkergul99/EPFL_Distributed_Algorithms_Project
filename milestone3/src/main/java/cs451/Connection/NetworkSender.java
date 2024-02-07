package cs451.Connection;

import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;
import cs451.milestone1.FairLossChannel;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;

/**
 * NetworkSender is a thread that handles the sending of network packets via UDP.
 * It encapsulates the functionality for creating and transmitting message batches,
 * and communicates with a network observer for event tracking and management.
 */
public class NetworkSender extends Thread {
    private DatagramSocket outgoingSocket; // Socket for sending and receiving network datagrams.
    private InetAddress networkAddress; // IP address for the network communication.
    private int sendingPort; // Port number used for the network communication.
    private final MessageBatch messageBatch; // Container for batching messages before sending them over the network.
    private final int teklifSize; // Size of the proposal set, used in the lattice agreement protocol.
    private final NetworkObserver networkObserver; // Observer for monitoring and managing network events and activities.

    /**
     * Constructs a NetworkSender instance for managing network communications.
     *
     * @param ipAddress IP address for network communication.
     * @param sendingPort Port number for sending data.
     * @param outgoingSocket DatagramSocket used for sending data.
     * @param messageBatch Container for messages to be sent.
     * @param networkObserver Observer for network activities.
     * @param teklifSize Size of the proposal set.
     */
    public NetworkSender(String ipAddress, int sendingPort, DatagramSocket outgoingSocket,
                         MessageBatch messageBatch, NetworkObserver networkObserver, int teklifSize) {
        // Assigns the provided MessageBatch and proposal set size (teklifSize) to this instance.
        this.messageBatch = messageBatch;
        this.teklifSize = teklifSize;

        // Assigns the provided NetworkObserver to this instance for monitoring network activities.
        this.networkObserver = networkObserver;

        try {
            // Sets the sending port for network communication.
            this.sendingPort = sendingPort;

            // Converts the provided IP string to an InetAddress object for network communication.
            this.networkAddress = InetAddress.getByName(ipAddress);

            // Assigns the provided DatagramSocket for outgoing network communications.
            this.outgoingSocket = outgoingSocket;
        }
        catch (Exception e) {
            // Catches and prints the stack trace of any exceptions that occur during setup.
            e.printStackTrace();
        }
    }

    /**
     * The main run method of the thread.
     * Responsible for sending the message package as a single UDP packet and notifying the observer upon completion.
     */
    @Override
    public void run() {
        try {
            // Prepare a DatagramPacket from the message batch for sending.
            DatagramPacket packet = preparePacket();

            // Send the prepared packet over the network using the outgoing socket.
            outgoingSocket.send(packet);

            // After sending the packet, notify the observer for each message in the batch.
            notifyObserver();
        } catch (Exception exception) {
            // Catch and log any exceptions that occur during packet preparation, sending, or observer notification.
            exception.printStackTrace();
        }
    }

    /**
     * Notifies the network observer for each message in the message batch.
     * This method iterates through all messages in the batch and triggers a notification for each.
     */
    private void notifyObserver() {
        // Loop through each message in the message batch.
        for (Message individualMessage : messageBatch.getMessages()) {
            // Notify the network observer about the completion of the transmission of each individual message.
            // The observer is expected to handle any post-transmission logic for each message.
            networkObserver.onSingleMessageTransmissionComplete(individualMessage);
        }
    }

    /**
     * Prepares a DatagramPacket from the message batch.
     * Converts the batch of messages into a byte array and encapsulates it in a DatagramPacket.
     *
     * @return The prepared DatagramPacket ready for transmission.
     */
    private DatagramPacket preparePacket() {
        // Converts the message batch into a byte array for transmission.
        // The 'toBytes' method serializes the batch according to 'teklifSize'.
        byte[] sendingMessages = messageBatch.toBytes(teklifSize);

        // Creates a new DatagramPacket with the serialized message data.
        // The packet is prepared for sending to 'networkAddress' on 'sendingPort'.
        return new DatagramPacket(sendingMessages, sendingMessages.length, networkAddress, sendingPort);
    }

    public MessageBatch getMessageBatch(){
        return this.messageBatch;
    }

    /**
     * Closes the DatagramSocket to free up resources.
     */
    public void close() {
        // Closing socket
        outgoingSocket.close();
    }
}
