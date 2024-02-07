package cs451.Connection;

import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * NetworkSender extends the Thread class to send a bulk of messages over UDP in a separate thread.
 * It is responsible for sending a package of messages to a specified IP address and port.
 */
public class NetworkSender extends Thread {
    private DatagramSocket socket; // Socket to send UDP packets
    private InetAddress address; // IP address to send the packets to
    private int port; // Port to send the packets to
    private NetworkObserver networkObserver; // Observer to notify upon completion of sending
    private MessageBatch messageBatch; // Package of messages to send

    /**
     * Constructor for NetworkSender.
     *
     * @param ip IP address as a string where the messages will be sent.
     * @param port Port number where the messages will be sent.
     * @param messageBatch The package of messages to be sent.
     * @param socket The DatagramSocket for sending packets.
     * @param NetworkObserver The observer that will be notified after sending operations.
     */
    public NetworkSender(String ip, int port, MessageBatch messageBatch, DatagramSocket socket, NetworkObserver networkObserver) {
        try {
            this.port = port;
            this.address = InetAddress.getByName(ip); // Convert string IP to InetAddress
            this.socket = socket;
            this.networkObserver = networkObserver;
            this.messageBatch = messageBatch;
        } catch (Exception e) {
            // Print stack trace in case of an exception
            e.printStackTrace();
        }
    }

    /**
     * Closes the DatagramSocket to free up resources.
     */
    public void close() {
        // Closing socket
        socket.close();
    }

    /**
     * The main run method of the thread.
     * Sends the message package as a single UDP packet and notifies the observer upon completion.
     */
    @Override
    public void run() {
        // Transform the messages in the batch to a byte array for transmission
        byte[] sendData = messageBatch.toBytes();
        // Prepare the DatagramPacket with the necessary information for sending
        DatagramPacket outgoingPacket = new DatagramPacket(sendData, sendData.length, address, port);
        try {
            // Transmit the prepared packet via the socket
            socket.send(outgoingPacket);
            // Signal the observer that the entire message batch has been sent
            networkObserver.onMessageBatchTransmissionComplete();
            // Individually notify the observer for every message dispatched
            for (Message individualMessage : messageBatch.getMessages()) {
                networkObserver.onSingleMessageTransmissionComplete(individualMessage);
            }
        } catch (Exception exception) {
            // Log any exceptions that occur during the sending process
            exception.printStackTrace();
        }
    }

}
