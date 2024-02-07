package cs451.Connection;

import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;
import cs451.CallbackMethods.Forward;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

// UDPReceiver class extends Thread for concurrent execution.
public class NetworkReceiver extends Thread {
    private boolean threadRunning; // Flag to control the running state of the thread
    private byte[] messageBuffer = new byte[64]; // Buffer for receiving UDP packets
    private DatagramSocket udpSocket; // Socket to receive UDP packets
    private final Forward forwarder; // Forward interface for message handling
    private final DatagramPacket dataPacket; // Datagram packet for receiving messages


    /**
     * Constructor for NetworkReceiver.
     *
     * @param port The port number on which the receiver will listen.
     * @param forwarder The forwarder that will handle received messages.
     */
    public NetworkReceiver(int port, Forward forwarder) {
        this.forwarder = forwarder;
        // Initialize DatagramPacket with buffer and its length
        this.dataPacket = new DatagramPacket(messageBuffer, messageBuffer.length);
        try {
            // Initialize DatagramSocket to listen on the specified port
            this.udpSocket = new DatagramSocket(port);
        } catch (SocketException e) {
            // Print stack trace in case of SocketException
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
        // Continue executing as long as 'running' remains true
        while (threadRunning) {
            try {
                // Wait and receive incoming packets
                udpSocket.receive(dataPacket);
                // Transform the received packet data into a MessageBatch object
                processPacket(dataPacket);
            } catch (SocketException exception) {
                // A SocketException might be thrown if the socket is closed while waiting for a packet.
                if (!threadRunning) {
                    // If the loop is no longer running, exit the loop gracefully.
                    break;
                }
                // Print the stack trace for debugging purposes if the exception was unexpected.
                exception.printStackTrace();
            }catch (Exception exception) {
                // Log any errors encountered during packet reception or processing
                exception.printStackTrace();
            }
        }
    }

    /**
     * Processes a given DatagramPacket.
     * Converts the packet's data into a MessageBatch and processes each message.
     * Messages with an ID of 0 are ignored.
     *
     * @param packet The DatagramPacket to be processed.
     */
    private void processPacket(DatagramPacket packet) {
        // Convert packet data to a MessageBatch object
        MessageBatch batchOfMessages = MessageBatch.fromBytes(packet.getData());

        // Process each message in the batch
        for (Message singleMessage : batchOfMessages.getMessages()) {
            // Skip messages with ID 0
            if (singleMessage.getId() == 0) continue;

            // Forward non-zero ID messages for processing
            forwarder.forward(singleMessage);
        }

        // Nullify the batchOfMessages reference to aid garbage collection
        batchOfMessages = null;
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
