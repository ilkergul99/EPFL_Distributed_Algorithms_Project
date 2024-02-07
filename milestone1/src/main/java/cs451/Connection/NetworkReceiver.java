package cs451.Connection;

import cs451.Forward;
import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The UDPReceiver class extends Thread and is responsible for receiving UDP packets,
 * converting them to Message objects, and then delivering them using the provided Deliverer.
 */
public class NetworkReceiver extends Thread {
    private volatile boolean running; // Flag to control the receiver loop
    private final byte[] receiveBuffer = new byte[128]; // Buffer for receiving packets
    private DatagramSocket socket; // The socket to receive UDP packets
    private final Forward upperLayerDeliverer; // The delivery mechanism for received messages

    private final DatagramPacket incomingPacket; // Packet structure for incoming data

    /**
     * Constructor for the UDPReceiver.
     *
     * @param port        The port number on which to listen for incoming UDP packets.
     * @param upperLayerDeliverer   The deliverer instance for delivering received messages.
     * @param maxMemory   The maximum allowed memory usage for receiving packets.
     */
    public NetworkReceiver(int port, Forward upperLayerDeliverer) {

        this.upperLayerDeliverer = upperLayerDeliverer;

        this.incomingPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length); // Initialize the packet for receiving data

        // Attempt to create the socket for receiving packets
        try {
            this.socket = new DatagramSocket(port);

        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    /**
     * The main run method executed by the thread, listens for UDP packets and processes them.
     */
    public void run() {
        // Set the running flag to true to start the processing loop.
        running = true;

        // Continue processing incoming messages as long as the 'running' flag is true.
        while (running) {
            try {
                // Attempt to receive a packet from the network.
                socket.receive(incomingPacket);

                // Deserialize the bytes received into a batch of messages.
                MessageBatch receivedMessages = MessageBatch.fromBytes(incomingPacket.getData());

                // Iterate through each message in the batch.
                for (Message message : receivedMessages.getMessages()) {
                    // Check for a sentinel value of message ID. If it's 0, skip the processing.
                    if (message.getId() == 0) {
                        continue;
                    }

                    // Pass the message to the upper layer for further processing.
                    upperLayerDeliverer.forward(message);
                }

                // Set the receivedMessages to null to suggest to the garbage collector that
                // the memory can be reclaimed.
                receivedMessages = null;

            } catch (SocketException e) {
                // A SocketException might be thrown if the socket is closed while waiting for a packet.
                if (!running) {
                    // If the loop is no longer running, exit the loop gracefully.
                    break;
                }
                // Print the stack trace for debugging purposes if the exception was unexpected.
                e.printStackTrace();
            } catch (Exception e) {
                // Catch all other exceptions and print their stack trace for debugging.
                e.printStackTrace();
            }
        }
    }


    /**
     * Halts the receiver from processing any more incoming packets and closes the socket.
     */
    public void haltReceiving() {
        running = false; // Set running to false to stop the loop
        // Close the socket to release the port
        // This also makes sure that if the thread is blocked on receive(), it throws a SocketException
        if (!socket.isClosed()) {
            socket.close();
        }
    }

}
