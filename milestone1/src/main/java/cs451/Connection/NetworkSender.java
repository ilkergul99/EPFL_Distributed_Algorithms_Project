package cs451.Connection;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;

/**
 * This class is responsible for sending a bulk of messages over UDP to a specified receiver.
 * After each message is sent, it notifies an observer about the completion of the send action.
 */
public class NetworkSender extends Thread {
    private DatagramSocket datagramSocket;
    private InetAddress receiverAddress;
    private int receiverPort;
    private NetworkObserver networkObserver;
    private byte[] messageDataBuffer;
    private int receiverIdentifier;
    private List<Integer> messageIdentifiers;

    /**
     * Constructs a UDPBulkSender instance.
     *
     * @param ipAddress      The IP address of the receiver.
     * @param port           The port number of the receiver.
     * @param receiverId     The unique identifier of the receiver.
     * @param dataBuffer     The buffer containing message data to send.
     * @param messageIds     A list of message IDs to notify the observer about.
     * @param socket         The socket used for sending the packet.
     * @param networkObserver    The observer to notify after sending the messages.
     */
    public NetworkSender(String ipAddress, int port, int receiverId, byte[] dataBuffer, List<Integer> messageIds, DatagramSocket socket, NetworkObserver networkObserver) {
        try {
            this.receiverPort = port;
            this.receiverAddress = InetAddress.getByName(ipAddress);
            this.datagramSocket = socket;
            this.messageDataBuffer = dataBuffer;
            this.networkObserver = networkObserver;
            this.receiverIdentifier = receiverId;
            this.messageIdentifiers = messageIds;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        // Create a UDP packet with the data to be sent, the destination address, and port.
        DatagramPacket packet = new DatagramPacket(messageDataBuffer, messageDataBuffer.length, receiverAddress, receiverPort);

        try {
            // Attempt to send the UDP packet through the socket.
            datagramSocket.send(packet);

            // For each message ID in the list of message identifiers, invoke the observer's method.
            // This is to notify that a message with the particular ID has been sent to the receiver.
            for (Integer messageId : messageIdentifiers) {
                networkObserver.observe(receiverIdentifier, messageId);
            }
        } catch (Exception e) {
            // If there's an I/O issue during the sending of the packet, print the stack trace for debugging.
            e.printStackTrace();
        } finally {
            // Clear the reference to the data buffer to allow the garbage collector to reclaim the memory.
            // This is especially useful if the buffer is large or if the system is memory-constrained.
            messageDataBuffer = null;
        }
    }


    /**
     * Closes the datagram socket.
     */
    public void close() {
        datagramSocket.close();
    }
}
