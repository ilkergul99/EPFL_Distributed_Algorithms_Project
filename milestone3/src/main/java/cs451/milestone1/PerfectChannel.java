package cs451.milestone1;

import cs451.Host;
import cs451.DataPacket.Message;
import cs451.CallbackMethods.Forward;
import cs451.CallbackMethods.LatticeForward;

import java.util.*;

/**
 * The PerfectChannel class represents a communication channel that ensures reliable message delivery
 * and forwarding in a distributed system. It uses a StubbornChannel for guaranteed message delivery
 * and a LatticeForward mechanism for managing message proposals and rounds.
 */
public class PerfectChannel implements Forward {
    // Array to keep track of the count of acknowledgements received for each round
    private int[] roundAcknowledgementTally;

    // Array to keep track of the count of negative acknowledgements received for each round
    private int[] negativeAckCountPerRound;

    // The unique identifier for the current node in the network
    private final byte currentNodeId;

    // The number of parallel rounds that the channel can handle concurrently
    private final int concurrentRoundsLimit;

    // Map of Hosts indexed by their unique Byte identifier
    private final HashMap<Byte, Host> hostMapByIdentifier;

    // LatticeForward instance used for managing and forwarding proposals in the lattice
    private final LatticeForward latticeForwarder;

    // Array of HashSet, each storing delivered values (represented as bytes) for different rounds
    private final HashSet<Byte>[] deliveredValues;

    // StubbornChannel instance used for reliable message delivery
    private final StubbornChannel stubbornChannel;


    /**
     * Constructs a PerfectChannel instance with specified parameters. This channel integrates
     * stubborn message delivery, lattice-based forwarding, and manages multiple rounds and proposal sets.
     *
     * @param port The port number for the StubbornChannel.
     * @param currentNodeId The unique identifier of the current node.
     * @param latticeForwarder The LatticeForward instance used for proposal forwarding.
     * @param hostMapByIdentifier The map of all hosts in the network.
     * @param concurrentRoundsLimit The number of rounds that can be handled concurrently.
     * @param teklifSize The size of the proposal set for the StubbornChannel.
     */
    public PerfectChannel(int port,
                          byte currentNodeId,
                          LatticeForward latticeForwarder,
                          HashMap<Byte, Host> hostMapByIdentifier,
                          int concurrentRoundsLimit,
                          int teklifSize) {

        // Assigns the provided map of hosts to the class variable
        this.hostMapByIdentifier = hostMapByIdentifier;

        // Initializes an array to track the number of negative acknowledgements for each round
        this.negativeAckCountPerRound = new int[concurrentRoundsLimit];

        // Sets the unique identifier for the current node
        this.currentNodeId = currentNodeId;

        // Sets the number of parallel rounds the channel can handle
        this.concurrentRoundsLimit = concurrentRoundsLimit;

        // Assigns the provided LatticeForward instance to the class variable
        this.latticeForwarder = latticeForwarder;

        // Initializes an array to track the number of acknowledgements for each round
        this.roundAcknowledgementTally = new int[concurrentRoundsLimit];

        // Initializes the StubbornChannel with the provided port, hosts, current instance, and proposal set size
        this.stubbornChannel = new StubbornChannel(port, hostMapByIdentifier, this, teklifSize);

        // Initializes an array of HashSet to store delivered values for each round
        deliveredValues = new HashSet[concurrentRoundsLimit];

        // Populates the array with a new HashSet for each round
        for (int i = 0; i < concurrentRoundsLimit; i++) {
            deliveredValues[i] = new HashSet<>();
        }
    }

    /**
     * Sends a message to the specified host using the StubbornChannel.
     * This method ensures reliable delivery of the message.
     *
     * @param message The message to be sent.
     * @param host The host to which the message is to be sent.
     */
    public void send(Message message, Host host) {
        // Delegates the message sending task to the StubbornChannel instance
        // StubbornChannel ensures that the message is delivered reliably
        stubbornChannel.send(message, host);
    }

    /**
     * Starts the StubbornChannel for message transmission.
     * This method initializes and prepares the channel for sending and receiving messages.
     */
    public void start() {
        // Activates the StubbornChannel, enabling it to send and receive messages
        // This could involve starting threads, opening sockets, or other initialization tasks
        stubbornChannel.start();
    }

    /**
     * Retrieves the maximum lattice round that has been reached so far.
     * This method is used to understand the progress of the lattice-based communication process.
     *
     * @return The highest lattice round number that has been handled by the lattice forwarder.
     */
    @Override
    public int getMaxLatticeRound() {
        // Delegates to the LatticeForward instance to get the maximum lattice round
        // This reflects the latest round in the consensus or coordination process
        return latticeForwarder.getMaxLatticeRound();
    }

    /**
     * Checks if a specific lattice round has been delivered successfully.
     * This method is useful for ensuring that all operations for a given round are completed.
     *
     * @param latticeRound The lattice round to check.
     * @return True if the specified lattice round has been delivered, false otherwise.
     */
    public boolean isDelivered(int latticeRound) {
        // Queries the LatticeForward instance to check if the specified round has been delivered
        // This is important for tracking the completion status of rounds in the distributed system
        return latticeForwarder.isDelivered(latticeRound);
    }

    /**
     * Stops the PerfectChannel, particularly the underlying StubbornChannel.
     * This method is used to cleanly shutdown the channel's operations.
     */
    public void stop() {
        // Stops the StubbornChannel, which may involve closing sockets or stopping threads
        // This is typically called when the channel is no longer needed
        stubbornChannel.stop();
    }

    /**
     * Stops all message senders in the StubbornChannel.
     * This can be used to halt outgoing message traffic while keeping the channel active.
     */
    public void stopSenders() {
        // Instructs the StubbornChannel to stop all its sender threads or processes
        // This method is useful for controlling the flow of outgoing messages
        stubbornChannel.stopSenders();
    }

    /**
     * Handles a delivered message by incrementing the acknowledgment count and sending a confirmation.
     *
     * @param message The message that has been confirmed as delivered.
     */
    private void handleDeliveredMessage(Message message) {
        // Increment the count of delivered acknowledgments for the given lattice round and sender.
        latticeForwarder.acknowledgeProposalDelivery(message.getLatticeRound(), message.getSenderId());

        // Create and send a new confirmation message back to the sender.
        // The message ID is set to 1 and the boolean flag 'true' indicates successful delivery.
        send(new Message(1, currentNodeId, message.getSenderId(), message.getLatticeRound(), true), hostMapByIdentifier.get(message.getSenderId()));
    }

    /**
     * Processes an acknowledgment message if it meets certain criteria.
     *
     * @param message The acknowledgment message to be processed.
     */
    private void handleAcknowledgeMessage(Message message) {
        // Ignore message if it doesn't meet specific criteria.
        if (shouldIgnoreAcknowledgeMessage(message)) return;

        // Skip processing if the message's round has already been delivered.
        if (isDelivered(message.getLatticeRound())) return;

        // Process the acknowledgment message for counting and decision-making.
        processAcknowledgeMessage(message);
    }

    /**
     * Determines whether an acknowledgment message should be ignored based on its relevance.
     *
     * @param message The acknowledgment message to evaluate.
     * @return true if the message should be ignored, false otherwise.
     */
    private boolean shouldIgnoreAcknowledgeMessage(Message message) {
        // Returns true to ignore the message if its ID does not match the active proposal number
        // for its corresponding lattice round. This check ensures only relevant messages are processed.
        return message.getId() != latticeForwarder.fetchCurrentProposalNumber(message.getLatticeRound());
    }

    /**
     * Processes an acknowledgment message by updating acknowledgment counts and
     * potentially making decisions based on these counts.
     *
     * @param message The acknowledgment message to process.
     */
    private void processAcknowledgeMessage(Message message) {
        // Calculate the index for the current lattice round within the parallel round count.
        int roundIndex = message.getLatticeRound() % concurrentRoundsLimit;

        // Add the sender's ID to the set for this round. Process further only if this is the first time
        // the sender's ID is being added, ensuring each sender is counted only once per round.
        if (deliveredValues[roundIndex].add(message.getSenderId())) {
            // Update the counts of acknowledgments and negative acknowledgments.
            updateAcknowledgeCounts(message, roundIndex);

            // Check if a decision should be made based on the updated counts and make the decision if necessary.
            checkAndProcessLatticeAgreement(message, roundIndex);
        }
    }

    /**
     * Updates the counts of acknowledgments and negative acknowledgments for a given round.
     *
     * @param message    The acknowledgment message containing the count information.
     * @param roundIndex The index of the current lattice round.
     */
    private void updateAcknowledgeCounts(Message message, int roundIndex) {
        // Increment the acknowledgment count if the message is an acknowledgment (type 1).
        if (message.getAcknowledgementMessageType() == 1) {
            roundAcknowledgementTally[roundIndex]++;
        }
        // Increment the negative acknowledgment count if the message is a negative acknowledgment (type 2).
        // Also, add any new proposals from the message to the lattice forwarder for this round.
        else if (message.getAcknowledgementMessageType() == 2) {
            negativeAckCountPerRound[roundIndex]++;
            latticeForwarder.getProposal(message.getLatticeRound()).addAll(message.getProposals());
        }
    }

    /**
     * Checks whether a decision can be made based on the accumulated acknowledgments and negative acknowledgments for a round.
     * If the threshold is reached, it either decides on a value or broadcasts a new proposal.
     *
     * @param message    The acknowledgment message.
     * @param roundIndex The index of the current lattice round.
     */
    private void checkAndProcessLatticeAgreement(Message message, int roundIndex) {
        // Check if the combined count of ACKs and NACKs reaches the threshold (half of the hosts).
        if (roundAcknowledgementTally[roundIndex] + negativeAckCountPerRound[roundIndex] >= hostMapByIdentifier.size() / 2) {
            deliveredValues[roundIndex].clear();

            // If there are no negative acknowledgments, decide on the value for this round.
            if (negativeAckCountPerRound[roundIndex] == 0) {
                latticeForwarder.finalizeRoundDecision(message.getLatticeRound());
            }
            // If there are negative acknowledgments, broadcast a new proposal for this round.
            else {
                latticeForwarder.sendNewRoundProposal(message.getLatticeRound());
            }

            // Reset the acknowledgment and negative acknowledgment counts for the next decision process.
            roundAcknowledgementTally[roundIndex] = 0;
            negativeAckCountPerRound[roundIndex] = 0;
        }
    }

    /**
     * Processes the proposal set in a received currentMessage.
     * Compares the proposal set in the currentMessage with the current proposal set in the lattice.
     * If the currentMessage's proposal set is not a subset of the current proposal set, sends a NACK and updates the set.
     * Otherwise, sends an ACK.
     *
     * @param currentMessage The received currentMessage containing proposals.
     */
    private void processMessageProposalSet(Message currentMessage) {
        // Iterate through each proposal in the received currentMessage
        for (int proposal : currentMessage.getProposals()) {
            // Check if the current proposal is not in the current proposal set
            if (!latticeForwarder.getProposal(currentMessage.getLatticeRound()).contains(proposal)) {
                // If any proposal is not present in the current set, then update the current proposal set
                latticeForwarder.getProposal(currentMessage.getLatticeRound()).addAll(currentMessage.getProposals());

                // Send a NACK (Negative Acknowledgement) indicating that the proposal set has been updated
                send(new Message(currentMessage.getId(), currentNodeId, currentMessage.getSenderId(), currentMessage.getLatticeRound(),
                                latticeForwarder.getProposal(currentMessage.getLatticeRound()), (byte)2),
                        hostMapByIdentifier.get(currentMessage.getSenderId()));

                // Exit the method immediately as we have processed the necessary action for this scenario
                return;
            }
        }

        // If this point is reached, it means all proposals in the currentMessage are already in the current set
        // Send an ACK (Acknowledgement) indicating that the proposal set was already up-to-date
        send(new Message(currentMessage.getId(), currentNodeId, currentMessage.getSenderId(), currentMessage.getLatticeRound(),
                        latticeForwarder.getProposal(currentMessage.getLatticeRound()), (byte)1),
                hostMapByIdentifier.get(currentMessage.getSenderId()));
    }

    /**
     * Handles the forwarding of messages based on their type and lattice round.
     * This method is responsible for processing different types of messages in the context of lattice-based rounds.
     *
     * @param message The message to be processed and forwarded.
     */
    @Override
    public void forward(Message message) {
        // Check if the message received is marked as a delivered message.
        // 'isDeliveredMessage' typically checks a flag in the message indicating it has been successfully delivered.
        if (message.isDeliveredMessage()) {
            // If the message is a delivered message, handle it using the 'handleDeliveredMessage' method.
            // This method performs specific actions for delivered messages, such as acknowledging receipt.
            handleDeliveredMessage(message);
        }
        else if(latticeForwarder.getMinLatticeRound() <= message.getLatticeRound() && message.getLatticeRound() < latticeForwarder.getMaxLatticeRound()){
            if(message.isAckMessage()){
                // If the received message is an acknowledgment (ACK) message, handle it.
                handleAcknowledgeMessage(message);
            }
            else{
                // Compare the message's proposal set to the current proposal set
                processMessageProposalSet(message);
            }
        }
        else{
            if(message.isAckMessage()) {
                // I am already in the next round
                return;
            }
            // Check if the message's lattice round is greater than or equal to the maximum lattice round known by the lattice forwarder
            if (message.getLatticeRound() >= latticeForwarder.getMaxLatticeRound()) {
                // Message is from a future round; add its proposals for later processing.
                latticeForwarder.getProposal(message.getLatticeRound()).addAll(message.getProposals());

                // Send an ACK to acknowledge receipt of the future round message.
                send(new Message(message.getId(), currentNodeId, message.getSenderId(), message.getLatticeRound(),
                                latticeForwarder.getProposal(message.getLatticeRound()), (byte)1),
                        hostMapByIdentifier.get(message.getSenderId()));
            }

            if(message.getLatticeRound() < latticeForwarder.getMinLatticeRound()){ // It's from a previous round,
                // Compare the message's proposal set to the current proposal set
                processMessageProposalSet(message);
            }
        }
    }
}
