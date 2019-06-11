package assignment4;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.TimeUnit;

public class CustomReceiverDoctect extends Receiver<String> {

    String host = null;
    int port = -1;

    public CustomReceiverDoctect(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread() {
            @Override
            public void run() {
                receive();
            }
        }.start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    /**
     * Create a socket connection and receive data until receiver is stopped
     */
    private void receive() {

        DatagramSocket socket = null;
        String userInput = null;
        String[] lines = null;
        String[] tmp = null;
        String output = null;
        String unixTime = null;
        Long millis = null;
        byte[] receiveData = null;

        try {
            // connect to the server
            socket = new DatagramSocket(port);

            receiveData = new byte[1024];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);


            // Until stopped or connection broken continue reading
            while (!isStopped()) {
                socket.receive(receivePacket);
                userInput = new String(receivePacket.getData(), 0, receivePacket.getLength());

                // split 5 csv lines
                lines = userInput.split("\\r?\\n");

                for (int i = 0; i < lines.length; i++) {
                    tmp = lines[i].split(",");
                    millis = Long.parseLong(tmp[3]);
                    // convert unixTime into minutes
                    unixTime = String.format("%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
                            TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)));
                    output = unixTime + "," + tmp[15];
                    store(output);
                }
            }
            socket.close();

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch (ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch (Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }
}
