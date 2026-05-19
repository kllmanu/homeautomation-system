package at.fhv.sysarch.lab2.homeautomation.external;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttReconnaissance {
    private static final Logger logger = LoggerFactory.getLogger(MqttReconnaissance.class);
    private static final String BROKER_URL = "tcp://10.0.40.161:1883";
    private static final String CLIENT_ID = "GeminiReconClient";

    public static void main(String[] args) {
        try {
            MqttClient client = new MqttClient(BROKER_URL, CLIENT_ID, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    logger.error("Connection lost: {}", cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    logger.info("Received message on topic: {} - Content: {}", topic, new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Not used for subscriber
                }
            });

            logger.info("Connecting to broker: {}", BROKER_URL);
            client.connect(options);
            logger.info("Connected. Subscribing to all topics...");
            client.subscribe("#");

            // Keep alive
            while (true) {
                Thread.sleep(1000);
            }

        } catch (MqttException | InterruptedException e) {
            logger.error("MQTT Error: {}", e.getMessage());
        }
    }
}
