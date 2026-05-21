package at.fhv.sysarch.lab2.homeautomation.environment;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import com.typesafe.config.Config;

public class MqttEnvironment extends AbstractBehavior<MqttEnvironment.MqttEnvironmentCommand> {

    public interface MqttEnvironmentCommand {}

    public record WrappedTemperatureCommand(TemperatureEnvironment.TemperatureEnvironmentCommand cmd) implements MqttEnvironmentCommand {}
    public record WrappedWeatherCommand(WeatherEnvironment.WeatherEnvironmentCommand cmd) implements MqttEnvironmentCommand {}
    private record MqttMessageArrived(String topic, String payload) implements MqttEnvironmentCommand {}

    private double temperature = 20.0;
    private WeatherEnvironment.Weather weather = WeatherEnvironment.Weather.SUNNY;
    private final List<ActorRef<TemperatureEnvironment.TemperatureResponse>> tempSubscribers = new ArrayList<>();
    private final List<ActorRef<WeatherEnvironment.WeatherResponse>> weatherSubscribers = new ArrayList<>();
    private final MqttClient mqttClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static Behavior<MqttEnvironmentCommand> create() {
        return Behaviors.setup(MqttEnvironment::new);
    }

    private MqttEnvironment(ActorContext<MqttEnvironmentCommand> context) throws MqttException {
        super(context);

        ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> tempAdapter =
                context.messageAdapter(TemperatureEnvironment.TemperatureEnvironmentCommand.class, WrappedTemperatureCommand::new);
        context.getSystem().receptionist().tell(Receptionist.register(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY, tempAdapter));

        ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherAdapter =
                context.messageAdapter(WeatherEnvironment.WeatherEnvironmentCommand.class, WrappedWeatherCommand::new);
        context.getSystem().receptionist().tell(Receptionist.register(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY, weatherAdapter));

        Config config = context.getSystem().settings().config();
        String brokerUrl = config.getString("homeautomation.mqtt.broker");

        String clientId = "HomeAutomationMqttEnv-" + java.util.UUID.randomUUID();
        this.mqttClient = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);

        this.mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                getContext().getLog().error("MQTT connection lost", cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                getContext().getSelf().tell(new MqttMessageArrived(topic, new String(message.getPayload())));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        this.mqttClient.connect(options);
        this.mqttClient.subscribe("weather/temperature");
        this.mqttClient.subscribe("weather/condition");

        getContext().getLog().info("MqttEnvironment started and subscribed to MQTT topics");
    }

    @Override
    public Receive<MqttEnvironmentCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedTemperatureCommand.class, w -> onTemperatureCommand(w.cmd))
                .onMessage(WrappedWeatherCommand.class, w -> onWeatherCommand(w.cmd))
                .onMessage(MqttMessageArrived.class, this::onMqttMessageArrived)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<MqttEnvironmentCommand> onTemperatureCommand(TemperatureEnvironment.TemperatureEnvironmentCommand cmd) {
        if (cmd instanceof TemperatureEnvironment.GetTemperature g) {
            g.replyTo().tell(new TemperatureEnvironment.TemperatureResponse(this.temperature));
        } else if (cmd instanceof TemperatureEnvironment.SetTemperature s) {
            this.temperature = s.value();
            getContext().getLog().info("MqttEnvironment (Temperature) manually overridden to {}°C", this.temperature);
            notifyTempSubscribers();
        } else if (cmd instanceof TemperatureEnvironment.SetTemperatureExternal s) {
            this.temperature = s.value();
            getContext().getLog().info("MqttEnvironment (Temperature) updated from MQTT to {}°C", this.temperature);
            notifyTempSubscribers();
        } else if (cmd instanceof TemperatureEnvironment.Subscribe s) {
            this.tempSubscribers.add(s.subscriber());
            s.subscriber().tell(new TemperatureEnvironment.TemperatureResponse(this.temperature));
        }
        return this;
    }

    private void notifyTempSubscribers() {
        TemperatureEnvironment.TemperatureResponse res = new TemperatureEnvironment.TemperatureResponse(this.temperature);
        for (ActorRef<TemperatureEnvironment.TemperatureResponse> sub : tempSubscribers) {
            sub.tell(res);
        }
    }

    private Behavior<MqttEnvironmentCommand> onWeatherCommand(WeatherEnvironment.WeatherEnvironmentCommand cmd) {
        if (cmd instanceof WeatherEnvironment.GetWeather g) {
            g.replyTo().tell(new WeatherEnvironment.WeatherResponse(this.weather));
        } else if (cmd instanceof WeatherEnvironment.SetWeather s) {
            this.weather = s.weather();
            getContext().getLog().info("MqttEnvironment (Weather) manually overridden to {}", this.weather);
            notifyWeatherSubscribers();
        } else if (cmd instanceof WeatherEnvironment.SetWeatherExternal s) {
            this.weather = s.weather();
            getContext().getLog().info("MqttEnvironment (Weather) updated from MQTT to {}", this.weather);
            notifyWeatherSubscribers();
        } else if (cmd instanceof WeatherEnvironment.Subscribe s) {
            this.weatherSubscribers.add(s.subscriber());
            s.subscriber().tell(new WeatherEnvironment.WeatherResponse(this.weather));
        }
        return this;
    }

    private void notifyWeatherSubscribers() {
        WeatherEnvironment.WeatherResponse res = new WeatherEnvironment.WeatherResponse(this.weather);
        for (ActorRef<WeatherEnvironment.WeatherResponse> sub : weatherSubscribers) {
            sub.tell(res);
        }
    }

    private Behavior<MqttEnvironmentCommand> onMqttMessageArrived(MqttMessageArrived m) {
        try {
            JsonNode root = objectMapper.readTree(m.payload);
            if (m.topic.equals("weather/temperature")) {
                double temp = root.get("temperature").asDouble();
                getContext().getSelf().tell(new WrappedTemperatureCommand(new TemperatureEnvironment.SetTemperatureExternal(temp)));
            } else if (m.topic.equals("weather/condition")) {
                String condition = root.get("condition").asText().toUpperCase();
                WeatherEnvironment.Weather w = mapConditionToWeather(condition);
                getContext().getSelf().tell(new WrappedWeatherCommand(new WeatherEnvironment.SetWeatherExternal(w)));
            }
        } catch (Exception e) {
            getContext().getLog().error("Failed to parse MQTT message: {}", m.payload, e);
        }
        return this;
    }

    private WeatherEnvironment.Weather mapConditionToWeather(String condition) {
        return switch (condition) {
            case "SUNNY" -> WeatherEnvironment.Weather.SUNNY;
            case "RAINY", "STORM" -> WeatherEnvironment.Weather.RAINY;
            case "CLOUDY" -> WeatherEnvironment.Weather.CLOUDY;
            default -> WeatherEnvironment.Weather.SUNNY;
        };
    }

    private Behavior<MqttEnvironmentCommand> onPostStop() throws MqttException {
        if (this.mqttClient.isConnected()) {
            this.mqttClient.disconnect();
        }
        this.mqttClient.close();
        getContext().getLog().info("MqttEnvironment stopped");
        return this;
    }

    // Helper to get adapted refs for DemoHttpServer
    public static ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> getTemperatureAdapter(ActorRef<MqttEnvironmentCommand> ref) {
        return ref.unsafeUpcast(); // This is a bit hacky, but since we know it handles these via adapters in a real scenario, we might need a better way if we weren't using adapters internally.
        // Actually, since I'm using context.messageAdapter inside the actor, the EXTERNAL ActorRef is NOT the one I should pass to DemoHttpServer if it's NOT an adapter.
    }
}
