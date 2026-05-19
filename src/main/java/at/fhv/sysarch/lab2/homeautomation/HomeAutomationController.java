package at.fhv.sysarch.lab2.homeautomation;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.devices.TemperatureSensor;
import at.fhv.sysarch.lab2.homeautomation.devices.WeatherSensor;
import at.fhv.sysarch.lab2.homeautomation.environment.ManualEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.MqttEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.TemperatureEnvironment;
import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.uihandler.DemoHttpServer;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class HomeAutomationController extends AbstractBehavior<Void> {

    private final String[] args;

    public static Behavior<Void> create(String[] args) {
        return Behaviors.setup(context -> new HomeAutomationController(context, args));
    }

    private  HomeAutomationController(ActorContext<Void> context, String[] args) {
        super(context);
        this.args = args;

        boolean external = false;
        boolean manual = false;

        for (String arg : args) {
            getContext().getLog().info("Received argument: [{}]", arg);
            if (arg.trim().equals("--external")) {
                external = true;
            } else if (arg.trim().equals("--manual")) {
                manual = true;
            }
        }

        ActorRef<AirCondition.AirConditionCommand> airCondition = getContext().spawn(AirCondition.create(UUID.randomUUID().toString()), "airCondition");
        getContext().spawn(TemperatureSensor.create(airCondition), "temperatureSensor");
        
        ActorRef<Blinds.BlindsCommand> blinds = getContext().spawn(Blinds.create(), "blinds");
        getContext().spawn(WeatherSensor.create(blinds), "weatherSensor");
        ActorRef<MediaStation.MediaStationCommand> mediaStation = getContext().spawn(MediaStation.create(blinds), "mediaStation");

        ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> tempEnv;
        ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherEnv;

        if (external) {
            getContext().getLog().info("HomeAutomation Application starting in mode: EXTERNAL");
            ActorRef<MqttEnvironment.MqttEnvironmentCommand> mqttEnv = getContext().spawn(MqttEnvironment.create(), "mqttEnvironment");
            
            tempEnv = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                mqttEnv.tell(new MqttEnvironment.WrappedTemperatureCommand(cmd));
                return Behaviors.same();
            }), "tempProxy");

            weatherEnv = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                mqttEnv.tell(new MqttEnvironment.WrappedWeatherCommand(cmd));
                return Behaviors.same();
            }), "weatherProxy");

        } else if (manual) {
            getContext().getLog().info("HomeAutomation Application starting in mode: MANUAL");
            ActorRef<ManualEnvironment.ManualEnvironmentCommand> manualEnv = getContext().spawn(ManualEnvironment.create(), "manualEnvironment");

            tempEnv = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                manualEnv.tell(new ManualEnvironment.WrappedTemperatureCommand(cmd));
                return Behaviors.same();
            }), "tempProxy");

            weatherEnv = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                manualEnv.tell(new ManualEnvironment.WrappedWeatherCommand(cmd));
                return Behaviors.same();
            }), "weatherProxy");

        } else {
            getContext().getLog().info("HomeAutomation Application starting in mode: INTERNAL");
            tempEnv = getContext().spawn(TemperatureEnvironment.create(20.0), "temperatureEnvironment");
            weatherEnv = getContext().spawn(WeatherEnvironment.create(WeatherEnvironment.Weather.SUNNY), "weatherEnvironment");
        }

        final Http http = Http.get(context.getSystem());
        String modeString = external ? "EXTERNAL" : (manual ? "MANUAL" : "INTERNAL");
        DemoHttpServer app = new DemoHttpServer(tempEnv, weatherEnv, airCondition, mediaStation, blinds, modeString);
        final CompletionStage<ServerBinding> binding = http.newServerAt("localhost", 8084).bind(app.createRoute());

        getContext().getLog().info("PRESS RETURN TO EXIT");

        try {
            System.in.read(); 
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        binding
                .thenCompose(ServerBinding::unbind) 
                .thenAccept(unbound -> getContext().getSystem().terminate()); 
    }

    @Override
    public Receive<Void> createReceive() {
        return newReceiveBuilder().onSignal(PostStop.class, signal -> onPostStop()).build();
    }

    private HomeAutomationController onPostStop() {
        getContext().getLog().info("HomeAutomation Application stopped");
        return this;
    }
}
