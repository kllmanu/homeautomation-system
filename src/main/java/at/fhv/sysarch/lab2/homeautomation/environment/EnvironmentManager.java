package at.fhv.sysarch.lab2.homeautomation.environment;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import org.apache.pekko.actor.typed.receptionist.Receptionist;

public class EnvironmentManager extends AbstractBehavior<EnvironmentManager.EnvironmentManagerCommand> {

    public interface EnvironmentManagerCommand {}

    public static Behavior<EnvironmentManagerCommand> create(String[] args) {
        return Behaviors.setup(context -> new EnvironmentManager(context, args));
    }

    private EnvironmentManager(ActorContext<EnvironmentManagerCommand> context, String[] args) {
        super(context);

        boolean external = false;
        boolean manual = false;

        for (String arg : args) {
            if (arg.trim().equals("--external")) {
                external = true;
            } else if (arg.trim().equals("--manual")) {
                manual = true;
            }
        }

        if (external) {
            getContext().getLog().info("EnvironmentManager: Starting in mode EXTERNAL");
            ActorRef<MqttEnvironment.MqttEnvironmentCommand> mqttEnv = getContext().spawn(MqttEnvironment.create(), "mqttEnvironment");
            
            ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> tempProxy = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                mqttEnv.tell(new MqttEnvironment.WrappedTemperatureCommand(cmd));
                return Behaviors.same();
            }), "tempProxy");
            getContext().getSystem().receptionist().tell(Receptionist.register(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY, tempProxy));

            ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherProxy = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                mqttEnv.tell(new MqttEnvironment.WrappedWeatherCommand(cmd));
                return Behaviors.same();
            }), "weatherProxy");
            getContext().getSystem().receptionist().tell(Receptionist.register(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY, weatherProxy));

        } else if (manual) {
            getContext().getLog().info("EnvironmentManager: Starting in mode MANUAL");
            ActorRef<ManualEnvironment.ManualEnvironmentCommand> manualEnv = getContext().spawn(ManualEnvironment.create(), "manualEnvironment");

            ActorRef<TemperatureEnvironment.TemperatureEnvironmentCommand> tempProxy = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                manualEnv.tell(new ManualEnvironment.WrappedTemperatureCommand(cmd));
                return Behaviors.same();
            }), "tempProxy");
            getContext().getSystem().receptionist().tell(Receptionist.register(TemperatureEnvironment.TEMPERATURE_ENVIRONMENT_SERVICE_KEY, tempProxy));

            ActorRef<WeatherEnvironment.WeatherEnvironmentCommand> weatherProxy = getContext().spawn(Behaviors.receiveMessage(cmd -> {
                manualEnv.tell(new ManualEnvironment.WrappedWeatherCommand(cmd));
                return Behaviors.same();
            }), "weatherProxy");
            getContext().getSystem().receptionist().tell(Receptionist.register(WeatherEnvironment.WEATHER_ENVIRONMENT_SERVICE_KEY, weatherProxy));

        } else {
            getContext().getLog().info("EnvironmentManager: Starting in mode INTERNAL");
            getContext().spawn(TemperatureEnvironment.create(20.0), "temperatureEnvironment");
            getContext().spawn(WeatherEnvironment.create(WeatherEnvironment.Weather.SUNNY), "weatherEnvironment");
        }
    }

    @Override
    public Receive<EnvironmentManagerCommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
