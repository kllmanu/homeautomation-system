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
            getContext().spawn(MqttEnvironment.create(), "mqttEnvironment");
        } else if (manual) {
            getContext().getLog().info("EnvironmentManager: Starting in mode MANUAL");
            getContext().spawn(ManualEnvironment.create(), "manualEnvironment");
        } else {
            getContext().getLog().info("EnvironmentManager: Starting in mode INTERNAL");
            getContext().spawn(TemperatureEnvironment.create(16.0), "temperatureEnvironment");
            getContext().spawn(WeatherEnvironment.create(WeatherEnvironment.Weather.SUNNY), "weatherEnvironment");
        }
    }

    @Override
    public Receive<EnvironmentManagerCommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
