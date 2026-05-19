package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.model.WeatherCondition;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

public class Blinds extends AbstractBehavior<Blinds.BlindsCommand> {

    public interface BlindsCommand {}

    public record WeatherChanged(WeatherCondition weatherCondition) implements BlindsCommand {}

    public static Behavior<BlindsCommand> create() {
        return Behaviors.setup(Blinds::new);
    }

    private Blinds(ActorContext<BlindsCommand> context) {
        super(context);
        getContext().getLog().info("Blinds actor started");
    }

    @Override
    public Receive<BlindsCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WeatherChanged.class, this::onWeatherChanged)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<BlindsCommand> onWeatherChanged(WeatherChanged r) {
        getContext().getLog().info("Blinds received weather update: {} ({})", 
                r.weatherCondition().value(), r.weatherCondition().unit());
        // Simple logic: Close blinds if it's SUNNY (to keep cool) or RAINY (protection)
        switch (r.weatherCondition().value()) {
            case SUNNY, RAINY -> getContext().getLog().info("Blinds: Closing");
            case CLOUDY -> getContext().getLog().info("Blinds: Opening");
        }
        return this;
    }

    private Behavior<BlindsCommand> onPostStop() {
        getContext().getLog().info("Blinds actor stopped");
        return this;
    }
}
