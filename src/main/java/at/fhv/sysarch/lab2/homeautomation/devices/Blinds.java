package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
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
    public record MediaStationPlaying(boolean playing) implements BlindsCommand {}

    public static Behavior<BlindsCommand> create() {
        return Behaviors.setup(Blinds::new);
    }

    private boolean blindsClosed = false;
    private boolean moviePlaying = false;
    private WeatherEnvironment.Weather lastWeather = WeatherEnvironment.Weather.UNKNOWN;

    private Blinds(ActorContext<BlindsCommand> context) {
        super(context);
        getContext().getLog().info("Blinds actor started");
    }

    @Override
    public Receive<BlindsCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(WeatherChanged.class, this::onWeatherChanged)
                .onMessage(MediaStationPlaying.class, this::onMediaStationPlaying)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<BlindsCommand> onWeatherChanged(WeatherChanged r) {
        this.lastWeather = r.weatherCondition().value();
        getContext().getLog().info("Blinds received weather update: {} ({})", 
                r.weatherCondition().value(), r.weatherCondition().unit());

        updateBlindsState();
        return this;
    }

    private Behavior<BlindsCommand> onMediaStationPlaying(MediaStationPlaying m) {
        this.moviePlaying = m.playing();
        getContext().getLog().info("Blinds received MediaStation update: playing={}", m.playing());
        
        updateBlindsState();
        return this;
    }

    private void updateBlindsState() {
        boolean shouldBeClosed = moviePlaying || lastWeather == WeatherEnvironment.Weather.SUNNY;

        if (shouldBeClosed && !blindsClosed) {
            getContext().getLog().info("Blinds: Closing");
            blindsClosed = true;
        } else if (!shouldBeClosed && blindsClosed) {
            getContext().getLog().info("Blinds: Opening");
            blindsClosed = false;
        }
    }

    private Behavior<BlindsCommand> onPostStop() {
        getContext().getLog().info("Blinds actor stopped");
        return this;
    }
}
