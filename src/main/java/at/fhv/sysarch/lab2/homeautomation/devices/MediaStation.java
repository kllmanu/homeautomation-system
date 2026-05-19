package at.fhv.sysarch.lab2.homeautomation.devices;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

public class MediaStation extends AbstractBehavior<MediaStation.MediaStationCommand> {

    public interface MediaStationCommand {}

    public record PlayMovie() implements MediaStationCommand {}
    public record StopMovie() implements MediaStationCommand {}

    public static Behavior<MediaStationCommand> create(ActorRef<Blinds.BlindsCommand> blinds) {
        return Behaviors.setup(context -> new MediaStation(context, blinds));
    }

    private final ActorRef<Blinds.BlindsCommand> blinds;
    private boolean moviePlaying = false;

    private MediaStation(ActorContext<MediaStationCommand> context, ActorRef<Blinds.BlindsCommand> blinds) {
        super(context);
        this.blinds = blinds;
        getContext().getLog().info("MediaStation actor started");
    }

    @Override
    public Receive<MediaStationCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(PlayMovie.class, this::onPlayMovie)
                .onMessage(StopMovie.class, this::onStopMovie)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<MediaStationCommand> onPlayMovie(PlayMovie p) {
        if (moviePlaying) {
            getContext().getLog().warn("MediaStation: A movie is already playing!");
        } else {
            getContext().getLog().info("MediaStation: Starting movie");
            moviePlaying = true;
            blinds.tell(new Blinds.MediaStationPlaying(true));
        }
        return this;
    }

    private Behavior<MediaStationCommand> onStopMovie(StopMovie s) {
        if (moviePlaying) {
            getContext().getLog().info("MediaStation: Stopping movie");
            moviePlaying = false;
            blinds.tell(new Blinds.MediaStationPlaying(false));
        } else {
            getContext().getLog().warn("MediaStation: No movie is playing!");
        }
        return this;
    }

    private Behavior<MediaStationCommand> onPostStop() {
        getContext().getLog().info("MediaStation actor stopped");
        return this;
    }
}
