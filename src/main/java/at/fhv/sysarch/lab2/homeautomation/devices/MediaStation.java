package at.fhv.sysarch.lab2.homeautomation.devices;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

import org.apache.pekko.actor.typed.pubsub.Topic;

public class MediaStation extends AbstractBehavior<MediaStation.MediaStationCommand> {

    public static final ServiceKey<MediaStationCommand> SERVICE_KEY = ServiceKey.create(MediaStationCommand.class, "MediaStation");

    public interface MediaStationCommand {}

    public record PlayMovie(ActorRef<PlayMovieResponse> replyTo) implements MediaStationCommand {}
    public record StopMovie() implements MediaStationCommand {}
    public record Subscribe(ActorRef<MediaStationStateChanged> subscriber) implements MediaStationCommand {}
    public record MediaStationStateChanged(boolean playing) {}
    public record ReadState(ActorRef<MediaStationStateChanged> replyTo) implements MediaStationCommand {}

    public record PlayMovieResponse(boolean success, String message) {}

    public static Behavior<MediaStationCommand> create(ActorRef<Topic.Command<Blinds.MediaStationPlaying>> mediaTopic) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, context.getSelf()));
            return new MediaStation(context, mediaTopic);
        });
    }

    private final ActorRef<Topic.Command<Blinds.MediaStationPlaying>> mediaTopic;
    private boolean moviePlaying = false;
    private ActorRef<MediaStationStateChanged> subscriber;

    private MediaStation(ActorContext<MediaStationCommand> context, ActorRef<Topic.Command<Blinds.MediaStationPlaying>> mediaTopic) {
        super(context);
        this.mediaTopic = mediaTopic;
        getContext().getLog().info("MediaStation actor started");
    }

    @Override
    public Receive<MediaStationCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(PlayMovie.class, this::onPlayMovie)
                .onMessage(StopMovie.class, this::onStopMovie)
                .onMessage(Subscribe.class, this::onSubscribe)
                .onMessage(ReadState.class, this::onReadState)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<MediaStationCommand> onReadState(ReadState r) {
        r.replyTo().tell(new MediaStationStateChanged(this.moviePlaying));
        return this;
    }

    private Behavior<MediaStationCommand> onSubscribe(Subscribe s) {
        this.subscriber = s.subscriber();
        this.subscriber.tell(new MediaStationStateChanged(moviePlaying));
        return this;
    }

    private void notifySubscriber() {
        if (this.subscriber != null) {
            this.subscriber.tell(new MediaStationStateChanged(moviePlaying));
        }
    }

    private Behavior<MediaStationCommand> onPlayMovie(PlayMovie p) {
        if (moviePlaying) {
            getContext().getLog().warn("MediaStation: A movie is already playing!");
            p.replyTo().tell(new PlayMovieResponse(false, "A movie is already playing!"));
        } else {
            getContext().getLog().info("MediaStation: Starting movie");
            moviePlaying = true;
            this.mediaTopic.tell(Topic.publish(new Blinds.MediaStationPlaying(true)));
            notifySubscriber();
            p.replyTo().tell(new PlayMovieResponse(true, "Movie started"));
        }
        return this;
    }

    private Behavior<MediaStationCommand> onStopMovie(StopMovie s) {
        if (moviePlaying) {
            getContext().getLog().info("MediaStation: Stopping movie");
            moviePlaying = false;
            this.mediaTopic.tell(Topic.publish(new Blinds.MediaStationPlaying(false)));
            notifySubscriber();
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
