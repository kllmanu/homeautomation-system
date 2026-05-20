package at.fhv.sysarch.lab2.homeautomation.room;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

public class Space extends AbstractBehavior<Space.SpaceCommand> {

    public interface SpaceCommand {}

    public static Behavior<SpaceCommand> create(String spaceName) {
        return Behaviors.setup(context -> new Space(context, spaceName));
    }

    private final String spaceName;

    private Space(ActorContext<SpaceCommand> context, String spaceName) {
        super(context);
        this.spaceName = spaceName;
        getContext().getLog().info("Space {} started", spaceName);

        // For now, just spawn one room
        getContext().spawn(Room.create("LivingRoom"), "livingRoom");
    }

    @Override
    public Receive<SpaceCommand> createReceive() {
        return newReceiveBuilder().build();
    }
}
