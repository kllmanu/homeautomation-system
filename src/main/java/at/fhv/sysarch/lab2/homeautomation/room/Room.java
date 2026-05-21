package at.fhv.sysarch.lab2.homeautomation.room;

import at.fhv.sysarch.lab2.homeautomation.devices.AirCondition;
import at.fhv.sysarch.lab2.homeautomation.devices.Blinds;
import at.fhv.sysarch.lab2.homeautomation.devices.MediaStation;
import at.fhv.sysarch.lab2.homeautomation.devices.TemperatureSensor;
import at.fhv.sysarch.lab2.homeautomation.devices.WeatherSensor;
import at.fhv.sysarch.lab2.homeautomation.fridge.Fridge;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.pubsub.Topic;

import java.util.UUID;

public class Room extends AbstractBehavior<Room.RoomCommand> {

    public interface RoomCommand {}

    public static Behavior<RoomCommand> create(String roomName) {
        return Behaviors.setup(context -> new Room(context, roomName));
    }

    private final String roomName;

    private Room(ActorContext<RoomCommand> context, String roomName) {
        super(context);
        this.roomName = roomName;
        getContext().getLog().info("Room {} started", roomName);

        if (roomName.equalsIgnoreCase("Kitchen")) {

            getContext().spawn(Fridge.create(20, 20.0), "fridge");
            
        } else if (roomName.equalsIgnoreCase("LivingRoom")) {
            ActorRef<Topic.Command<AirCondition.EnrichedTemperature>> tempTopic = 
                    getContext().spawn(Topic.create(AirCondition.EnrichedTemperature.class, "temperature-topic-" + roomName), "temperature-topic");
            
            ActorRef<Topic.Command<Blinds.WeatherChanged>> weatherTopic = 
                    getContext().spawn(Topic.create(Blinds.WeatherChanged.class, "weather-topic-" + roomName), "weather-topic");
            
            ActorRef<Topic.Command<Blinds.MediaStationPlaying>> mediaTopic = 
                    getContext().spawn(Topic.create(Blinds.MediaStationPlaying.class, "media-topic"), "media-topic");

            //actuators
            ActorRef<AirCondition.AirConditionCommand> airCondition = getContext().spawn(AirCondition.create(UUID.randomUUID().toString()), "airCondition");
            tempTopic.tell(Topic.subscribe(airCondition.narrow())); 
            
            ActorRef<Blinds.BlindsCommand> blinds = getContext().spawn(Blinds.create(), "blinds");
            weatherTopic.tell(Topic.subscribe(blinds.narrow())); 
            mediaTopic.tell(Topic.subscribe(blinds.narrow())); 

            ActorRef<MediaStation.MediaStationCommand> mediaStation = getContext().spawn(MediaStation.create(mediaTopic), "mediaStation");

            //sensors
            getContext().spawn(TemperatureSensor.create(tempTopic), "temperatureSensor");
            getContext().spawn(WeatherSensor.create(weatherTopic), "weatherSensor");
        }
    }

    @Override
    public Receive<RoomCommand> createReceive() {
        return newReceiveBuilder().build();
    }
}