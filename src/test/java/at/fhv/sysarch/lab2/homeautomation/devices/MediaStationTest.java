package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.model.WeatherCondition;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.LoggingTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.apache.pekko.actor.typed.pubsub.Topic;

public class MediaStationTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    public static void cleanup() {
        testKit.shutdownTestKit();
    }

    @Test
    public void testMediaStationControlLogic() {
        ActorRef<Topic.Command<Blinds.MediaStationPlaying>> mediaTopic = testKit.spawn(Topic.create(Blinds.MediaStationPlaying.class, "test-media-topic"));
        ActorRef<Blinds.BlindsCommand> blinds = testKit.spawn(Blinds.create());
        mediaTopic.tell(Topic.subscribe(blinds.narrow()));
        
        ActorRef<MediaStation.MediaStationCommand> mediaStation = testKit.spawn(MediaStation.create(mediaTopic));

        // 1. Play movie -> Blinds should close
        LoggingTestKit.info("Blinds: Closing")
                .expect(testKit.system(), () -> {
                    mediaStation.tell(new MediaStation.PlayMovie(testKit.<MediaStation.PlayMovieResponse>createTestProbe().getRef()));
                    return null;
                });

        // 2. Try to play another movie -> Warning, no change to blinds
        LoggingTestKit.warn("MediaStation: A movie is already playing!")
                .expect(testKit.system(), () -> {
                    mediaStation.tell(new MediaStation.PlayMovie(testKit.<MediaStation.PlayMovieResponse>createTestProbe().getRef()));
                    return null;
                });

        // 3. Weather change to CLOUDY while movie playing -> Blinds should stay closed
        LoggingTestKit.info("Blinds: Opening")
                .withOccurrences(0)
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.WeatherChanged(new WeatherCondition(WeatherEnvironment.Weather.CLOUDY, "Condition")));
                    return null;
                });

        // 4. Stop movie -> Blinds should open (since weather is CLOUDY)
        LoggingTestKit.info("Blinds: Opening")
                .expect(testKit.system(), () -> {
                    mediaStation.tell(new MediaStation.StopMovie());
                    return null;
                });

        // 5. Play movie again -> Blinds should close
        LoggingTestKit.info("Blinds: Closing")
                .expect(testKit.system(), () -> {
                    mediaStation.tell(new MediaStation.PlayMovie(testKit.<MediaStation.PlayMovieResponse>createTestProbe().getRef()));
                    return null;
                });

        // 6. Weather change to SUNNY while movie playing -> Blinds stay closed (redundant)
        LoggingTestKit.info("Blinds: Closing")
                .withOccurrences(0)
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.WeatherChanged(new WeatherCondition(WeatherEnvironment.Weather.SUNNY, "Condition")));
                    return null;
                });

        // 7. Stop movie -> Blinds stay closed (since weather is SUNNY)
        LoggingTestKit.info("Blinds: Opening")
                .withOccurrences(0)
                .expect(testKit.system(), () -> {
                    mediaStation.tell(new MediaStation.StopMovie());
                    return null;
                });
    }
}
