package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.environment.WeatherEnvironment;
import at.fhv.sysarch.lab2.homeautomation.model.WeatherCondition;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.LoggingTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class BlindsTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    public static void cleanup() {
        testKit.shutdownTestKit();
    }

    @Test
    public void testBlindsControlLogic() {
        ActorRef<Blinds.BlindsCommand> blinds = testKit.spawn(Blinds.create());

        // Test SUNNY -> Closing
        LoggingTestKit.info("Blinds: Closing")
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.WeatherChanged(new WeatherCondition(WeatherEnvironment.Weather.SUNNY, "Condition")));
                    return null;
                });

        // Test CLOUDY -> Opening
        LoggingTestKit.info("Blinds: Opening")
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.WeatherChanged(new WeatherCondition(WeatherEnvironment.Weather.CLOUDY, "Condition")));
                    return null;
                });

        // Test RAINY -> Opening (since not sunny)
        // First close them again
        LoggingTestKit.info("Blinds: Closing")
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.WeatherChanged(new WeatherCondition(WeatherEnvironment.Weather.SUNNY, "Condition")));
                    return null;
                });

        LoggingTestKit.info("Blinds: Opening")
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.WeatherChanged(new WeatherCondition(WeatherEnvironment.Weather.RAINY, "Condition")));
                    return null;
                });
    }

    @Test
    public void testBlindsMovieLogic() {
        ActorRef<Blinds.BlindsCommand> blinds = testKit.spawn(Blinds.create());

        // Test Movie Playing -> Closing
        LoggingTestKit.info("Blinds: Closing")
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.MediaStationPlaying(true));
                    return null;
                });

        // Test Movie Stopped -> Opening (if weather not sunny)
        LoggingTestKit.info("Blinds: Opening")
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.MediaStationPlaying(false));
                    return null;
                });
        
        // Test Movie Playing while SUNNY -> Stays Closed
        blinds.tell(new Blinds.WeatherChanged(new WeatherCondition(WeatherEnvironment.Weather.SUNNY, "Condition")));
        LoggingTestKit.info("Blinds: Closing")
                .withOccurrences(0)
                .expect(testKit.system(), () -> {
                    blinds.tell(new Blinds.MediaStationPlaying(true));
                    return null;
                });
    }
}
