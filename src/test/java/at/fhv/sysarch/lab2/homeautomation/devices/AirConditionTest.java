package at.fhv.sysarch.lab2.homeautomation.devices;

import at.fhv.sysarch.lab2.homeautomation.model.Temperature;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.LoggingTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class AirConditionTest {

    static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    public static void cleanup() {
        testKit.shutdownTestKit();
    }

    @Test
    public void testAirConditionControlLogic() {
        ActorRef<AirCondition.AirConditionCommand> airCondition = testKit.spawn(AirCondition.create("test-ac"));

        // Test temperature > 20 -> ON
        LoggingTestKit.info("Aircondition turned ON")
                .expect(testKit.system(), () -> {
                    airCondition.tell(new AirCondition.EnrichedTemperature(new Temperature(21, "Celsius")));
                    return null;
                });

        // Test temperature < 20 -> OFF
        LoggingTestKit.info("Aircondition turned OFF")
                .expect(testKit.system(), () -> {
                    airCondition.tell(new AirCondition.EnrichedTemperature(new Temperature(19, "Celsius")));
                    return null;
                });
                
        // Test redundant temperature < 20 -> no log
        LoggingTestKit.info("Aircondition turned OFF")
                .withOccurrences(0)
                .expect(testKit.system(), () -> {
                    airCondition.tell(new AirCondition.EnrichedTemperature(new Temperature(18, "Celsius")));
                    return null;
                });

        // Test temperature > 20 -> ON again
        LoggingTestKit.info("Aircondition turned ON")
                .expect(testKit.system(), () -> {
                    airCondition.tell(new AirCondition.EnrichedTemperature(new Temperature(25, "Celsius")));
                    return null;
                });
    }
}
