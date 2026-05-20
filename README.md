# Lab2

## Starten

- `./gradlew run` startet das System mit der internen Simulation der Umgebung
- `./gradlew run --args="--external"` startet das System mit den externen Werten die per MQTT zur Verfügung gestellt werden
- `./gradlew run --args="--manual"` startet das System ohne Simulation, mit manueller Kontrolle, die Werte können
  dann über das Web UI gesendet werden

Zusätzlich dazu den `GroceryStore` (externes Bestellungssystem) starten mit:

```
./gradlew runGroceryStore
```

## Web UI

http://localhost:8084

## MQTT

Mit [mqttcli](https://packages.fedoraproject.org/pkgs/mqttcli/mqttcli/) schauen wir erstmal, was da überhaupt von der MQTT daher kommt:

```
sub -broker tcp://10.0.40.161:1883 -topic "#"
```

## Interaktionsmuster

Für detailliertere Informationen siehe die offizielle [Pekko Dokumentation zu Interaktionsmustern](https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html).

Folgende Interaktionsmuster wurden verwendet:

### 1. [Tell (Fire-and-Forget)](https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html#fire-and-forget)
Wird für einfache Befehle verwendet, bei denen keine unmittelbare Bestätigung durch den Absender erforderlich ist.
- **Beispiel**: Die UI weist die Klimaanlage (`AirCondition`) an, sich ein- oder auszuschalten.
- **Beispiel**: Der Kühlschrank (`Fridge`) verarbeitet den Verbrauch eines Produkts.

### 2. [Ask (Request-Response)](https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html#request-response)
Wird verwendet, wenn der Absender eine Bestätigung oder Daten vom Actor benötigt.
- **Beispiel**: `DemoHttpServer` fragt bei der `MediaStation` an, einen Film abzuspielen. Die UI wartet auf eine `PlayMovieResponse`, um Erfolgs- oder Fehlermeldungen anzuzeigen.
- **Beispiel**: `DemoHttpServer` fragt beim Kühlschrank (`Fridge`) die Bestellung von Produkten an, wobei Kapazitätsgrenzen validiert werden.

### 3. [Publish/Subscribe (Distributed Pub-Sub)](https://pekko.apache.org/docs/pekko/current/typed/actor-discovery.html#receptionist)
Wird für entkoppelte Kommunikation verwendet, bei der mehrere Actoren auf dasselbe Ereignis reagieren müssen. (In diesem Projekt realisiert über Pekko Topics).
- **Beispiel**: Der Wettersensor (`WeatherSensor`) veröffentlicht `WeatherChanged` auf einem `weather-topic`. Alle Jalousien-Actoren (`Blinds`) im System abonnieren dieses Topic, um ihren Zustand automatisch anzupassen.
- **Beispiel**: Die `MediaStation` veröffentlicht `MediaStationPlaying` auf einem globalen `media-topic`, um sicherzustellen, dass sich alle Jalousien während eines Films schließen.

### 4. [Per-Session Child Actor](https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html#per-session-child-actor)
Wird verwendet, um langlaufende oder externe Aufgaben an einen dedizierten, kurzlebigen Actor zu delegieren.
- **Beispiel**: Wenn der Kühlschrank (`Fridge`) einen `OrderProduct`-Befehl erhält, erzeugt er einen einmaligen `OrderProcessor`-Child-Actor. Dieser übernimmt die gRPC-Kommunikation mit dem Grocery Store und beendet sich selbst, nachdem das Ergebnis an den Parent-Actor gesendet wurde.
