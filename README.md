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
