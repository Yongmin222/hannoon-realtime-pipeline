#!/bin/bash
./gradlew shadowJar
java -Dfile.encoding=UTF-8 -jar build/libs/franchise-top-store.jar
