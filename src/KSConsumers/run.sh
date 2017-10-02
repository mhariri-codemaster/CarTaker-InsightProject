#!/bin/bash

export CLASSPATH=../Cassandra-java-driver/*: ../kafka/libs/*: ../jackson.jar:.

javac StreamSensorConsumer.java
java StreamSensorConsumer

javac StreamBookingConsumer.java
java StreamBookingConsumer
