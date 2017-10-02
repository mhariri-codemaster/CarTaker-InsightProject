#!/bin/bash

export CLASSPATH=../kafka/libs/*: ../jackson.jar:.
javac CarTakerProducer.java
java CarTakerProducer
