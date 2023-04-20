#!/bin/bash

git pull
git submodule update
cd neuron-search-tools
./mvnw clean package
cd ..
