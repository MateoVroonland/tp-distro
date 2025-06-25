#!/bin/bash
set -e

echo 'Building Docker images in parallel...'

docker build -t movies/requesthandler:latest -f cmd/requesthandler/Dockerfile . &
docker build -t movies/client:latest -f cmd/client/Dockerfile . &
docker build -t movies/moviesreceiver:latest -f cmd/moviesreceiver/Dockerfile . &
docker build -t movies/filter:latest -f cmd/filter/Dockerfile . &
docker build -t movies/ratingsreceiver:latest -f cmd/ratingsreceiver/Dockerfile . &
docker build -t movies/ratingsjoiner:latest -f cmd/joiners/Dockerfile . &
docker build -t movies/sink:latest -f cmd/sinks/Dockerfile . &
docker build -t movies/sink_q3:latest -f cmd/sinks_q3/Dockerfile . &
docker build -t movies/reducer:latest -f cmd/reducers/Dockerfile . &
docker build -t movies/sink_q2:latest -f cmd/sinks_q2/Dockerfile . &
docker build -t movies/sentiment:latest -f cmd/sentiment/Dockerfile . &
docker build -t movies/sentiment_reducer:latest -f cmd/reducers/sentiment_reducer/Dockerfile . &
docker build -t movies/credits_joiner:latest -f cmd/joiners_q4/Dockerfile . &
docker build -t movies/credits_receiver:latest -f cmd/credits_receiver/Dockerfile . &
docker build -t movies/sink_q4:latest -f cmd/sinks_q4/Dockerfile . &
docker build -t movies/sink_q5:latest -f cmd/sinks_q5/Dockerfile . &
docker build -t movies/resuscitator:latest -f cmd/resuscitator/Dockerfile . &
docker build -t movies/chaosmonkey:latest -f cmd/chaosmonkey/Dockerfile . &
wait

echo 'All images built successfully.'

