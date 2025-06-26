#!/bin/bash

# Absolute Chaos Script - Kills all services in the distributed system
# This script kills all the services that the chaos monkey can target

echo "🔥 ABSOLUTE CHAOS MODE ACTIVATED 🔥"
echo "Killing all distributed system services..."
echo ""

# Array of all services that can be killed
services=(
    "moviesreceiver_1"
    "moviesreceiver_2"
    "filter_q1_1"
    "filter_q1_2"
    "filter_q3_1"
    "filter_q3_2"
    "filter_q4_1"
    "filter_q4_2"
    "ratingsreceiver_1"
    "ratingsreceiver_2"
    "ratingsjoiner_1"
    "ratingsjoiner_2"
    "q1_sink_1"
    "q3_sink_1"
    "budget_reducer_1"
    "budget_reducer_2"
    "budget_sink_1"
    "credits_joiner_1"
    "credits_joiner_2"
    "credits_receiver_1"
    "credits_receiver_2"
    "credits_sink_1"
    "resuscitator_2"
    "resuscitator_3"
    "sentiment_worker_1"
    "sentiment_worker_2"
    "sentiment_sink_1"
    "sentiment_reducer_1"
)

# Counter for killed services
killed_count=0
failed_count=0

echo "Targeting ${#services[@]} services for termination..."
echo ""

# Kill each service
for service in "${services[@]}"; do
    echo -n "Killing $service... "
    
    # Use docker compose kill to terminate the service
    if docker compose kill "$service" > /dev/null 2>&1; then
        echo "✅ KILLED"
        ((killed_count++))
    else
        echo "❌ FAILED (service may not be running)"
        ((failed_count++))
    fi
done

echo ""
echo "🎯 CHAOS SUMMARY:"
echo "   Services killed: $killed_count"
echo "   Failed attempts: $failed_count"
echo "   Total targeted: ${#services[@]}"
echo ""
echo "💀 All services have been terminated. The system is now in absolute chaos! 💀"
