import yaml, sys, os
from dotenv import load_dotenv

def get_env(key):
    value = os.getenv(key)
    if value is None:
        raise ValueError(f"Required environment variable {key} is not set")
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Environment variable {key} must be an integer")

def generate_compose():
    load_dotenv()

    movies_receiver_amount = get_env("MOVIES_RECEIVER_AMOUNT")
    credits_receiver_amount = get_env("CREDITS_RECEIVER_AMOUNT")
    ratings_receiver_amount = get_env("RATINGS_RECEIVER_AMOUNT")
    q1_filter_amount = get_env("Q1_FILTER_AMOUNT")
    q3_filter_amount = get_env("Q3_FILTER_AMOUNT")
    q4_filter_amount = get_env("Q4_FILTER_AMOUNT")
    credits_joiner_amount = get_env("CREDITS_JOINER_AMOUNT")
    ratings_joiner_amount = get_env("RATINGS_JOINER_AMOUNT")
    budget_reducer_amount = get_env("BUDGET_REDUCER_AMOUNT")
    sentiment_reducer_amount = get_env("SENTIMENT_REDUCER_AMOUNT")
    budget_sink_amount = get_env("BUDGET_SINK_AMOUNT")
    q1_sink_amount = get_env("Q1_SINK_AMOUNT")
    credits_sink_amount = get_env("CREDITS_SINK_AMOUNT")
    sentiment_sink_amount = get_env("SENTIMENT_SINK_AMOUNT")
    sentiment_worker_amount = get_env("SENTIMENT_WORKER_AMOUNT")
    sentiment_sink_amount = get_env("SENTIMENT_SINK_AMOUNT")
    clients_amount = get_env("CLIENTS_AMOUNT")
    resuscitator_amount = get_env("RESUSCITATOR_AMOUNT")

    compose = {
        "name": "movies-analysis",
        "services": {
            "rabbitmq": {
                "image": "rabbitmq:4-management",
                "ports": [
                    "5672:5672",
                    "15672:15672"
                ],
                "volumes": [
                    "./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro",
                    "./definitions.json:/etc/rabbitmq/definitions.json:ro"
                ],
                "healthcheck": {
                    "test": ["CMD", "rabbitmqctl", "status"],
                    "interval": "10s",
                    "timeout": "5s",
                    "retries": 5
                }
            },
            "requesthandler": {
                "environment": {
                    "ID": 1,
                    "REPLICAS": 1,
                },
                "image": "movies/requesthandler:latest",
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "volumes": [
                    "./docs:/docs",
                    "requesthandler_volume:/data"
                ],
                "command": "/requesthandler/request_handler.go"
            },
            "chaosmonkey": {
                "image": "movies/chaosmonkey:latest",
                "environment": {
                    "ID": 1,
                    "MIN_INTERVAL_SECONDS": 5,
                    "MAX_INTERVAL_SECONDS": 8,
                    "KILL_PROBABILITY": 0.4
                },
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "volumes": [
                    "/var/run/docker.sock:/var/run/docker.sock",
                    "./docker-compose.yml:/app/docker-compose.yml"
                ],
                "privileged": True
            }
        },
        "volumes": {
            "rabbitmq_volume": {},
            "requesthandler_volume": {}
        }
    }

    def add_services(prefix, amount, image, extra_env=None, command=None):
        for i in range(1, amount + 1):
            env = {"ID": i, "REPLICAS": amount}
            if extra_env:
                env.update(extra_env)
            service_name = f"{prefix}_{i}"
            compose["services"][service_name] = {
                "container_name": service_name,
                "image": image,
                "environment": env,
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "volumes": [
                    f"{service_name}_volume:/data"
                ]
            }
            if command:
                compose["services"][service_name]["command"] = command
            
            # Add volume definition if it doesn't exist
            volume_name = f"{service_name}_volume"
            if volume_name not in compose["volumes"]:
                compose["volumes"][volume_name] = {}


#  resuscitator_1:
#     image: movies/resuscitator:latest
#     environment:
#       ID: 1
#       REPLICAS: 3
#       SERVICE_TYPE: resuscitator_1
#     depends_on:
#       rabbitmq:
#         condition: service_healthy
#     volumes:
#       - /var/run/docker.sock:/var/run/docker.sock
#       - ./docker-compose.yml:/app/docker-compose.yml
    def add_resuscitators(amount):
        for i in range(1, amount + 1):
            env = {"ID": i, "REPLICAS": amount}
            service_name = f"resuscitator_{i}"
            compose["services"][service_name] = {
                "container_name": service_name,
                "image": "movies/resuscitator:latest",
                "environment": env,
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "volumes": [
                    "/var/run/docker.sock:/var/run/docker.sock",
                    "./docker-compose.yml:/app/docker-compose.yml"
                ]
            }
            


    add_services("moviesreceiver", movies_receiver_amount, "movies/moviesreceiver:latest")
    add_services("filter_q1", q1_filter_amount, "movies/filter:latest", {"QUERY": 1}, "/filter/filter.go")
    add_services("filter_q3", q3_filter_amount, "movies/filter:latest", {"QUERY": 3}, "/filter/filter.go")
    add_services("filter_q4", q4_filter_amount, "movies/filter:latest", {"QUERY": 4}, "/filter/filter.go")
    add_services("ratingsreceiver", ratings_receiver_amount, "movies/ratingsreceiver:latest")
    add_services("ratingsjoiner", ratings_joiner_amount, "movies/ratingsjoiner:latest")
    add_services("q1_sink", q1_sink_amount, "movies/sink:latest", None, "/q1_sink/q1_sink.go")
    add_services("q3_sink", sentiment_sink_amount, "movies/sink_q3:latest")
    add_services("budget_reducer", budget_reducer_amount, "movies/reducer:latest", None, "/budget_reducer.go")
    add_services("budget_sink", budget_sink_amount, "movies/sink_q2:latest", None, "/budget_sink/budget_sink.go")
    add_services("sentiment_worker", sentiment_worker_amount, "movies/sentiment:latest", None, "/sentiment/sentiment_worker.go")
    add_services("sentiment_reducer", sentiment_reducer_amount, "movies/sentiment_reducer:latest", None, "/sentiment_reducer/sentiment_reducer.go")
    add_services("sentiment_sink", sentiment_sink_amount, "movies/sink_q5:latest", None, "/sentiment_sink/sentiment_sink.go")
    add_services("credits_joiner", credits_joiner_amount, "movies/credits_joiner:latest")
    add_services("credits_receiver", credits_receiver_amount, "movies/credits_receiver:latest", None, "/credits_receiver/credits_receiver.go")
    add_services("credits_sink", credits_sink_amount, "movies/sink_q4:latest", None, "/credits_sink/credits_sink.go")
    add_resuscitators(resuscitator_amount)
    
    compose["services"]["client"] = {
        "image": "movies/client:latest",
        "depends_on": ["requesthandler"],
        "deploy": {
            "replicas": clients_amount
        },
        "volumes": [
            "./docs:/docs"
        ]
    }

    with open('docker-compose.yml', 'w') as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)

if __name__ == "__main__":
    try:
        generate_compose()
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
