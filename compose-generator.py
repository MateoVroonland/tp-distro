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
    # Load environment variables from .env file
    load_dotenv()

    # Get replica counts from environment variables
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
    clients_amount = get_env("CLIENTS_AMOUNT")

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
                    "REPLICAS": 1
                },
                "build": {
                    "context": ".",
                    "dockerfile": "cmd/requesthandler/Dockerfile"
                },
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "volumes": [
                    "./docs:/docs"
                ],
                "command": "/requesthandler/request_handler.go"
            }
        }
    }

    # Generate movies receiver services
    for i in range(1, movies_receiver_amount + 1):
        service_name = f"moviesreceiver_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": movies_receiver_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/moviesreceiver/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            }
        }

    # Generate filter services
    for query in [1, 3, 4]:
        amount = q1_filter_amount if query == 1 else (q3_filter_amount if query == 3 else q4_filter_amount)
        for i in range(1, amount + 1):
            service_name = f"filter_q{query}_{i}"
            compose["services"][service_name] = {
                "environment": {
                    "QUERY": query,
                    "ID": i,
                    "REPLICAS": amount
                },
                "build": {
                    "context": ".",
                    "dockerfile": "cmd/filter/Dockerfile"
                },
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "command": "/filter/filter.go"
            }

    # Generate ratings receiver services
    for i in range(1, ratings_receiver_amount + 1):
        service_name = f"ratingsreceiver_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": ratings_receiver_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/ratingsreceiver/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            }
        }

    # Generate ratings joiner services
    for i in range(1, ratings_joiner_amount + 1):
        service_name = f"ratingsjoiner_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": ratings_joiner_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/joiners/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            }
        }

    # Generate sink services
    for query in [1, 3, 5]:
        if query == 1:
            amount = q1_sink_amount
            dockerfile = "cmd/sinks/Dockerfile"
            command = "/q1_sink/q1_sink.go"
        elif query == 3:
            amount = sentiment_sink_amount  # Ajustar si es necesario
            dockerfile = "cmd/sinks_q3/Dockerfile"
            command = None
        else:
            amount = sentiment_sink_amount
            dockerfile = "cmd/sinks_q5/Dockerfile"
            command = None

        for i in range(1, amount + 1):
            service_name = f"q{query}_sink_{i}"
            compose["services"][service_name] = {
                "environment": {
                    "ID": i,
                    "REPLICAS": amount
                },
                "build": {
                    "context": ".",
                    "dockerfile": dockerfile
                },
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                }
            }
            if command:
                compose["services"][service_name]["command"] = command

    # Generate budget reducer services
    for i in range(1, budget_reducer_amount + 1):
        service_name = f"budget_reducer_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": budget_reducer_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/reducers/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "command": "/budget_reducer.go"
        }

    # Generate budget sink services
    for i in range(1, budget_sink_amount + 1):
        service_name = f"budget_sink_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": budget_sink_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/sinks_q2/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "command": "/budget_sink/budget_sink.go"
        }

    # Generate sentiment worker services
    for i in range(1, sentiment_worker_amount + 1):
        service_name = f"sentiment_worker_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": sentiment_worker_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/sentiment/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            }
        }

    # Generate sentiment reducer services
    for i in range(1, sentiment_reducer_amount + 1):
        service_name = f"sentiment_reducer_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": sentiment_reducer_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/reducers/sentiment_reducer/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "command": "/sentiment_reducer/sentiment_reducer.go"
        }

    # Generate credits joiner services
    for i in range(1, credits_joiner_amount + 1):
        service_name = f"credits_joiner_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": credits_joiner_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/joiners_q4/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            }
        }

    # Generate credits receiver services
    for i in range(1, credits_receiver_amount + 1):
        service_name = f"credits_receiver_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": credits_receiver_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/credits_receiver/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "command": "/credits_receiver/credits_receiver.go"
        }

    # Generate credits sink services
    for i in range(1, credits_sink_amount + 1):
        service_name = f"credits_sink_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i,
                "REPLICAS": credits_sink_amount
            },
            "build": {
                "context": ".",
                "dockerfile": "cmd/sinks_q4/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "command": "/credits_sink/credits_sink.go"
        }

    # Generate clients 
    compose["services"]["client"] = {
        "build": {
            "context": ".",
            "dockerfile": "cmd/client/Dockerfile"
        },
        "depends_on": [
            "requesthandler"
        ],
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
    
    