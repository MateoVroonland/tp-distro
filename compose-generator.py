import yaml, sys

def generate_compose(num_replicas):
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
            },
            "moviesreceiver": {
                "build": {
                    "context": ".",
                    "dockerfile": "cmd/moviesreceiver/Dockerfile"
                },
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "deploy": {
                    "replicas": 3
                }
            }
        }
    }
    for query in [1, 3, 4]:
        service_name = f"filter_q{query}"
        compose["services"][service_name] = {
            "environment": {
                "QUERY": query
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
        compose["services"][service_name]["deploy"] = {"replicas": num_replicas}
    compose["services"]["ratingsreceiver"] = {
        "build": {
            "context": ".",
            "dockerfile": "cmd/ratingsreceiver/Dockerfile"
        },
        "depends_on": {
            "rabbitmq": {
                "condition": "service_healthy"
            }
        },
        "deploy": {
            "replicas": 3
        }
    }
    for i in range(1, 4):
        service_name = f"ratingsjoiner_{i}"
        compose["services"][service_name] = {
            "environment": {
                "ID": i
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
    for query in [1, 3]:
        service_name = f"q{query}_sink"
        dockerfile = "cmd/sinks/Dockerfile" if query == 1 else "cmd/sinks_q3/Dockerfile"
        command = "/q1_sink/q1_sink.go" if query == 1 else None 
        compose["services"][service_name] = {
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
    compose["services"].update({
        "budget_reducer": {
            "build": {
                "context": ".",
                "dockerfile": "cmd/reducers/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "command": "/budget_reducer.go",
            "deploy": {
                "replicas": 3
            }
        },
        "budget_sink": {
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
    })

    compose["services"].update({
        "sentiment_worker": {
            "build": {
                "context": ".",
                "dockerfile": "cmd/sentiment/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "deploy": {
                "replicas": 2
            }
        },
        "sentiment_reducer": {
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
    })

    for i in range(1, 4):
        service_name = f"credits_joiner_{i}" if i > 1 else "credits_joiner"
        compose["services"][service_name] = {
            "environment": {
                "ID": i
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
    compose["services"].update({
        "credits_receiver": {
            "build": {
                "context": ".",
                "dockerfile": "cmd/credits_receiver/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "command": "/credits_receiver/credits_receiver.go",
            "deploy": {
                "replicas": 3
            }
        },
        "credits_sink": {
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
    })

    with open('docker-compose-prueba.yml', 'w') as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)

if __name__ == "__main__":
    num_replicas = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    generate_compose(num_replicas)
    
    