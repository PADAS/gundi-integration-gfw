# Add your integration-specific settings here

from environs import Env

env = Env()
env.read_env()

GFW_DATASET_QUERY_CONCURRENCY = env.int("GFW_DATASET_QUERY_CONCURRENCY", 5)

# Display name registered in Gundi (overrides the name derived from the type slug)
INTEGRATION_TYPE_NAME = env.str("INTEGRATION_TYPE_NAME", "Global Forest Watch")
PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND = env.bool("PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND", False)
