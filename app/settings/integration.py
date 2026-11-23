# Add your integration-specific settings here

from environs import Env

env = Env()
env.read_env()

GFW_DATASET_QUERY_CONCURRENCY = env.int("GFW_DATASET_QUERY_CONCURRENCY", 5)

# Display name registered in Gundi (overrides the name derived from the type slug)
INTEGRATION_TYPE_NAME = env.str("INTEGRATION_TYPE_NAME", "Global Nature Watch")

# Shared Global Nature Watch account, used by every integration that doesn't
# bring its own. Set as deployment secrets so they never show in the portal.
GNW_DEFAULT_EMAIL = env.str("GNW_DEFAULT_EMAIL", None)
GNW_DEFAULT_PASSWORD = env.str("GNW_DEFAULT_PASSWORD", None)
