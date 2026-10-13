# Root conftest: runs before pytest imports anything under app/, so an
# environment default set here is in place before app.settings loads.
import os

import pytest

# Tests share Gundi OAuth tokens within the process only. The runner's default
# is redis://<REDIS_HOST>:<REDIS_PORT>/2, and a developer with a local Redis up
# would otherwise have a token minted by one pytest run persisted and served to
# the next, while CI (no Redis) sees none of it. An explicit value in the
# environment is respected.
os.environ.setdefault("GUNDI_TOKEN_CACHE_URL", "")


@pytest.fixture(autouse=True)
def template_default_integration_type_name(mocker):
    # The template's self-registration tests assume no integration-specific
    # display name is configured; this integration sets one in
    # app/settings/integration.py. Neutralize it here (integration-owned
    # conftest) so the template-owned test files stay unmodified and
    # merge cleanly on future upstream syncs.
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_NAME", None)
    yield
