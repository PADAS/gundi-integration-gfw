import pytest


@pytest.fixture(autouse=True)
def template_default_integration_type_name(mocker):
    # The template's self-registration tests assume no integration-specific
    # display name is configured; this integration sets one in
    # app/settings/integration.py. Neutralize it here (integration-owned
    # conftest) so the template-owned test files stay unmodified and
    # merge cleanly on future upstream syncs.
    mocker.patch("app.services.self_registration.INTEGRATION_TYPE_NAME", None)
    yield
