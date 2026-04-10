import os
import logging

import knime.extension as knext
import smartsheet

LOGGER = logging.getLogger(__name__)

TOKEN_NAME = "SMARTSHEET_ACCESS_TOKEN"
REGION_NAME = "SMARTSHEET_REGION"


def get_access_token_from_credentials(context: knext.ConfigurationContext):
    try:
        credentials = context.get_credentials(TOKEN_NAME)
        if credentials.password == "":
            raise KeyError
        LOGGER.debug(
            f"{TOKEN_NAME} has been set via credentials coming in as flow variable."
        )
        return credentials.password
    except KeyError:
        raise knext.InvalidParametersError(
            f"Either {TOKEN_NAME} was not set in your env or "
            "the Credentials Configuration node (which should "
            "set the flow variable for this node) did not contain "
            f"a parameter called {TOKEN_NAME} or the password in there was empty."
        )


def resolve_token_and_region():
    """Read token and region from environment variables, parsing region prefix if present."""
    token = os.environ.get(TOKEN_NAME, "")
    region = os.environ.get(REGION_NAME, "")
    if token and not region:
        parts = token.split(":", maxsplit=1)
        if len(parts) == 2 and parts[0] in ("eu", "gov"):
            region, token = parts
    return token, region


def create_client(context, access_token, access_region):
    """Resolve credentials and create a Smartsheet client.

    If access_token is empty, falls back to credentials configuration.
    Returns (smartsheet.Smartsheet, access_token, access_region).
    """
    if not access_token:
        access_token = get_access_token_from_credentials(context)
        parts = access_token.split(":", maxsplit=1)
        if len(parts) == 2 and parts[0] in ("eu", "gov"):
            access_region, access_token = parts

    if access_region == "eu":
        api_base = smartsheet.__eu_base__
    elif access_region == "gov":
        api_base = smartsheet.__gov_base__
    else:
        api_base = smartsheet.__api_base__

    client = smartsheet.Smartsheet(access_token=access_token, api_base=api_base)
    return client, access_token, access_region


def validate_credentials(context, access_token):
    """Validate that credentials are available during configure phase."""
    if not access_token:
        get_access_token_from_credentials(context)
