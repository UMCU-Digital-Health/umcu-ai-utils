import json
import logging

from flask import Request

logger = logging.getLogger(__name__)


def get_user_email(req: Request) -> str:
    """Get the user email from RStudio credentials.

    Parameters
    ----------
    req : Request
        The request object.

    Returns
    -------
    str
        The user's email.
    """
    credential_header = req.headers.get("RStudio-Connect-Credentials")
    if not credential_header:
        logger.warning("No credentials found in request headers")
        return "No user"

    credential_header = json.loads(credential_header)
    user = credential_header.get("user").lower()
    return user
