import logging
import requests

COMMIT_BASE_URL = "https://api.github.com/repos/RedHatInsights/insights-pup/git/commits/"

logger = logging.getLogger('advisor-pup')


def get_commit_date(commit_id):
    url = COMMIT_BASE_URL + commit_id
    try:
        response = requests.get(url, timeout=0.5).json()
        return response['committer']['date']
    except Exception:
        # Log the exception and keep going.  Do not let failing to
        # determine the commit date stop the process from starting.
        logger.exception("Error retrieving commit date from github")
        return "unknown"
