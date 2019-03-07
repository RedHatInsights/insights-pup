import requests

from pup.utils import configuration

COMMIT_BASE_URL = "https://api.github.com/repos/RedHatInsights/insights-pup/git/commits/"


def get_commit_date(commit_id):
    url = COMMIT_BASE_URL + commit_id
    response = requests.get(url).json()
    return response['committer']['date']


def get_version_info():
    if configuration.DEVMODE:
        date = 'devmode'
    elif configuration.BUILD_ID:
        date = get_commit_date(configuration.BUILD_ID)
    else:
        raise "OPENSHIFT_BUILD_COMMIT or DEVMODE not set"
    return {"version": configuration.BUILD_ID, "date": date}
