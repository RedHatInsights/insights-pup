import requests

COMMIT_BASE_URL = "https://api.github.com/repos/RedHatInsights/insights-pup/git/commits/"


def get_commit_date(commit_id):
    url = COMMIT_BASE_URL + commit_id
    response = requests.get(url).json()
    return response['committer']['date']
