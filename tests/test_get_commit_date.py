import requests
import responses
import unittest

from pup.utils.get_commit_date import get_commit_date


class GetCommitDateTests(unittest.TestCase):

    @responses.activate
    def test_get_commit_date(self):
        commit_url = "https://api.github.com/repos/RedHatInsights/insights-pup/git/commits/ca55e77e"
        commit_url_response = '{"committer": {"date": "last summer"}}'
        responses.add(responses.GET, commit_url, body=commit_url_response,
                      status=requests.codes.ok, content_type='application/json')

        result = get_commit_date('ca55e77e')
        self.assertEquals(result, 'last summer')
