from pup.utils import tracker

from unittest import TestCase
from freezegun import freeze_time


class TestTrackerMessage(TestCase):

    @freeze_time("2019-06-14T08:48:40.953420")
    def test_payload_tracker(self):

        topic = "platform.payload-status"
        good_msg = {"service": "advisor-pup",
                    "request_id": "dootdoot",
                    "payload_id": "dootdoot",
                    "status": "test",
                    "status_msg": "unittest",
                    "account": "123456",
                    "date": tracker.get_time()}

        self.assertEqual({"topic": topic, "msg": good_msg}, tracker.payload_tracker("dootdoot", "123456", "test", "unittest"))
