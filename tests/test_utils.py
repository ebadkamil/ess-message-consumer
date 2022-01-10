import unittest

from ess_message_consumer.utils import BoundOrderedDict


class TestBoundOrderedDict(unittest.TestCase):
    def setUp(self):
        self._dict = BoundOrderedDict(maxlen=2)

    def test_adding_element_beyond_maxlen_removes_from_begin(self):
        self._dict["key1"] = "value1"
        self._dict["key2"] = "value2"
        self._dict["key3"] = "value3"

        self.assertTrue("key1" not in self._dict)
        # First element is ("key2", "value2")
        kev_val = self._dict.popitem(last=False)
        self.assertTupleEqual(kev_val, ("key2", "value2"))
