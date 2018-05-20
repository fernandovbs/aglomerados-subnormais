import unittest

import aglomerados_subnormais


class Aglomerados_subnormaisTestCase(unittest.TestCase):

    def setUp(self):
        self.app = aglomerados_subnormais.app.test_client()

    def test_index(self):
        rv = self.app.get('/')
        self.assertIn('Welcome to aglomerados_subnormais', rv.data.decode())


if __name__ == '__main__':
    unittest.main()
