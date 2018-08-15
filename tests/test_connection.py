import unittest
import easyaccess as ea
from easyaccess.eautils.python_api import connect 

class TestConnection(unittest.TestCase):

    def test_connect_dessci(self):
        con = connect('dessci', quiet=True)
        self.assertTrue(con.ping())

    def test_connect_desoper(self):
        con = connect('desoper', quiet=True)
        self.assertTrue(con.ping())

    def test_connect_destest(self):
        con = connect('destest', quiet=True)
        self.assertTrue(con.ping())

    # @unittest.skip("Not implemented yet")
    # def test_connect_memsql(self):
    #    con = ea.connect('memsql')
    #    self.assertTrue(con)


if __name__ == '__main__':
    unittest.main()
    # suite = unittest.TestLoader().loadTestsFromTestCase(TestConnection)
    # unittest.TextTestRunner(verbosity=2).run(suite)
