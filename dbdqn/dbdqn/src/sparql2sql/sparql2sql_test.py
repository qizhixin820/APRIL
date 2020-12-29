import unittest

from sparql2sql import sparql2sql

class TestSparql2sql(unittest.TestCase):

    def test_query1(self):
        print("1:")
        with open("../../data/test_sparql2sql/test1.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query2(self):
        print("2:")
        with open("../../data/test_sparql2sql/test2.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query3(self):
        print("3:")
        with open("../../data/test_sparql2sql/test3.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query4(self):
        print("4:")
        with open("../../data/test_sparql2sql/test4.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()

    def test_query5(self):
        print("5:")
        with open("../../data/test_sparql2sql/test5.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query6(self):
        print("6:")
        with open("../../data/test_sparql2sql/test6.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query7(self):
        print("7:")
        with open("../../data/test_sparql2sql/test7.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query8(self):
        print("8:")
        with open("../../data/test_sparql2sql/test8.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query9(self):
        print("9:")
        with open("../../data/test_sparql2sql/test9.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query10(self):
        print("10:")
        with open("../../data/test_sparql2sql/test10.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()
    def test_query11(self):
        print("11:")
        with open("../../data/test_sparql2sql/test11.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()

    def test_query12(self):
        print("12:")
        with open("../../data/test_sparql2sql/test12.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()

    def test_query13(self):
        print("13:")
        with open("../../data/test_sparql2sql/test13.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()

    def test_query14(self):
        print("14:")
        with open("../../data/test_sparql2sql/test14.txt", "r") as f:
            test = f.read()
            sparql2sql(test)
            f.close()

if __name__ == '__main__':
    unittest.main()