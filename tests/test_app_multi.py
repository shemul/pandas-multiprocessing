from unittest import TestCase
from main import calculate_age

class Test(TestCase):
    def test_calculate_age(self):
        dob = "1982-09-09"
        res = calculate_age(dob)
        self.assertEqual(res, 37)
        
