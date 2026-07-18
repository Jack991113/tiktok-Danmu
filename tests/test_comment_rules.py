import unittest

from comment_rules import classify_print_content, has_required_permanent_number


class CommentRulesTests(unittest.TestCase):
    def classify(self, content: str):
        return classify_print_content(
            content,
            numeric_enabled=True,
            keyword_enabled=False,
            keywords=[],
            min_length=1,
            max_length=100,
        )

    def test_pure_ascii_digits_qualify(self):
        self.assertEqual(self.classify("88"), (True, "numeric"))

    def test_digit_letter_code_qualifies(self):
        self.assertEqual(self.classify("A88b"), (True, "alphanumeric"))

    def test_letters_or_punctuation_do_not_qualify(self):
        self.assertEqual(self.classify("ABC"), (False, ""))
        self.assertEqual(self.classify("88-A"), (False, ""))

    def test_alphanumeric_requires_an_existing_permanent_number(self):
        self.assertFalse(has_required_permanent_number("alphanumeric", ""))
        self.assertTrue(has_required_permanent_number("alphanumeric", 123))
        self.assertTrue(has_required_permanent_number("numeric", ""))


if __name__ == "__main__":
    unittest.main()
