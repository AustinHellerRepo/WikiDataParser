from __future__ import annotations
import unittest


class IterateFirstThenLoopTest(unittest.TestCase):

	def test_no_iteration(self):
		iterator = enumerate(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
		count = 0
		for _ in iterator:
			count += 1
		self.assertEqual(10, count)

	def test_one_iteration(self):
		iterator = enumerate(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
		count = 0
		next(iterator)
		for _ in iterator:
			count += 1
		self.assertEqual(9, count)

	def test_all_but_one_iteration(self):
		iterator = enumerate(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
		count = 0
		for _ in range(9):
			next(iterator)
		for index, letter in iterator:
			count += 1
		self.assertEqual(1, count)
		self.assertEqual(9, index)
		self.assertEqual('j', letter)

	def test_all_iterations(self):
		iterator = enumerate(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
		count = 0
		for _ in range(10):
			next(iterator)
		for _ in iterator:
			count += 1
		self.assertEqual(0, count)
