from __future__ import annotations
import unittest
from datetime import datetime, timedelta
from austin_heller_repo.common import ElapsedTimerMessageManager


class BreakEarlyTest(unittest.TestCase):

	def test_print_check_each_time(self):
		elapsed_timer = ElapsedTimerMessageManager(
			include_datetime_prefix=True,
			include_stack=False
		)
		elapsed_timer.print(f"start")
		for _ in range(1000000):
			is_at_least_one_found = False
			for index in range(1000):
				if 250 < index < 750:
					if not is_at_least_one_found:
						is_at_least_one_found = True
				elif is_at_least_one_found:
					break
		elapsed_timer.print(f"end")

	def test_print_never_check(self):
		elapsed_timer = ElapsedTimerMessageManager(
			include_datetime_prefix=True,
			include_stack=False
		)
		elapsed_timer.print(f"start")
		for _ in range(1000000):
			is_at_least_one_found = False
			for index in range(1000):
				if 250 < index < 750:
					is_at_least_one_found = True
				elif is_at_least_one_found:
					break
		elapsed_timer.print(f"end")
