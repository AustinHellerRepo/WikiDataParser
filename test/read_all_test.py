from __future__ import annotations
import unittest
import configparser
import os
from src.austin_heller_repo.wiki_data_parser import WikiDataParser, RedisConfig, SearchCriteria, PageCriteria, EntityTypeEnum, SetComplimentTypeEnum, Entity, HostPointer, WikiDataParserIterator, LanguageEnum
from typing import List, Tuple, Dict
from datetime import datetime, timedelta


class ReadAllTest(unittest.TestCase):

	def test_read_every_element_and_print_total_time(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path
		)

		last_entity_index = -1
		start_time = datetime.utcnow()
		for entity_index, entity in enumerate(WikiDataParserIterator(
			wiki_data_parser=wiki_data_parser,
			search_criteria=None
		)):
			last_entity_index = entity_index
			if last_entity_index % 10000 == 0:
				print(f"{datetime.utcnow()}: {last_entity_index}: {entity}")
		end_time = datetime.utcnow()
		print(f"last entity index: {last_entity_index}")
		print(f"total time: {(end_time - start_time).total_seconds()}")
