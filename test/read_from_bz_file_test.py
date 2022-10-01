from __future__ import annotations
import unittest
import configparser
import os
from src.austin_heller_repo.wiki_data_parser import WikiDataParser, RedisConfig, SearchCriteria, PageCriteria, EntityTypeEnum, SetComplimentTypeEnum


class ReadFromBzFileTest(unittest.TestCase):

	def test_settings_and_file_exists(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		self.assertIsNotNone(file_path)
		self.assertTrue(os.path.exists(file_path))

	def test_initialize(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path,
			redis_config=None
		)

	def test_read_one_element(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path,
			redis_config=None
		)

		entities = wiki_data_parser.search(
			search_criteria=SearchCriteria(
				entity_types=[
					EntityTypeEnum.Item,
					EntityTypeEnum.Property
				],
				entity_types_set_compliment_type=SetComplimentTypeEnum.Inclusive,
				id=None,
				label_parts=None,
				description_parts=None
			),
			page_criteria=PageCriteria(
				page_index=0,
				page_size=1
			)
		)

		self.assertIsNotNone(entities)
		self.assertEqual(1, len(entities))
		print(entities[0])
