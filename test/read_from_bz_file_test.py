from __future__ import annotations
import unittest
import configparser
import os
from src.austin_heller_repo.wiki_data_parser import WikiDataParser, RedisConfig, SearchCriteria, PageCriteria, EntityTypeEnum, SetComplimentTypeEnum, Entity, HostPointer
from typing import List, Tuple, Dict


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

	def test_read_multiple_of_the_same_element(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path,
			redis_config=None
		)

		found_entity = None
		for _ in range(10):
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

			if found_entity is None:
				found_entity = entities[0]
			else:
				self.assertEqual(found_entity, entities[0])

	def test_read_multiple_of_different_elements_without_cache(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path,
			redis_config=None
		)

		found_entities = []  # type: List[Entity]
		for page_index in range(10):
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
					page_index=page_index,
					page_size=1
				)
			)

			self.assertIsNotNone(entities)
			self.assertEqual(1, len(entities))
			print(entities[0])

			self.assertNotIn(entities[0], found_entities)
			found_entities.append(entities[0])

	def test_read_multiple_of_different_elements_with_cache(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path,
			redis_config=RedisConfig(
				host_pointer=HostPointer(
					host_address="0.0.0.0",
					host_port=6379
				),
				expire_seconds=5
			)
		)

		found_entities = []  # type: List[Entity]
		for page_index in range(10):
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
					page_index=page_index,
					page_size=1
				)
			)

			self.assertIsNotNone(entities)
			self.assertEqual(1, len(entities))
			print(entities[0])

			self.assertNotIn(entities[0], found_entities)
			found_entities.append(entities[0])

		self.assertEqual(found_entities[-1].get_label(), "penis")

	def test_read_100_elements_without_cache(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path,
			redis_config=None
		)

		entities_total = 0
		for _ in range(100):
			try:
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
						page_index=entities_total,
						page_size=1
					)
				)
			except Exception as ex:
				print(f"Encountered exception after {entities_total} entities.")
				raise

			if not entities:
				break

			entities_total += 1

	def test_read_100_elements_with_cache(self):

		config = configparser.ConfigParser()
		config.read("settings.ini")

		file_locations_config = config["FilePath"]
		file_path = file_locations_config["Compressed"]

		wiki_data_parser = WikiDataParser(
			json_file_path=file_path,
			redis_config=RedisConfig(
				host_pointer=HostPointer(
					host_address="0.0.0.0",
					host_port=6379
				),
				expire_seconds=5
			)
		)

		entities_total = 0
		for _ in range(100):
			try:
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
						page_index=entities_total,
						page_size=1
					)
				)
			except Exception as ex:
				print(f"Encountered exception after {entities_total} entities.")
				raise

			if not entities:
				break

			entities_total += 1
