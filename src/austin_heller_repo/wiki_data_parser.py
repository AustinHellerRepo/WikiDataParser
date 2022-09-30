from __future__ import annotations
from austin_heller_repo.common import StringEnum
from typing import List, Tuple, Dict
import ijson


class EntityTypeEnum(StringEnum):
	Item = "item"
	Property = "property"


class Entity():

	def __init__(self):
		pass

	@classmethod
	def parse_json(cls, *, json_dict: Dict) -> Entity:
		raise NotImplementedError()


class PageCriteria():

	def __init__(self, *, page_index: int, page_size: int):
		self.__page_index = page_index
		self.__page_size = page_size

		self.__start_inclusive_entity_index = self.__page_index * self.__page_size
		self.__end_exclusive_entity_index = self.__start_inclusive_entity_index + self.__page_size

	def is_valid(self, *, entity_index: int) -> bool:
		return self.__start_inclusive_entity_index <= entity_index < self.__end_exclusive_entity_index


class SearchCriteria():

	def __init__(self, *, included_entity_types: List[EntityTypeEnum], excluded_entity_types: List[EntityTypeEnum], label_parts: List[str], description_parts: List[str]):
		self.__included_entity_types = included_entity_types
		self.__excluded_entity_types = excluded_entity_types
		self.__label_parts = label_parts
		self.__description_parts = description_parts

	def is_valid(self, *, entity: Entity) -> bool:
		raise NotImplementedError()


class SearchCriteriaBuilder():

	def __init__(self):

		self.__included_entity_types = []  # type: List[EntityTypeEnum]
		self.__excluded_entity_types = []  # type: List[EntityTypeEnum]
		self.__label_parts = []  # type: List[str]
		self.__description_parts = []  # type: List[str]

	def get_search_criteria(self) -> SearchCriteria:
		return SearchCriteria(
			included_entity_types=self.__included_entity_types.copy(),
			excluded_entity_types=self.__excluded_entity_types.copy(),
			label_parts=self.__label_parts.copy(),
			description_parts=self.__description_parts.copy()
		)

	def include_entity_type(self, *, entity_type: EntityTypeEnum):
		self.__included_entity_types.append(entity_type)

	def exclude_entity_type(self, *, entity_type: EntityTypeEnum):
		self.__excluded_entity_types.append(entity_type)

	def label_contains(self, *, label_part: str):
		self.__label_parts.append(label_part)

	def description_contains(self, *, description_part: str):
		self.__description_parts.append(description_part)


class WikiDataParser():

	def __init__(self, *, json_file_path: str):
		self.__json_file_path = json_file_path

	def search(self, *, search_criteria: SearchCriteria, page_criteria: PageCriteria) -> List[Entity]:
		entities = []  # type: List[Entity]
		entity_index = 0
		is_at_least_one_entry_found = False
		# TODO keep record of item_index for specific search_criteria and page_criteria for quick skips, jumping to next possible area of search
		with open(self.__json_file_path, "rb") as file_handle:
			for entity_json in ijson.items(file_handle, "item"):
				entity = Entity.parse_json(
					json_dict=entity_json
				)
				if search_criteria.is_valid(
					entity=entity
				):
					if page_criteria.is_valid(
						entity_index=entity_index
					):
						if not is_at_least_one_entry_found:
							is_at_least_one_entry_found = True
						entities.append(entity)
					elif is_at_least_one_entry_found:
						# already found all of the entities that satisfy the page criteria
						break
					entity_index += 1
		return entities
