from __future__ import annotations
from austin_heller_repo.common import StringEnum, HostPointer, convert_decimal_to_complex_string
from typing import List, Tuple, Dict, Optional
import ijson
import json
import redis
import hashlib
from abc import ABC, abstractmethod
import bz2
import gzip


class EntityTypeEnum(StringEnum):
	Item = "item"
	Property = "property"


class SetComplimentTypeEnum(StringEnum):
	Inclusive = "inclusive"
	Exclusive = "exclusive"


class PropertyTypeEnum(StringEnum):
	DateTime = "date_time"
	Item = "item"
	ExternalId = "external_id"
	Quantity = "quantity"
	Coordinate = "coordinate"
	String = "string"
	MonolingualText = "monolingual_text"
	Url = "url"
	Lexeme = "lexeme"
	Property = "property"
	Math = "math"


class ClaimPropertyValue():

	def __init__(self, *, property_type: PropertyTypeEnum, property_value: str):
		self.__property_type = property_type
		self.__property_value = property_value

	def __str__(self):
		return f"{self.__property_value} ({self.__property_type})"

	def __eq__(self, other):
		if isinstance(other, ClaimPropertyValue):
			return self.__property_type == other.get_type() and \
				self.__property_value == other.get_value()
		return False

	def __hash__(self):
		return hash((self.__property_type.value, self.__property_value))

	def get_type(self) -> PropertyTypeEnum:
		return self.__property_type

	def get_value(self) -> str:
		return self.__property_value

	@classmethod
	def try_parse_json(cls, *, json_dict: Dict) -> Tuple[bool, Optional[ClaimPropertyValue]]:
		data_type = json_dict["datatype"]
		if data_type == "wikibase-item":
			property_type = PropertyTypeEnum.Item
			if "datavalue" not in json_dict:
				raise Exception(f"Missing datavalue: {json_dict}")
			property_value = json_dict["datavalue"]["value"]["id"]
		elif data_type == "time":
			property_type = PropertyTypeEnum.DateTime
			property_value = json_dict["datavalue"]["value"]["time"]
		elif data_type == "external-id":
			property_type = PropertyTypeEnum.ExternalId
			property_value = json_dict["datavalue"]["value"]
		elif data_type == "quantity":
			property_type = PropertyTypeEnum.Quantity
			property_value = json.dumps({
				"amount": json_dict["datavalue"]["value"]["amount"],
				"unit": json_dict["datavalue"]["value"]["unit"]
			})
		elif data_type == "globe-coordinate":
			property_type = PropertyTypeEnum.Coordinate
			property_value = json.dumps({
				"latitude": convert_decimal_to_complex_string(
					decimal_value=json_dict["datavalue"]["value"]["latitude"]
				),
				"longitude": convert_decimal_to_complex_string(
					decimal_value=json_dict["datavalue"]["value"]["longitude"]
				),
				"altitude": convert_decimal_to_complex_string(
					decimal_value=json_dict["datavalue"]["value"]["altitude"]
				),
				"precision": convert_decimal_to_complex_string(
					decimal_value=json_dict["datavalue"]["value"]["precision"]
				)
			})
		elif data_type == "string":
			property_type = PropertyTypeEnum.String
			property_value = json_dict["datavalue"]["value"]
		elif data_type == "monolingualtext":
			property_type = PropertyTypeEnum.MonolingualText
			property_value = json.dumps({
				"text": json_dict["datavalue"]["value"]["text"],
				"language": json_dict["datavalue"]["value"]["language"]
			})
		elif data_type == "commonsMedia":
			property_type = None
			property_value = None
		elif data_type == "url":
			property_type = PropertyTypeEnum.Url
			property_value = json_dict["datavalue"]["value"]
		elif data_type == "geo-shape":
			property_type = None
			property_value = None
		elif data_type == "wikibase-lexeme":
			property_type = PropertyTypeEnum.Lexeme
			property_value = json_dict["datavalue"]["value"]["id"]
		elif data_type == "wikibase-property":
			property_type = PropertyTypeEnum.Property
			property_value = json_dict["datavalue"]["value"]["id"]
		elif data_type == "math":
			property_type = PropertyTypeEnum.Math
			property_value = json_dict["datavalue"]["value"]
		else:
			raise NotImplementedError(f"Data type \"{data_type}\" not implemented: {json_dict}")

		if property_type is None:
			return False, None

		return True, ClaimPropertyValue(
			property_type=property_type,
			property_value=property_value
		)


class Claim():

	def __init__(self, *, property_id: str, property_values: List[ClaimPropertyValue]):
		self.__property_id = property_id
		self.__property_values = property_values

	def __str__(self):
		return f"{self.__property_id}: {', '.join([str(property_value) for property_value in self.__property_values])}"

	def __eq__(self, other):
		if isinstance(other, Claim):
			return self.__property_id == other.get_property_id() and \
				self.__property_values == other.get_property_values()
		return False

	def __hash__(self):
		return hash((self.__property_id, self.__property_values))

	def get_property_id(self) -> str:
		return self.__property_id

	def get_property_values(self) -> List[ClaimPropertyValue]:
		return self.__property_values.copy()


class Entity():

	def __init__(self, *, entity_type: EntityTypeEnum, id: str, label: str, description: str, claims: List[Claim]):
		self.__entity_type = entity_type
		self.__id = id
		self.__label = label
		self.__description = description
		self.__claims = claims

	def __str__(self):
		return f"{self.__entity_type.value} ({self.__id}): {self.__label}, {self.__description}. {len(self.__claims)} claim{'s' if self.__claims else ''}."

	def __eq__(self, other):
		if isinstance(other, Entity):
			return self.__entity_type == other.get_entity_type() and \
				self.__id == other.get_id() and \
				self.__label == other.get_label() and \
				self.__description == other.get_description() and \
				self.__claims == other.get_claims()
		return False

	def __hash__(self):
		return hash((self.__entity_type.value, self.__id, self.__label, self.__description, self.__claims))

	def get_entity_type(self) -> EntityTypeEnum:
		return self.__entity_type

	def get_id(self) -> str:
		return self.__id

	def get_label(self) -> str:
		return self.__label

	def get_description(self) -> str:
		return self.__description

	def get_claims(self) -> List[Claim]:
		return self.__claims

	@classmethod
	def parse_json(cls, *, json_dict: Dict) -> Entity:
		return Entity(
			entity_type=EntityTypeEnum(json_dict["type"]),
			id=json_dict["id"],
			label=json_dict["labels"]["en"]["value"],
			description=json_dict["descriptions"]["en"]["value"],
			claims=[
				Claim(
					property_id=claim_key,
					property_values=[
						claim_property_value for is_successful, claim_property_value in
						[
							ClaimPropertyValue.try_parse_json(
								json_dict=claim_property_value_json_dict["mainsnak"]
							) for claim_property_value_json_dict in claim_property_value_json_dicts if "datavalue" in claim_property_value_json_dict["mainsnak"]
						]
						if is_successful
					]
				) for claim_key, claim_property_value_json_dicts in json_dict["claims"].items()
			]
		)


class PageCriteria():

	def __init__(self, *, page_index: int, page_size: int):
		self.__page_index = page_index
		self.__page_size = page_size

		self.__start_inclusive_entity_index = self.__page_index * self.__page_size
		self.__end_exclusive_entity_index = self.__start_inclusive_entity_index + self.__page_size
		self.__current_redis_key = hashlib.sha1(f"{self.__start_inclusive_entity_index}\u0000{self.__end_exclusive_entity_index}".encode()).hexdigest()
		self.__next_redis_key = hashlib.sha1(f"{self.__end_exclusive_entity_index}\u0000{self.__end_exclusive_entity_index + self.__page_size}".encode()).hexdigest()

	def is_valid(self, *, entity_index: int) -> bool:
		return self.__start_inclusive_entity_index <= entity_index < self.__end_exclusive_entity_index

	def is_last_valid_entity_index(self, *, entity_index: int):
		return entity_index + 1 == self.__end_exclusive_entity_index

	def get_first_valid_entity_index(self) -> int:
		return self.__start_inclusive_entity_index

	def get_current_redis_key(self) -> str:
		return self.__current_redis_key

	def get_next_redis_key(self) -> str:
		return self.__next_redis_key


class SearchCriteria():

	def __init__(self, *, entity_types: List[EntityTypeEnum], entity_types_set_compliment_type: SetComplimentTypeEnum, id: Optional[str], label_parts: Optional[List[str]], description_parts: Optional[List[str]]):
		self.__entity_types = entity_types
		self.__entity_types_set_compliment_type = entity_types_set_compliment_type
		self.__id = id
		self.__label_parts = label_parts
		self.__description_parts = description_parts

		self.__redis_key = hashlib.sha1(f"{','.join([entity_type.value for entity_type in self.__entity_types])}\u0000{self.__entity_types_set_compliment_type.value}\u0000{self.__id}\u0000{self.__label_parts}\u0000{self.__description_parts}".encode()).hexdigest()

	def is_valid(self, *, entity: Entity) -> bool:
		if (self.__entity_types_set_compliment_type == SetComplimentTypeEnum.Inclusive and entity.get_entity_type() not in self.__entity_types) or \
				(self.__entity_types_set_compliment_type == SetComplimentTypeEnum.Exclusive and entity.get_entity_type() in self.__entity_types):
			return False
		if self.__id is not None and entity.get_id() != self.__id:
			return False
		if self.__label_parts is not None:
			for label_part in self.__label_parts:
				if label_part not in entity.get_label():
					return False
		if self.__description_parts is not None:
			for description_part in self.__description_parts:
				if description_part not in entity.get_description():
					return False
		return True

	def get_redis_key(self) -> str:
		return self.__redis_key


class RedisConfig():

	def __init__(self, *, host_pointer: HostPointer, expire_seconds: Optional[float]):
		self.__host_pointer = host_pointer
		self.__expire_seconds = expire_seconds

	def get_host_pointer(self) -> HostPointer:
		return self.__host_pointer

	def get_expire_seconds(self) -> float:
		return self.__expire_seconds


class WikiDataParser():

	def __init__(self, *, json_file_path: str, redis_config: Optional[RedisConfig]):
		self.__json_file_path = json_file_path
		self.__redis_config = redis_config

		if self.__redis_config is not None:
			self.__next_index_cache = redis.Redis(
				host=self.__redis_config.get_host_pointer().get_host_address(),
				port=self.__redis_config.get_host_pointer().get_host_port(),
				db=0,
				connection_pool=None
			)
		else:
			self.__next_index_cache = None

	def __search_file_handle(self, *, file_handle, start_index: int, is_started_at_next_index: bool, search_criteria: SearchCriteria, page_criteria: PageCriteria) -> List[Entity]:

		entities = []  # type: List[Entity]

		# TODO see if it's possible to set the offset of the file_handle instead of burning records
		iterable = enumerate(ijson.items(file_handle, "item"))

		if is_started_at_next_index:
			# burn through previous entity json records
			for _ in range(start_index):
				next(iterable)
			found_entity_index = page_criteria.get_first_valid_entity_index()
		else:
			found_entity_index = 0

		is_last_valid_entry_found = False
		entity_json_index = -1
		for entity_json_index, entity_json in iterable:
			entity = Entity.parse_json(
				json_dict=entity_json
			)
			if search_criteria.is_valid(
				entity=entity
			):
				if page_criteria.is_valid(
					entity_index=found_entity_index
				):
					entities.append(entity)
					if page_criteria.is_last_valid_entity_index(
						entity_index=found_entity_index
					):
						is_last_valid_entry_found = True

				found_entity_index += 1

			if is_last_valid_entry_found:
				break

		if self.__next_index_cache is not None:
			next_redis_key = search_criteria.get_redis_key() + page_criteria.get_next_redis_key()
			self.__next_index_cache.set(next_redis_key, entity_json_index + 1)
			if self.__redis_config.get_expire_seconds() is not None:
				self.__next_index_cache.expire(next_redis_key, self.__redis_config.get_expire_seconds())

		return entities

	def search(self, *, search_criteria: SearchCriteria, page_criteria: PageCriteria) -> List[Entity]:
		redis_key = search_criteria.get_redis_key() + page_criteria.get_current_redis_key()

		if self.__next_index_cache is not None and self.__next_index_cache.exists(redis_key):
			next_index = int(self.__next_index_cache.get(redis_key).decode())
			if next_index is None:
				next_index = 0
				is_started_at_next_index = False
			else:
				is_started_at_next_index = True
		else:
			next_index = 0
			is_started_at_next_index = False

		if self.__json_file_path.endswith(".bz2"):
			with bz2.open(self.__json_file_path, "rb") as file_handle:
				entities = self.__search_file_handle(
					file_handle=file_handle,
					start_index=next_index,
					is_started_at_next_index=is_started_at_next_index,
					search_criteria=search_criteria,
					page_criteria=page_criteria
				)
		elif self.__json_file_path.endswith(".gz"):
			with gzip.open(self.__json_file_path, "rb") as file_handle:
				entities = self.__search_file_handle(
					file_handle=file_handle,
					start_index=next_index,
					is_started_at_next_index=is_started_at_next_index,
					search_criteria=search_criteria,
					page_criteria=page_criteria
				)
		elif self.__json_file_path.endswith(".json"):
			with open(self.__json_file_path, "rb") as file_handle:
				entities = self.__search_file_handle(
					file_handle=file_handle,
					start_index=next_index,
					is_started_at_next_index=is_started_at_next_index,
					search_criteria=search_criteria,
					page_criteria=page_criteria
				)
		else:
			raise NotImplementedError(f"Unable to parse file type: {self.__json_file_path}")

		return entities
