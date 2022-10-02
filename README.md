# WikiDataParser
This project allows for easy retrieval of data from the WikiData JSON download via Python. Simply download the JSON file, create an instance of WikiDataParser, and perform searches directly or create an iterator for convenient looping.

The WikiData JSON file can be downloaded as described from the official site at https://www.wikidata.org/wiki/Wikidata:Database_download. Extracting the compressed file is **not** a requirement.

## Features

- Reads one entity at a time from the massive file, reducing memory requirements
- Maintains file handles per search criteria to enhance sequential pagination

## Usage

_Search for the first 10 entities that have the text "apple" in the title_
```python
from austin_heller_repo.wiki_data_parser import WikiDataParser, SearchCriteria, PageCriteria, SetComplimentTypeEnum
wiki_data_parser = WikiDataParser(
    json_file_path="/path/to/download/file.json.gz"
)
entities = wiki_data_parser.search(
    search_criteria=SearchCriteria(
        entity_types=[],
        entity_types_set_compliment_type=SetComplimentTypeEnum.Exclusive,
        id=None,
        label_parts=["apple"],
        description_parts=None
    ),
    page_criteria=PageCriteria(
        page_index=0,
        page_size=10
    )
)
```
The WikiDataParser will internally parse through the entity JSON objects, comparing them to the search criteria and stopping once the page criteria is satisfied.

_Iterate over every entity until custom condition discovered_
```python
from austin_heller_repo.wiki_data_parser import WikiDataParser, SetComplimentTypeEnum, WikiDataParserIterator
for entity in WikiDataParserIterator(
    wiki_data_parser=WikiDataParser(
        json_file_path="/path/to/download/file.json.gz"
    ),
    search_criteria=None
):
    if len(entity.get_claims()) == 5:
        break
```
This code will iterate over all entities (since search_criteria is None) and will break out of the loop once it discovers an entity with exactly five claims.
