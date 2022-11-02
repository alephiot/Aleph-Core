from typing import Dict
from aleph_core.utils.typing import Data


class Transforms:
    """
    Collection of static methods that transform Data -> Data
    """

    @staticmethod
    def rename_fields(data: Data, mapping: Dict[str, str], defaults: Dict = None) -> Data:
        if defaults is None:
            defaults = {}

        result = []
        for record in data:
            new_record = {}
            for field in mapping:
                if field in record:
                    new_record[field] = record.get(field)
                else:
                    new_record[field] = defaults.get(field, None)

            result.append(record)

        return result
