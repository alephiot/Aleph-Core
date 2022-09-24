class ReportByExceptionHelper:
    __past_data__ = {}

    def compare(self, data: list[dict]):
        new_data = []
        for record in data:
            new_record = self.compare_record(record)
            if new_record is not None:
                new_data.append(new_record)
        return new_data

    def compare_record(self,  record: dict):
        id_ = record.get("id_")
        previous_record = self.__past_data__.get(id_)

        if previous_record is None:
            new_record = record
            self.__past_data__[id_] = {}
        else:
            new_record = {
                r: record[r] for r in record if
                r == "id_"
                or r not in previous_record
                or record[r] != previous_record[r]
            }

        self.__past_data__[id_].update(record)

        if self.record_is_not_empty(new_record):
            return new_record
        return None

    @staticmethod
    def record_is_not_empty(record: dict):
        keys_ = set(record.keys())
        keys_.discard("id_")
        keys_.discard("t")
        return len(keys_) > 0
