import random


class JSONGenerator:
    COLUMNS = [f"column_{i}" for i in range(21)]

    def generate_epoch(self):
        return {column: random.uniform(0, 1) for column in self.COLUMNS}


    def batch_generate_epochs(self, epoch_in_record):
        return {
            f"epoch_{i}": self.generate_epoch() for i in range(epoch_in_record)
        }


    def batch_generate_records(self, *, record_count: int = 1, epoch_in_record: int = 1):
        return [
            self.batch_generate_epochs(epoch_in_record) for _ in range(record_count)
        ]


if __name__ == '__main__':
    gen = JSONGenerator()
    some_kson = gen.batch_generate_records(record_count=1, epoch_in_record=1_000_000)