import random


class JSONGenerator:
    @staticmethod
    def generate_epoch():
        return {
            "id_epoch": random.randint(0, 100),
            "known_data": random.choice([True, False]),
            "cycle_count": random.randint(5, 100),
            "MFE_trunc": random.uniform(0, 1),
            "norm_MFE": random.uniform(0, 1),
            "MAE_trunc": random.uniform(0, 1),
            "norm_MAE": random.uniform(0, 1),
            "AIR_trunc": random.uniform(0, 1),
            "norm_AIR": random.uniform(0, 1),
            "HitRate": random.uniform(0, 1),
            "norm_Hit": random.uniform(0, 1),
            "median(duration) cycle": random.uniform(0, 1),
            "time_to_MFE(median)": random.randint(5, 100),
            "time_to_MAE(median)": random.randint(5, 100),
            "Intermediate score(median)": random.uniform(0, 1),
        }

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