import timeit
from mojofs.filemeta.filemeta import FileMeta
from mojofs.filemeta import test_data

def bench_create_real_xlmeta():
    def create():
        test_data.create_real_xlmeta()
    t = timeit.timeit(create, number=1000)
    print(f"create_real_xlmeta: {t:.6f}s")

def bench_create_complex_xlmeta():
    def create():
        test_data.create_complex_xlmeta()
    t = timeit.timeit(create, number=1000)
    print(f"create_complex_xlmeta: {t:.6f}s")

def bench_parse_real_xlmeta():
    data = test_data.create_real_xlmeta()
    def parse():
        FileMeta.load(data)
    t = timeit.timeit(parse, number=1000)
    print(f"parse_real_xlmeta: {t:.6f}s")

def bench_parse_complex_xlmeta():
    data = test_data.create_complex_xlmeta()
    def parse():
        FileMeta.load(data)
    t = timeit.timeit(parse, number=1000)
    print(f"parse_complex_xlmeta: {t:.6f}s")

def bench_serialize_real_xlmeta():
    data = test_data.create_real_xlmeta()
    fm = FileMeta.load(data)
    def serialize():
        fm.marshal_msg()
    t = timeit.timeit(serialize, number=1000)
    print(f"serialize_real_xlmeta: {t:.6f}s")

def bench_serialize_complex_xlmeta():
    data = test_data.create_complex_xlmeta()
    fm = FileMeta.load(data)
    def serialize():
        fm.marshal_msg()
    t = timeit.timeit(serialize, number=1000)
    print(f"serialize_complex_xlmeta: {t:.6f}s")

def bench_round_trip_real_xlmeta():
    original_data = test_data.create_real_xlmeta()
    def round_trip():
        fm = FileMeta.load(original_data)
        serialized = fm.marshal_msg()
        FileMeta.load(serialized)
    t = timeit.timeit(round_trip, number=1000)
    print(f"round_trip_real_xlmeta: {t:.6f}s")

def bench_round_trip_complex_xlmeta():
    original_data = test_data.create_complex_xlmeta()
    def round_trip():
        fm = FileMeta.load(original_data)
        serialized = fm.marshal_msg()
        FileMeta.load(serialized)
    t = timeit.timeit(round_trip, number=1000)
    print(f"round_trip_complex_xlmeta: {t:.6f}s")

def bench_version_stats():
    data = test_data.create_complex_xlmeta()
    fm = FileMeta.load(data)
    def stats():
        fm.get_version_stats()
    t = timeit.timeit(stats, number=1000)
    print(f"version_stats: {t:.6f}s")

def bench_validate_integrity():
    data = test_data.create_real_xlmeta()
    fm = FileMeta.load(data)
    def validate():
        fm.validate_integrity()
    t = timeit.timeit(validate, number=1000)
    print(f"validate_integrity: {t:.6f}s")

def main():
    bench_create_real_xlmeta()
    bench_create_complex_xlmeta()
    bench_parse_real_xlmeta()
    bench_parse_complex_xlmeta()
    bench_serialize_real_xlmeta()
    bench_serialize_complex_xlmeta()
    bench_round_trip_real_xlmeta()
    bench_round_trip_complex_xlmeta()
    bench_version_stats()
    bench_validate_integrity()

if __name__ == "__main__":
    main()