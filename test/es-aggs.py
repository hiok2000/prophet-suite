import importlib

def call(module_name, function_name, args, kwargs):
    mod = importlib.import_module(module_name)
    func = getattr(mod, function_name)

    return func(*args, **kwargs)

if __name__ == "__main__":
    result = call(
        'lib.reader',
        'aggs_avg_duration_by_ts',
        [
            '192.168.0.21',
            '9900',
            'mobile-mbank-log-2018.01',
            "2018-01-29T12:00:00",
            "2018-01-29T12:59:00"
        ],
        { }
    )

    print(result)
