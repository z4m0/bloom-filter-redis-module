import redis

r = redis.Redis()

def do_tests(*tests):
    r.execute_command('flushdb')
    count = 0
    for test in tests:
        print('Starting', test.__name__)
        try:
            test()
            print('\t Test successful')
            count += 1
        except AssertionError:
            print('\tTest failed')
    print('Successful tests:', str(count)+ '/' + str(len(tests)))


# This test ensures that the simple features of a bloom filter work
def simple_test():
    key = 'test1'
    r.execute_command('bf.init',key)
    for i in range(100):
        r.execute_command('bf.add', key, str(i))
    for i in range(100):
        assert r.execute_command('bf.exists', key, str(i)) == 1
    for i in range(100, 200):
        assert r.execute_command('bf.exists', key, str(i)) == 0
    r.execute_command('del',key)

# This test checks that the amount of false positives is below the rate specified
def false_positive_test():
    key = 'test2'
    count = 0
    r.execute_command('bf.init',key, 10000, 0.1)
    for i in range(10000):
        r.execute_command('bf.add', key, str(i))
    for i in range(10000, 20000):
        if r.execute_command('bf.exists', key, str(i)) != 0:
            count += 1
    error_rate = count/(20000-10000)
    assert error_rate <= 0.1
    r.execute_command('del',key)


def merge_test():
    key1 = 'test3_1'
    key2 = 'test3_2'
    r.execute_command('bf.init', key1, 10000, 0.1, 1234)
    r.execute_command('bf.init', key2, 10000, 0.1, 1234)
    for i in range(100):
        r.execute_command('bf.add', key1, str(i))
    for i in range(100, 200):
        r.execute_command('bf.add', key2, str(i))
    r.execute_command('bf.merge',key1, key2)
    for i in range(100, 200):
        assert r.execute_command('bf.exists', key1, str(i)) == 1
    for i in range(100):
        assert r.execute_command('bf.exists', key2, str(i)) == 0
    r.execute_command('del',key1)
    r.execute_command('del',key2)


do_tests(simple_test, false_positive_test, merge_test)
