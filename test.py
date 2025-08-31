print([x for x in dir() if x.startswith('_')])
print([x for x in range(10)])

def test_generator():
    yield from range(5)
print([x for x in test_generator()])

g=test_generator


c=range(20,30)
print([x for x in c if x % 2 == 0])



def mults(func):
    def wrapper(*args, **kwargs):
        print("Before calling the function")
        result = func(*args, **kwargs)
        print("After calling the function")
        return result
    return wrapper

@mults
def my_function(x,y):
    print(f"Function called with argument: {x}")
    return x * 2 * y 

result = my_function(5, 4)
print(f"Result: {result}")

dict1 = dict(a=1, b=2, jano=3)
print(dict1)
print(dict1['a'])
print(dict1.get('b', 'Not Found'))
print(dict1.get('d', 'Not Found'))
