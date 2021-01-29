def make_pretty(func):
    def inner(call):
        print("I got decorated")
        call='greate' +call

        func(call)
    return inner


@make_pretty
def ordinary(call):

    
    print(call)


ordinary('sumesh')




import math 
math.fsum(  )





print('hello')
