from datetime import datetime

def not_during_the_night(func):
    def wrapper():
        if 0<= datetime.now().hour < 22:
            func()
        else:
            pass
     
         # Hush, the neighbors are asleep
    return wrapper

def say_whee():

    
    print("Whee!")

say_whee = not_during_the_night(say_whee)
say_whee()