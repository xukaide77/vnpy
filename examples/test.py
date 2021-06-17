# class Singleton(object):
#     def __new__(cls):
#     # 关键在于这，每一次实例化的时候，我们都只会返回这同一个 instance 对象
#       if not hasattr(cls, 'instance'):
#         cls.instance = super().__new__(cls)
#       return cls.instance
#
# obj1 = Singleton()
# obj2 = Singleton()
#
# obj1.attr1 = 'value1'
# print(obj1.attr1, obj2.attr1)
# print(obj1 is obj2)

# def singleton(cls):
#   instances = {}
#   def getinstance(*args,**kwargs):
#     if cls not in instances:
#       instances[cls] = cls(*args,**kwargs)
#     return instances[cls]
#   return getinstance
#
# @singleton
# class MyClass:
#     a = 1
#
# c1 = MyClass()
# c2 = MyClass()
# print(c1 is c2) # True


# class CtaTemplate:
#   """CTA策略模板"""
#
#   author = ""
#   parameters = []
#   variables = None
#
#   def __init__(self):
#     self.variables = []
#     self.variables.append(1)
#
#   def app(self, x):
#     self.variables.append(x)
#
# c1 = CtaTemplate()
# c2 = CtaTemplate()
# c1.app(2)
# c2.app(3)
# print(c1.variables)
# print(c2.variables)

import json
from vnpy.trader.object import Direction

print(json.dumps(Direction.SHORT.name))
print(Direction[json.loads(json.dumps(Direction.SHORT.name))])
