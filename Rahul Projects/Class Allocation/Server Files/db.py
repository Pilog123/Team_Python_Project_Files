# from jproperties import Properties
#
#
# with open(r'C:\Users\Administrator\PythonDeployment\Class Allocation\Server Files\isk_connectit\jython_DB.properties', 'rb') as fp:
#     print(Properties.)


from jproperties import Properties

p = Properties()
# p["foobar"] = "A very important message from our sponsors: Python is great!"

with open(r'C:\Users\Administrator\PythonDeployment\Class Allocation\Server Files\isk_connectit\jython_DB.properties', 'rb') as f:
    # p.load(f, "utf-8")
    # print(p['172.16.1.61_SERVICE'].data)
    # print(p.list(f))
    p.load(f)
    # prop_data = Properties.items(f)
    for k in p.items():
        print('---------->', k[1].data)