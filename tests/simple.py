
import json
import multicorn
import pymongo
import unittest

import mongoose_fdw

def make_options(db, collection, pipe=None):
    options = { "db" : db,
                "collection" : collection,
                "host" : "127.0.0.1",
                "port" : 27017 }

    if pipe:
        options["pipe"] = pipe

    #user
    #pass
    #auth_db

    return options


class MyTest(unittest.TestCase):

    def columns(self):
        c = {}
        c["a"] = multicorn.ColumnDefinition("a", 1, 1, "numeric", "numeric", {})
        c["b"] = multicorn.ColumnDefinition("b", 1, 1, "numeric", "numeric", {})
        return c

    def column_list(self):
        return ["a", "b"]

    def count(self, fdw, pred={}):
        num = 0
        for doc in fdw.execute(pred, self.column_list()):
            num += 1
        return num

    def makeQual(self, op, left, right):
        return multicorn.Qual(op, left, right)

    def runCheck(self,fdw):
        self.assertEquals(5, self.count(fdw, {}))

        self.assertEquals(1, self.count(fdw, [multicorn.Qual("a", "=", 5)]))
        self.assertEquals(1, self.count(fdw, [multicorn.Qual("a", "=", 8)]))

        self.assertEquals(2, self.count(fdw, [multicorn.Qual("a", ">", 7)]))
        self.assertEquals(3, self.count(fdw, [multicorn.Qual("a", ">=", 7)]))
        self.assertEquals(2, self.count(fdw, [multicorn.Qual("a", "<", 7)]))
        self.assertEquals(3, self.count(fdw, [multicorn.Qual("a", "<=", 7)]))

        for one in fdw.execute([multicorn.Qual("a", "=", 5)], self.column_list()):
            self.assertEquals(one, {"a":5, "b":3})

        for one in fdw.execute([multicorn.Qual("a", "=", 5)], ["a"]):
            self.assertEquals(one, {"a":5})

class JustCollection(MyTest):
    def test(self):
        options = make_options("fdw", "foo1")

        db = pymongo.Connection(options["host"], options["port"])[options["db"]]
        collection = db[options["collection"]]

        fdw = mongoose_fdw.Mongoose_fdw(options, self.columns())

        db.drop_collection(options["collection"])
        for x in xrange(5,10):
            collection.insert({ "a" : x, "b" : 3})
        self.assertEquals(5, collection.count())

        self.runCheck(fdw)

class Pipeline1(MyTest):
    def test(self):
        options = make_options("fdw",
                               "foo1",
                               json.dumps([{"$unwind":"$a"}]))

        db = pymongo.Connection(options["host"], options["port"])[options["db"]]
        collection = db[options["collection"]]

        fdw = mongoose_fdw.Mongoose_fdw(options, self.columns())

        db.drop_collection(options["collection"])
        collection.insert( { "a" : 5, "b" : 3 } )
        collection.insert( { "a" : [6,7,8,9], "b" : 3 } )
        self.assertEquals(2, collection.count())

        self.runCheck(fdw)



