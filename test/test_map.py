# -*- coding: utf-8 -*-
import datetime
import unittest
import datetime as dt

from pyclickhouse import Connection

class TestMap(unittest.TestCase):
    def test_formatter(self):
        from pyclickhouse.Cursor import TabSeparatedWithNamesAndTypesFormatter
        formatter = TabSeparatedWithNamesAndTypesFormatter()
        r = formatter.clickhousetypefrompython({'abc': 555, 'nope': -1}, 'values')
        assert r == 'Map(String, Int64)'
        r = formatter.clickhousetypefrompython({'ts': datetime.datetime.now()}, 'values')
        assert r == 'Map(String, DateTime)'

    def test_db(self):
        conn = Connection('localhost:8124')
        conn.open()
        cur = conn.cursor()

        cur.ddl('drop table if exists moo')
        cur.ddl('create table moo(valuemap Map(String,Float64)) Engine=Log')
        cur.insert("insert into moo values (map('prop_1', 0.2, 'prop_2', 3.14))")
        cur.bulkinsert('moo', [{'valuemap': {'prop_1': 100.0, 'prop_2': -1.0}}])
        cur.select("select sum(valuemap['prop_1']) as val from moo")
        r = cur.fetchone()
        assert r['val'] == 100.2
        cur.select("select valuemap from moo order by valuemap['prop_2']")
        r = cur.fetchall()
        assert r[0]['valuemap']['prop_1'] == 100.0
        assert r[0]['valuemap']['prop_2'] == -1.0
        assert r[1]['valuemap']['prop_1'] == 0.2
        assert r[1]['valuemap']['prop_2'] == 3.14

        cur.ddl('drop table if exists moo')
        cur.ddl('create table moo(valuemap Map(String, DateTime)) Engine=Log')
        cur.insert("insert into moo values (map('ts', '2022-01-02 00:00:34'))")
        cur.bulkinsert('moo', [{'valuemap': {'ts': dt.datetime(2023,1,1,0,0,21)}}])
        cur.select("select valuemap from moo order by valuemap['ts']")
        r = cur.fetchall()
        assert r[0]['valuemap']['ts'] == dt.datetime(2022,1,2,0,0,34)
        assert r[1]['valuemap']['ts'] == dt.datetime(2023,1,1,0,0,21)

        cur.ddl('drop table if exists moo')
        cur.ddl('create table moo(valuemap Map(String, Date)) Engine=Log')
        cur.insert("insert into moo values (map('ts', '2022-01-02'))")
        cur.bulkinsert('moo', [{'valuemap': {'ts': dt.date(2023,1,1)}}])
        cur.select("select valuemap from moo order by valuemap['ts']")
        r = cur.fetchall()
        assert r[0]['valuemap']['ts'] == dt.date(2022,1,2)
        assert r[1]['valuemap']['ts'] == dt.date(2023,1,1)
