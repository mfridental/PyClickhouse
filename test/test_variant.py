# coding=utf-8
import unittest
import pyclickhouse
import datetime as dt
from dateutil import tz
from pyclickhouse.formatter import TabSeparatedWithNamesAndTypesFormatter
import json

class TestVariant(unittest.TestCase):
    def setUp(self):
        self.conn = pyclickhouse.Connection('localhost:8124', clickhouse_settings={'allow_experimental_variant_type':
                                                                                       1})
        self.cursor=self.conn.cursor()
        self.cursor.ddl("""
        create table if not exists VariantTable (
            historydatetime DateTime,
            objid Int64,
            data Variant(Int64,String,Array(String))
        ) engine = MergeTree
        order by objid
        """)

    def test_insert(self):
        self.cursor.insert("""
        insert into VariantTable values (now(), 1, 100),(now(),2, 'twohundred'),(now(),3,['three','hundred'])
        """)

        self.cursor.select("""select count() from VariantTable""")
        cnt = self.cursor.fetchone()['count()']
        self.assertGreater(cnt, 2, msg=cnt)

    def test_select(self):
        self.cursor.insert("""
        insert into VariantTable values (now(), 1, 100),(now(),2, 'twohundred'),(now(),3,['three','hundred'])
        """)

        self.cursor.select("""select * from VariantTable""")
        r = self.cursor.fetchall()
        #print(r)
        assert len(r) >= 3

    def test_unformat(self):
        fmt = self.cursor.formatter

        type = 'Variant(Int64, String)'
        r = fmt.unformatfield(42, type)
        self.assertEqual(r, 42)
        r = fmt.unformatfield('42 years', type)
        self.assertEqual(r, '42 years')
        r = fmt.unformatfield('2024-01-01 00:00:00', type)
        self.assertEqual(r, '2024-01-01 00:00:00')

        type = 'Variant(String, DateTime)'
        r = fmt.unformatfield('42 years', type)
        self.assertEqual(r, '42 years')
        r = fmt.unformatfield('2024-01-01 00:00:00', type)
        self.assertEqual(r, dt.datetime(2024,1,1,0,0,0))

        type = "Variant(Int64, String, DateTime('UTC'))"
        r = fmt.unformatfield(42, type)
        self.assertEqual(r, 42)
        r = fmt.unformatfield('42 years', type)
        self.assertEqual(r, '42 years')
        r = fmt.unformatfield('2024-01-01 00:00:00', type)
        self.assertEqual(r, dt.datetime(2024,1,1,0,0,0,tzinfo=tz.UTC))

        type = 'Variant(Int64, String, Array(Int64),Array(String))'
        r = fmt.unformatfield(42, type)
        self.assertEqual(r, 42)
        r = fmt.unformatfield('42 years', type)
        self.assertEqual(r, '42 years')
        r = fmt.unformatfield('[1,2,3]', type)
        self.assertEqual(r, [1,2,3])
        r = fmt.unformatfield("['one', 'two']", type)
        self.assertEqual(r, ['one', 'two'])

        type = 'Variant(String, Array(String), Map(String,Float64), Array(Map(String,Date)))'
        r = fmt.unformatfield('abc', type)
        self.assertEqual(r, 'abc')
        r = fmt.unformatfield("['one', 'two']", type)
        self.assertEqual(r, ['one', 'two'])
        r = fmt.unformatfield("{'pi': 3.14, 'e': 2.7}", type)
        self.assertEqual(r, {'pi': 3.14, 'e': 2.7})
        r = fmt.unformatfield("[{'begin':'2023-01-01'},{'end': '2024-01-01'}]", type)
        self.assertEqual(r, [{'begin':'2023-01-01'},{'end': '2024-01-01'}])

        type = "Variant(String, Variant(Int64, DateTime('UTC')))"
        r = fmt.unformatfield('abc', type)
        self.assertEqual(r, 'abc')
        r = fmt.unformatfield('42 years', type)
        self.assertEqual(r, '42 years')
        r = fmt.unformatfield('2024-01-01 00:00:00', type)
        self.assertEqual(r, dt.datetime(2024,1,1,0,0,0,tzinfo=tz.UTC))

    def test_format(self):
        fmt = self.cursor.formatter

        type = "Variant(Int64, String, DateTime('UTC'))"
        fmt.formatfield(123, type, 'field')
        self.assertRaises(Exception, lambda: fmt.formatfield(123.56, type, 'field'))

    def test_variant_detection(self):
        fmt = self.cursor.formatter
        values = [
            {'varfield': 123},
            {'varfield': dt.datetime.now()},
            {'varfield': 'not an error'},
        ]
        fields, types, payload = fmt.format(values, None, None)
        self.assertEqual(['Int64'], types)
        fmt.use_variant_for_generalization = True
        fields, types, payload = fmt.format(values, None, None)
        self.assertEqual(['Variant(DateTime, Int64, String)'], types)


    def test_bulk(self):
        self.cursor.select("""select count() as cnt from VariantTable""")
        before = self.cursor.fetchone()['cnt']

        data = [{'historydatetime': dt.datetime.now(), 'objid': 1001, 'data': 123}]
        fields = ['historydatetime', 'objid', 'data']
        types = ['DateTime', 'Int64', 'Variant(Array(String), Int64, String)']
        self.cursor.bulkinsert('VariantTable', data, fields, types)

        self.cursor.select("""select count() as cnt from VariantTable""")
        after = self.cursor.fetchone()['cnt']

        self.assertEqual(after, before + 1)

        data = [
            {'historydatetime': dt.datetime.now(), 'objid': 1001, 'data': 123},
            {'historydatetime': dt.datetime.now(), 'objid': 1002, 'data': "text"},
            {'historydatetime': dt.datetime.now(), 'objid': 1003, 'data': ["word", "word2"]}
        ]
        fields = ['historydatetime', 'objid', 'data']
        types = ['DateTime', 'Int64', 'Variant(Array(String), Int64, String)']
        self.cursor.bulkinsert('VariantTable', data, fields, types)

        self.cursor.select("""select count() as cnt from VariantTable""")
        after2 = self.cursor.fetchone()['cnt']

        self.assertEqual(after2, after + 3)

        data = [{'historydatetime': dt.datetime.now(), 'objid': 1001, 'data': dt.datetime.now()}]
        fields = ['historydatetime', 'objid', 'data']
        types = ['DateTime', 'Int64', 'Variant(Array(String), Int64, String)']
        self.assertRaises(Exception, lambda: self.cursor.bulkinsert('VariantTable', data, fields, types))

        data = [
            {'historydatetime': dt.datetime.now(), 'objid': 1001, 'data': 123},
            {'historydatetime': dt.datetime.now(), 'objid': 1002, 'data': "text"},
            {'historydatetime': dt.datetime.now(), 'objid': 1003, 'data': ["word", "word2"]}
        ]
        self.cursor.formatter.use_variant_for_generalization = True
        self.cursor.bulkinsert('VariantTable', data)

    def test_generalization(self):
        fmt = self.cursor.formatter

        a = 'Array(String)'
        b = 'Int64'
        fmt.use_variant_for_generalization = False
        self.assertRaises(Exception, lambda: fmt.generalize_type(a,b))
        fmt.use_variant_for_generalization = True
        self.assertEqual('Variant(Array(String), Int64)', fmt.generalize_type(a,b))

        a = 'Map(String, Int64)'
        b = 'Int64'
        fmt.use_variant_for_generalization = False
        self.assertRaises(Exception, lambda: fmt.generalize_type(a,b))
        fmt.use_variant_for_generalization = True
        self.assertEqual('Variant(Int64, Map(String, Int64))', fmt.generalize_type(a,b))

        a = 'DateTime'
        b = 'Int64'
        fmt.use_variant_for_generalization = False
        self.assertEqual('String', fmt.generalize_type(a,b))
        fmt.use_variant_for_generalization = True
        self.assertEqual('Variant(DateTime, Int64)', fmt.generalize_type(a,b))

        a = 'Variant(DateTime, String)'
        b = 'Variant(Int64, IPv4)'
        fmt.use_variant_for_generalization = False
        self.assertEqual('String', fmt.generalize_type(a,b))
        fmt.use_variant_for_generalization = True
        self.assertEqual('Variant(DateTime, IPv4, Int64, String)', fmt.generalize_type(a,b))

        a = 'Variant(DateTime, String)'
        b = 'Variant(String, Float64)'
        fmt.use_variant_for_generalization = False
        self.assertEqual('String', fmt.generalize_type(a,b))
        fmt.use_variant_for_generalization = True
        self.assertEqual('Variant(DateTime, Float64, String)', fmt.generalize_type(a,b))

    def test_is_compatible_type(self):
        fmt = self.cursor.formatter

        a = 'Array(String)'
        b = 'Int64'
        fmt.use_variant_for_generalization = False
        self.assertEqual(False, fmt.is_compatible_type(a, b))
        fmt.use_variant_for_generalization = True
        self.assertEqual(True, fmt.is_compatible_type(a, b))

if __name__ == '__main__':
    unittest.main(__name__)

