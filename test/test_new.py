# coding=utf-8
import unittest
import pyclickhouse
import datetime as dt
from pyclickhouse.formatter import TabSeparatedWithNamesAndTypesFormatter
import json

class TestNewUnitTests(unittest.TestCase):
    """Test compatibility of insert operations with Unicode text"""

    def setUp(self):
        self.conn = pyclickhouse.Connection('localhost:8124')
        self.cursor=self.conn.cursor()

    def test_array_serialization(self):
        self.cursor.select("select ['abc','def'] as t")
        result = self.cursor.fetchone()['t']
        assert result[0] == 'abc'
        assert result[1] == 'def'

    def test_nullables(self):
        self.cursor.ddl('drop table if exists NullTest')

        self.cursor.ddl("""
        create table NullTest (
            key Int64,
            f1 Nullable(String),
            f2 Nullable(Int64),
            f3 Array(Nullable(String)),
            f4 Array(Nullable(Int64))
        ) Engine = MergeTree order by key
        """)

        data = [{'key': 1, 'f1': None, 'f2': None, 'f3': ['pre', None, 'post'], 'f4': [None, 3]}]
        self.cursor.bulkinsert('NullTest', data, ['key','f1','f2','f3','f4'],['Int64', 'Nullable(String)',
                                                                              'Nullable(Int64)','Array(Nullable('
                                                                                                'String))',
                                                                              'Array(Nullable(Int64))'])
        self.cursor.select('select * from NullTest')
        r = self.cursor.fetchall()
        print(r)
        assert data == r

    def test_u64_serializaton(self):
        self.cursor.ddl('create table t64 (ts DateTime64(3)) Engine=MergeTree order by ts')
        self.cursor.bulkinsert('t64', [{'ts': dt.datetime.now()}], ['ts'], ['DateTime64(3)'])
        self.cursor.select("""select ts from t64""")
        assert 'ts' in self.cursor.fetchone()

    def test_unformat_of_commas(self):
        formatter = TabSeparatedWithNamesAndTypesFormatter()
        formatter.unformatfield("['abc',,'def']", 'Array(String)')  # boom

    def test_store_doc(self):
        doc = {'id': 3, 'historydate': dt.date(2019,6,7), 'Offer': {'price': 5, 'count': 1}, 'Images': [{'file': 'a', 'size': 400}, {'file': 'b', 'size': 500}]}
        self.cursor.formatter.enable_map_datatype = False
        self.cursor.ddl('drop table if exists docs')
        self.cursor.ddl('create table if not exists docs (historydate Date, id Int64) Engine=MergeTree(historydate, id, 8192)')
        self.cursor.store_documents('docs', [doc])
        self.cursor.select('select * from docs')
        r = self.cursor.fetchone()
        assert r['Images_file'] == ['a', 'b']
        assert r['Images_size'] == [400, 500]
        assert r['Offer_count'] == 1
        assert r['Offer_price'] == 5
        assert r['id'] == 3
        assert r['historydate'] == dt.date(2019, 6, 7)
        documents = self.cursor.retrieve_documents("select * from docs")
        assert doc == documents[0]

    def test_store_doc2(self):
        doc = {'id': 3, 'historydate': dt.date(2019,6,7), 'Offer': {'price': 5, 'count': 1}, 'Images': [{'file': 'a', 'size': 400, 'tags': ['cool','Nikon']}, {'file': 'b', 'size': 500}]}
        self.cursor.formatter.enable_map_datatype = False
        self.cursor.ddl('drop table if exists docs')
        self.cursor.ddl('create table if not exists docs (historydate Date, id Int64) Engine=MergeTree(historydate, id, 8192)')
        self.cursor.store_documents('docs', [doc])
        self.cursor.select('select * from docs')
        r = self.cursor.fetchone()
        assert 'Images_json' in r
        documents = self.cursor.retrieve_documents("select * from docs")
        assert doc == documents[0]

    def test_dict_flattening(self):
        doc = {'id': 3}
        bulk, map = pyclickhouse.Cursor._flatten_dict(doc)
        assert bulk == doc
        assert map == {'id':'id=scalar'}

        doc = {'id': 3, 'sub': {'dict': True}}
        bulk, map = pyclickhouse.Cursor._flatten_dict(doc)
        assert bulk == {'id': 3, 'sub_dict': True}
        assert map == {'id':'id=scalar', 'sub_dict': 'sub=dict&dict=scalar'}

        doc = {'id': 3, 'sub': {'dict': True, 'sub_sub': {'a': 1}}}
        bulk, map = pyclickhouse.Cursor._flatten_dict(doc)
        assert bulk == {'id': 3, 'sub_dict': True, 'sub_sub_sub_a': 1}
        assert map == {'id':'id=scalar', 'sub_dict': 'sub=dict&dict=scalar', 'sub_sub_sub_a': 'sub=dict&sub_sub=dict&a=scalar'}

        doc = {'id': 3, 'sub': ['array', 'abc']}
        bulk, map = pyclickhouse.Cursor._flatten_dict(doc)
        assert bulk == doc
        assert map == {'id':'id=scalar', 'sub': 'sub=array'}

        doc = {'id': 3, 'sub': [{'dict': 'in_array'}, {'dict': 'in_array_also', 'otherkey': True}]}
        bulk, map = pyclickhouse.Cursor._flatten_dict(doc)
        assert bulk == {'id': 3, 'sub_dict': ['in_array', 'in_array_also'], 'sub_otherkey': [None, True]}
        assert map == {'id':'id=scalar', 'sub_dict': 'sub=array&dict=scalar', 'sub_otherkey': 'sub=array&otherkey=scalar'}

        doc = {'id': 3, 'sub': {'array': ['in_dict', 'second']}}
        bulk, map = pyclickhouse.Cursor._flatten_dict(doc)
        assert bulk == {'id': 3, 'sub_array': ['in_dict', 'second']}
        assert map == {'id':'id=scalar', 'sub_array': 'sub=dict&array=array'}

        doc = {'id': 3, 'sub': [{'dict': 'in_array', 'needs': ['json', 'too_much_nesting']}]}
        bulk, map = pyclickhouse.Cursor._flatten_dict(doc)
        assert bulk['id'] == 3
        sub = json.loads(bulk['sub_json'])[0]
        assert sub['dict'] == 'in_array'
        assert sub['needs'] == ['json','too_much_nesting']
        assert map == {'id':'id=scalar', 'sub_json': 'sub=json'}


    def test_dict_unflattening(self):
        doc = {'id': 3}
        bulk = pyclickhouse.Cursor._unflatten_dict(doc,{'id':'id=scalar'})
        assert bulk == doc

        doc = {'id': 3, 'sub_dict': True}
        bulk = pyclickhouse.Cursor._unflatten_dict(doc, {'id':'id=scalar', 'sub_dict': 'sub=dict&dict=scalar'})
        assert bulk == {'id': 3, 'sub': {'dict': True}}

        doc = {'id': 3, 'sub_dict': True, 'sub_sub_sub_a': 1}
        bulk = pyclickhouse.Cursor._unflatten_dict(doc, {'id':'id=scalar', 'sub_dict': 'sub=dict&dict=scalar', 'sub_sub_sub_a': 'sub=dict&sub_sub=dict&a=scalar'})
        assert bulk == {'id': 3, 'sub': {'dict': True, 'sub_sub': {'a': 1}}}

        doc = {'id': 3, 'sub': ['array', 'abc']}
        bulk = pyclickhouse.Cursor._unflatten_dict(doc, {'id':'id=scalar', 'sub': 'sub=array'})
        assert bulk == doc

        doc = {'id': 3, 'sub_dict': ['in_array', 'in_array_also'], 'sub_otherkey': [None, True]}
        bulk = pyclickhouse.Cursor._unflatten_dict(doc, {'id':'id=scalar', 'sub_dict': 'sub=array&dict=scalar', 'sub_otherkey': 'sub=array&otherkey=scalar'})
        assert bulk == {'id': 3, 'sub': [{'dict': 'in_array'}, {'dict': 'in_array_also', 'otherkey': True}]}

        doc = {'id': 3, 'sub_array': ['in_dict', 'second']}
        bulk = pyclickhouse.Cursor._unflatten_dict(doc, {'id':'id=scalar', 'sub_array': 'sub=dict&array=array'})
        assert bulk == {'id': 3, 'sub': {'array': ['in_dict', 'second']}}

        doc = {'id': 3, 'sub_json': '[{"needs":["json","too_much_nesting"],"dict":"in_array"}]'}
        bulk = pyclickhouse.Cursor._unflatten_dict(doc, {'id':'id=scalar', 'sub_json': 'sub=json'})
        assert bulk == {'id': 3, 'sub': [{'dict': 'in_array', 'needs': ['json', 'too_much_nesting']}]}



    def test_type_generalization(self):
        types = ['Int8', 'Int16', 'Int32', 'Int64', 'Float32', 'Float64', 'Date', 'DateTime']
        results = ['Int8',
        'Int16',
        'Int32',
        'Int64',
        'Float32',
        'Float64',
        'String',
        'String',
        'Int16',
        'Int16',
        'Int32',
        'Int64',
        'Float32',
        'Float64',
        'String',
        'String',
        'Int32',
        'Int32',
        'Int32',
        'Int64',
        'Float32',
        'Float64',
        'String',
        'String',
        'Int64',
        'Int64',
        'Int64',
        'Int64',
        'Float32',
        'Float64',
        'String',
        'String',
        'Float32',
        'Float32',
        'Float32',
        'Float32',
        'Float32',
        'Float64',
        'String',
        'String',
        'Float64',
        'Float64',
        'Float64',
        'Float64',
        'Float64',
        'Float64',
        'String',
        'String',
        'String',
        'String',
        'String',
        'String',
        'String',
        'String',
        'Date',
        'DateTime',
        'String',
        'String',
        'String',
        'String',
        'String',
        'String',
        'DateTime',
        'DateTime']
        i = 0
        for left in types:
            for right in types:
                result = self.cursor.formatter.generalize_type(left, right)
                assert result == results[i]

                result = self.cursor.formatter.generalize_type('Array(%s)' % left, 'Array(%s)' % right)
                assert result == 'Array(%s)' % results[i]

                i += 1

    def test_array_generalization(self):
        result = self.cursor.formatter.clickhousetypefrompython([1, 2], 'test')
        assert result == 'Array(Int64)'

        result = self.cursor.formatter.clickhousetypefrompython([0.1, 0.2], 'test')
        assert result == 'Array(Float64)'

        result = self.cursor.formatter.clickhousetypefrompython([1, 0.2], 'test')
        assert result == 'Array(Float64)'

        result = self.cursor.formatter.clickhousetypefrompython([1, False], 'test')
        assert result == 'Array(String)'


    def test_nullable(self):
        result = self.cursor.formatter.generalize_type('Int64', 'Nullable(Int64)')
        assert result == 'Nullable(Int64)'
        result = self.cursor.formatter.generalize_type('Float64', 'Nullable(Int64)')
        assert result == 'Nullable(Float64)'

    def test_noextend(self):
        self.cursor.ddl('drop table if exists TestNoExtend ')
        self.cursor.ddl('create table TestNoExtend(id String, historydate Date) Engine=MergeTree(historydate, id, 8192)')

        doc = {'id':'first', 'historydate':dt.date.today(), 'extra':5}
        self.cursor.store_documents('TestNoExtend', [doc])
        fields, types = self.cursor.get_schema('TestNoExtend')
        assert 'extra' in fields

        doc = {'id':'second', 'historydate':dt.date.today(), 'noextra':42}
        self.cursor.store_documents('TestNoExtend', [doc], extendtable=False)
        fields, types = self.cursor.get_schema('TestNoExtend')
        assert 'noextra' not in fields

        self.cursor.ddl('drop table if exists TestNoExtend ')

    def test_nested_arrays(self):
        self.cursor.ddl('drop table if exists NestedA ')
        self.cursor.ddl('create table NestedA (a Array(Int64), b Array(String), c Array(DateTime)) Engine=Log')
        self.cursor.insert("insert into NestedA values ([1,2,3], ['a', 'b','c'], ['2023-01-01 00:00:00'])")
        docs = [{'a': [10,20,30],
                 'b': ['strinh'],
                 'c': [dt.datetime.now(), dt.datetime.now()+dt.timedelta(seconds=3)]}]
        self.cursor.bulkinsert('NestedA', docs)
        self.cursor.select("""
        select groupArray(a) aa, groupArray(b) bb, groupArray(c) cc
        from NestedA
        """)
        r = self.cursor.fetchone()
        assert r['aa'] == [[1, 2, 3], [10, 20, 30]]
        assert r['bb'] == [['a', 'b', 'c'], ['strinh']]
        assert r['cc'][0] == [dt.datetime(2023, 1, 1, 0, 0)]
        self.cursor.select('select c from NestedA limit 1')
        r = self.cursor.fetchone()
        print(r)

    def test_datetime_nanoseconds(self):
        formatter = TabSeparatedWithNamesAndTypesFormatter()
        r = formatter.unformatfield('2023-01-01 01:02:03.123456789', 'DateTime64(9)')
        assert r == dt.datetime(2023,1,1,1,2,3,123456)