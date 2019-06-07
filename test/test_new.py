# coding=utf-8
import unittest
import pyclickhouse
import datetime as dt
from pyclickhouse.formatter import TabSeparatedWithNamesAndTypesFormatter

class TestNewUnitTests(unittest.TestCase):
    """Test compatibility of insert operations with Unicode text"""

    def setUp(self):
        self.conn = pyclickhouse.Connection('localhost:8123')
        self.cursor=self.conn.cursor()

    def test_array_serialization(self):
        self.cursor.select("select ['abc','def'] as t")
        result = self.cursor.fetchone()['t']
        assert result[0] == 'abc'
        assert result[1] == 'def'

    def test_unformat_of_commas(self):
        formatter = TabSeparatedWithNamesAndTypesFormatter()
        formatter.unformatfield("['abc',,'def']", 'Array(String)')  # boom

    def test_store_doc(self):
        doc = {'id': 3, 'historydate': dt.date(2019,6,7), 'Offer': {'price': 5, 'count': 1}, 'Images': [{'file': 'a', 'size': 400}, {'file': 'b', 'size': 500}]}
        self.cursor.ddl('drop table if exists docs')
        self.cursor.ddl('create table if not exists docs (historydate Date, id Int64) Engine=MergeTree(historydate, id, 8192)')
        self.cursor.store_documents('docs', [doc])
        self.cursor.select('select * from docs')
        r = self.cursor.fetchone()
        assert str(r) == "{'Images.file': ['a', 'b'], 'Images.size': [400, 500], 'Offer.count': 1, 'Offer.price': 5, 'id': 3, 'historydate': datetime.date(2019, 6, 7)}"

    def test_store_doc2(self):
        doc = {'id': 3, 'Offer': {'price': 5, 'count': 1}, 'Images': [{'file': 'a', 'size': 400, 'tags': ['cool','Nikon']}, {'file': 'b', 'size': 500}]}
        self.cursor.ddl('drop table if exists docs')
        self.cursor.ddl('create table if not exists docs (historydate Date, id Int64) Engine=MergeTree(historydate, id, 8192)')
        self.cursor.store_documents('docs', [doc])
        self.cursor.select('select * from docs')
        r = self.cursor.fetchone()
        assert str(r) == """{'Images.json': '[{"tags":["cool","Nikon"],"file":"a","size":400},{"file":"b","size":500}]', 'Offer.price': 5, 'id': 3, 'Offer.count': 1, 'historydate': None}"""