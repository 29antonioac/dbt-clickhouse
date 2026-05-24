import json
from unittest.mock import MagicMock

import pytest
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt.adapters.clickhouse.relation import ClickHouseRelation, ClickHouseRelationType


def _make_row(name, schema, type_info='table', db_engine='Atomic', mvs='[]', on_cluster=0):
    return (name, schema, type_info, db_engine, mvs, on_cluster)


@pytest.fixture
def adapter():
    a = MagicMock(spec=ClickHouseAdapter)
    a.Relation = ClickHouseRelation
    a.supports_atomic_exchange.return_value = True
    a.get_clickhouse_cluster_name.return_value = None
    a.cache = MagicMock()
    a._get_cache_schemas = MagicMock()
    a.execute_macro = MagicMock()
    return a


def test_row_to_relation_table(adapter):
    row = _make_row('orders', 'staging')
    rel = ClickHouseAdapter._row_to_relation(adapter, row)
    assert rel.identifier == 'orders'
    assert rel.schema == 'staging'
    assert rel.type == ClickHouseRelationType.Table
    assert rel.can_exchange is True


def test_row_to_relation_view(adapter):
    row = _make_row('my_view', 'staging', type_info='view')
    rel = ClickHouseAdapter._row_to_relation(adapter, row)
    assert rel.type == ClickHouseRelationType.View
    assert rel.can_exchange is False


def test_row_to_relation_mv_with_sources(adapter):
    mvs = json.dumps([{'schema': 'raw', 'name': 'mv_orders', 'sql': 'SELECT 1'}])
    row = _make_row('orders', 'staging', mvs=mvs)
    rel = ClickHouseAdapter._row_to_relation(adapter, row)
    assert len(rel.mvs_pointing_to_it) == 1
    assert rel.mvs_pointing_to_it[0]['name'] == 'mv_orders'


def test_relations_cache_for_schemas_single_macro_call(adapter):
    schema_a = MagicMock(schema='staging', database='')
    schema_b = MagicMock(schema='marts', database='')
    cache_schemas = {schema_a, schema_b}

    adapter.execute_macro.return_value = [
        _make_row('orders', 'staging'),
        _make_row('customers', 'marts'),
    ]

    ClickHouseAdapter._relations_cache_for_schemas(adapter, [], cache_schemas=cache_schemas)

    adapter.execute_macro.assert_called_once_with(
        'clickhouse__list_relations_for_schemas',
        kwargs={'schema_relations': list(cache_schemas)},
    )
    assert adapter.cache.add.call_count == 2


def test_relations_cache_for_schemas_updates_schemas(adapter):
    schema_a = MagicMock(schema='staging', database='')
    schema_b = MagicMock(schema='marts', database='')
    cache_schemas = {schema_a, schema_b}

    adapter.execute_macro.return_value = []

    ClickHouseAdapter._relations_cache_for_schemas(adapter, [], cache_schemas=cache_schemas)

    adapter.cache.update_schemas.assert_called_once_with({('', 'staging'), ('', 'marts')})


def test_relations_cache_for_schemas_empty(adapter):
    adapter._get_cache_schemas.return_value = set()
    ClickHouseAdapter._relations_cache_for_schemas(adapter, [])
    adapter.execute_macro.assert_not_called()


def test_list_relations_without_caching(adapter):
    schema_relation = MagicMock(schema='staging', database='')
    adapter.execute_macro.return_value = [
        _make_row('orders', 'staging'),
        _make_row('my_view', 'staging', type_info='view'),
    ]

    relations = ClickHouseAdapter.list_relations_without_caching(adapter, schema_relation)

    adapter.execute_macro.assert_called_once_with(
        'list_relations_without_caching',
        kwargs={'schema_relation': schema_relation},
    )
    assert len(relations) == 2
