from asset_allocation_runtime_common.strategy_engine import universe as universe_service


def test_gold_catalog_exposes_us_liquid_governed_fields(monkeypatch) -> None:
    specs = {
        "market_data": universe_service.UniverseTableSpec(
            name="market_data",
            as_of_column="date",
            columns={
                "market_cap": universe_service.UniverseColumnSpec(
                    name="market_cap",
                    data_type="double precision",
                    value_kind="number",
                    operators=universe_service._NUMBER_OPERATORS,
                ),
                "dollar_volume_20d": universe_service.UniverseColumnSpec(
                    name="dollar_volume_20d",
                    data_type="double precision",
                    value_kind="number",
                    operators=universe_service._NUMBER_OPERATORS,
                ),
                "primary_listing": universe_service.UniverseColumnSpec(
                    name="primary_listing",
                    data_type="boolean",
                    value_kind="boolean",
                    operators=universe_service._BOOLEAN_OPERATORS,
                ),
                "country": universe_service.UniverseColumnSpec(
                    name="country",
                    data_type="text",
                    value_kind="string",
                    operators=universe_service._STRING_OPERATORS,
                ),
                "price_liquidity_eligible": universe_service.UniverseColumnSpec(
                    name="price_liquidity_eligible",
                    data_type="boolean",
                    value_kind="boolean",
                    operators=universe_service._BOOLEAN_OPERATORS,
                ),
            },
        )
    }
    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)

    fields = {field["field"] for field in universe_service.list_gold_universe_catalog("postgresql://test")["fields"]}

    assert {
        "security.market_cap",
        "market.dollar_volume_20d",
        "security.primary_listing",
        "security.country",
        "security.is_price_liquidity_eligible",
    }.issubset(fields)
