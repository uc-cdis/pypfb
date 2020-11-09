from pfb.etl.etl import ETL


def test_etl():
    etl = ETL("pfb-data/test.avro", "participant")
    etl.links = {
        "A_1": ["B_1", "C_1", "C_2", "B_2"],
        "B_1": ["D_1"],
        "D_1": ["C_1"],
        "C_1": ["D_2", "B_3"],
        "B_2": [],
        "C_2": [],
        "D_2": [],
        "B_3": [],
    }
    etl.build_spanning_table("A_1")
    assert len(etl.spanning_tree_rows) == 6
    for r in etl.spanning_tree_rows:
        assert r in [
            {"C_2", "B_2", "A_1"},
            {"C_2", "B_1", "D_1", "A_1"},
            {"C_1", "B_2", "D_2", "A_1"},
            {"C_1", "B_1", "D_2", "A_1"},
            {"C_1", "B_1", "D_1", "A_1"},
            {"C_1", "B_3", "D_2", "A_1"},
        ]
