from pfb.etl.etl import ETL


def test_etl():
    etl = ETL("http://localhost:9200", "", "tests/pfb-data/test.avro", "participant")
    etl.links = {
        ("A_1", "A"): [("B_1", "B"), ("C_1", "C"), ("C_2", "C"), ("B_2", "B")],
        ("B_1", "B"): [("D_1", "D")],
        ("D_1", "D"): [("C_1", "C")],
        ("C_1", "C"): [("D_2", "D"), ("B_3", "B")],
    }
    etl.build_spanning_table(("A_1", "A"))
    assert len(etl.spanning_tree_rows) == 6
    for r in etl.spanning_tree_rows:
        assert r in [
            {("C_2", "C"), ("B_2", "B"), ("A_1", "A")},
            {("C_2", "C"), ("B_1", "B"), ("D_1", "D"), ("A_1", "A")},
            {("C_1", "C"), ("B_2", "B"), ("D_2", "D"), ("A_1", "A")},
            {("C_1", "C"), ("B_1", "B"), ("D_2", "D"), ("A_1", "A")},
            {("C_1", "C"), ("B_1", "B"), ("D_1", "D"), ("A_1", "A")},
            {("C_1", "C"), ("B_3", "B"), ("D_2", "D"), ("A_1", "A")},
        ]
