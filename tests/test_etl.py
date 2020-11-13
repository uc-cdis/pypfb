from pfb.etl.etl import ETL


def test_etl():
    etl = ETL("http://localhost:9200", "", "tests/pfb-data/test.avro", "participant")

    etl.extract()
    etl.transform()
    assert len(etl.spanning_tree_rows) == 1
    assert etl.spanning_tree_rows == [
        {
            (
                "somatic_mutation_calling_workflow_palingenist_hipponosological",
                "somatic_mutation_calling_workflow",
            ),
            (
                "somatic_annotation_workflow_outgoer_cylindroconoidal",
                "somatic_annotation_workflow",
            ),
            ("medical_history_preterience_Jacobinize", "medical_history"),
            ("aliquot_melituria_Khattish", "aliquot"),
            ("sample_Wagnerism_buccally", "sample"),
            (
                "clinical_supplement_perfectibilist_benzenediazonium",
                "clinical_supplement",
            ),
            ("phenotype_colorrhaphy_endevil", "phenotype"),
            ("alignment_workflow_Valencia_nonsymbolic", "alignment_workflow"),
            (
                "annotated_somatic_mutation_nonimposition_superelevation",
                "annotated_somatic_mutation",
            ),
            ("participant_metalinguistics_monofilm", "participant"),
            (
                "submitted_aligned_reads_soixantine_counterimpulse",
                "submitted_aligned_reads",
            ),
            ("analysis_metadata_apheresis_tympanomandibular", "analysis_metadata"),
            ("biospecimen_supplement_ryot_wheelsman", "biospecimen_supplement"),
            ("family_relationship_odometrical_puritanically", "family_relationship"),
            ("aligned_reads_index_pedology_saccharobacillus", "aligned_reads_index"),
            ("slide_Epimedium_particularism", "slide"),
            ("outcome_hydrocellulose_womandom", "outcome"),
            ("diagnosis_saccadic_cardiometric", "diagnosis"),
            ("run_metadata_panclastic_bandannaed", "run_metadata"),
            (
                "simple_somatic_mutation_unfatherliness_intrafusal",
                "simple_somatic_mutation",
            ),
            ("slide_image_horsemanship_chalybite", "slide_image"),
            ("read_group_qc_bilberry_histoplasmosis", "read_group_qc"),
            ("somatic_mutation_index_arboreally_fingerstall", "somatic_mutation_index"),
            ("demographic_duteousness_unassailing", "demographic"),
            ("experiment_metadata_Russophobe_matai", "experiment_metadata"),
            (
                "germline_mutation_calling_workflow_helioscopy_unquiescently",
                "germline_mutation_calling_workflow",
            ),
            (
                "submitted_unaligned_reads_mutagenic_aflagellar",
                "submitted_unaligned_reads",
            ),
            ("aligned_reads_metric_cutaneal_actinodrome", "aligned_reads_metric"),
            ("read_group_ethnicon_fordless", "read_group"),
            ("aligned_reads_unpealed_sexdigital", "aligned_reads"),
            ("annotation_marikina_mysteriously", "annotation"),
        }
    ]


def test_etl2():
    etl = ETL("http://localhost:9200", "", "tests/pfb-data/test.avro", "participant")
    etl.links = {
        ("A_1", "A"): [("B_1", "B"), ("C_1", "C"), ("C_2", "C"), ("B_2", "B")],
        ("B_1", "B"): [("D_1", "D")],
        ("D_1", "D"): [("C_1", "C")],
        ("C_1", "C"): [("D_2", "D"), ("B_3", "B")],
    }
    etl._build_spanning_table(("A_1", "A"))
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
