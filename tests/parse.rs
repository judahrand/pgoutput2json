use glob::glob;

use pgoutput2json;

#[test]
fn parse_wal_data() {
    struct ExpectedRow {
        id: u32,
        val: String,
    }
    let _expected: std::collections::HashMap<u32, ExpectedRow> = std::collections::HashMap::from([
        (
            2,
            ExpectedRow {
                id: 40,
                val: "forty".to_string(),
            },
        ),
        (
            11,
            ExpectedRow {
                id: 11,
                val: "eleven".to_string(),
            },
        ),
        (
            14,
            ExpectedRow {
                id: 12,
                val: "twelve".to_string(),
            },
        ),
    ]);
    for filepath in glob("testdata/*.waldata").unwrap() {
        println!("filepath: {:?}", filepath);
        let waldata = std::fs::read(filepath.unwrap()).unwrap();
        let m = pgoutput2json::parse(&waldata).unwrap();

        match m {
            pgoutput2json::LogicalReplicationMessage::Relation(relation) => {
                println!("id: {:?}", relation.id);
                println!("namespace: {:?}", relation.namespace);
                println!("name: {:?}", relation.name);
                println!("replica: {:?}", relation.replica);
            }
            pgoutput2json::LogicalReplicationMessage::Insert(insert) => {
                println!("relation_id: {:?}", insert.relation_id);
                println!("new: {:?}", insert.new);
            }
            pgoutput2json::LogicalReplicationMessage::Type(type_) => {
                assert_eq!(type_.id, 35756);
                assert_eq!(type_.namespace, "public");
                assert_eq!(type_.name, "ticket_state");
            }
            _ => {}
        }
    }
}
