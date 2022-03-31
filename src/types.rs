struct Begin {
    lsn: u64,
    timestamp: u64,
    xid: u32,
}

impl Begin {
    fn from_bytes(buf &[u8]) -> Self {
        Self {
            lsn: u64::from_be_bytes(buf[..7].try_into().unwrap()),
            timestamp: u64::from_be_bytes(buf[8..15].try_into().unwrap()),
            xid: u64::from_be_bytes(buf[16..19].try_into().unwrap()),
        }
    }
}

struct Message {
    xid: Option<u32>,
    flags: u8,
    lsn: u64,
    prefix: String,
    length: u32,
    content: [u8],
}

struct Commit {
    flags: u8,
    lsn: u64
    transaction_lsn: u64,
    timestamp: u64,
}

struct Origin {
    lsn: u64,
    name: String,
}

struct Column {
    key: bool,
    name: String,
    type: u32,
    mode: u32,
}

struct Relation {
    xid: Option<u32>,
    id: u32,
    namespace: String,
    name: String,
    replica: u8,
    columns: [Column],
}

struct Type {
    xid: Option<u32>,
    id: u32,
    namespace: String,
    name: String,
}

struct Tuple {
    flag: u8,
    value: [u8],
}

struct Insert {
    xid: Option<u32>,
    relation_id: u32,
    new: bool,
    row: [Tuple],
}

struct Update {
    xid: Option<u32>,
    relation_id: u32,
    old: bool,
    key: bool,
    new: bool,
    old_row: [Tuple],
    row: [Tuple],
}

struct Delete {
    xid: Option<u32>,
    relation_id: u32,
    old: bool,
    new: bool,
    row: [Tuple],
}

struct Truncate {
    xid: Option<u32>,
    relation_id: u32,
    cascade: bool,
    restart_identity: bool,
    relations: [u32],
}

struct StreamStart {
    xid: u32,
    first_segment: bool,
}

struct StreamStop {}

struct StreamCommit {
    xid: u32,
    lsn: u64,
    transaction_lsn: u64,
    timestamp: u64,
}

struct StreamAbort {
    xid: u32,
    sub_xid: u32,
}
