use std::io::BufRead;
use std::ops::Add;

use bytes::Buf;
use chrono::prelude::*;
use chrono::Utc;
use chrono::{DateTime, Duration};

pub trait Decoder: Buf {
    fn get_bool(&mut self) -> bool;
    fn get_string(&mut self) -> String
    where
        Self: Sized;
    fn get_timestamp(&mut self) -> DateTime<Utc>;
    fn get_rowinfo(&mut self, byte: char) -> bool;
    fn get_tupledata(&mut self) -> Vec<Tuple>;
    fn get_columns(&mut self) -> Vec<Column>
    where
        Self: Sized;
}

impl Decoder for &[u8] {
    fn get_bool(&mut self) -> bool {
        self.get_u8() != 0
    }

    fn get_string(&mut self) -> String
    where
        Self: Sized,
    {
        let mut buf = vec![];
        self.reader().read_until(0, &mut buf).unwrap();
        buf.pop();
        std::str::from_utf8(&buf).unwrap().to_string()
    }

    fn get_timestamp(&mut self) -> DateTime<Utc> {
        let micro = self.get_u64();
        let ts = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        ts.add(Duration::from_std(std::time::Duration::from_micros(micro)).unwrap())
    }

    fn get_rowinfo(&mut self, byte: char) -> bool {
        assert!(self.remaining() >= 1);
        match self.chunk()[0] as char == byte {
            true => {
                self.advance(1);
                true
            }
            false => false,
        }
    }

    fn get_tupledata(&mut self) -> Vec<Tuple> {
        let size = self.get_u16();
        let mut data = Vec::<Tuple>::with_capacity(size as usize);
        for _ in 0..size {
            let flag = self.get_u8() as char;
            match flag {
                'n' | 'u' => data.push(Tuple {
                    flag: flag,
                    value: None,
                }),
                't' => {
                    let vsize = self.get_u32() as usize;
                    data.push(Tuple {
                        flag: flag as char,
                        value: Some((&self.chunk()[..vsize]).to_vec()),
                    });
                    self.advance(vsize);
                }
                _ => panic!("Unknown data type flag: {:?}", flag),
            }
        }
        data
    }

    fn get_columns(&mut self) -> Vec<Column>
    where
        Self: Sized,
    {
        let size = self.get_u16();
        let mut data = Vec::<Column>::with_capacity(size as usize);
        for _ in 0..size {
            data.push(Column {
                key: self.get_bool(),
                name: self.get_string(),
                pg_type: self.get_u32(),
                mode: self.get_u32(),
            });
        }
        data
    }
}

pub struct Begin {
    // The final LSN of the transaction.
    pub lsn: u64,
    // Commit timestamp of the transaction. The value is in number of
    // microseconds since PostgreSQL epoch (2000-01-01).
    pub timestamp: DateTime<Utc>,
    // Xid of the transaction.
    pub xid: i32,
}

pub struct Commit {
    pub flags: u8,
    // The final LSN of the transaction.
    pub lsn: u64,
    // The final LSN of the transaction.
    pub transaction_lsn: u64,
    pub timestamp: DateTime<Utc>,
}

pub struct Relation {
    // ID of the relation.
    pub id: u32,
    // Namespace (empty string for pg_catalog).
    pub namespace: String,
    pub name: String,
    pub replica: u8,
    pub columns: Vec<Column>,
}

impl Relation {
    fn is_empty(&self) -> bool {
        self.id == 0 && self.name.is_empty() && self.replica == 0 && self.columns.is_empty()
    }
}

pub struct Type {
    // ID of the data type
    pub id: u32,
    pub namespace: String,
    pub name: String,
}

pub struct Insert {
    /// ID of the relation corresponding to the ID in the relation message.
    pub relation_id: u32,
    // Identifies the following TupleData message as a new tuple.
    pub new: bool,
    pub row: Vec<Tuple>,
}

pub struct Update {
    /// ID of the relation corresponding to the ID in the relation message.
    pub relation_id: u32,
    // Identifies the following TupleData message as a new tuple.
    pub old: bool,
    pub key: bool,
    pub new: bool,
    pub old_row: Option<Vec<Tuple>>,
    pub row: Vec<Tuple>,
}

pub struct Delete {
    /// ID of the relation corresponding to the ID in the relation message.
    pub relation_id: u32,
    // Identifies the following TupleData message as a new tuple.
    pub key: bool,
    pub old: bool,
    pub row: Vec<Tuple>,
}

pub struct Origin {
    pub lsn: u64,
    pub name: String,
}

// TODO: Add support for more Postgres types
// pub DecoderValue interface {
// 	pgtype.TextDecoder
// 	pgtype.Value
// }

pub struct Column {
    pub key: bool,
    pub name: String,
    pub pg_type: u32,
    pub mode: u32,
}

pub struct Tuple {
    pub flag: char,
    pub value: Option<Vec<u8>>,
}

pub enum LogicalReplicationMessage {
    Begin(Begin),
    Commit(Commit),
    Origin(Origin),
    Relation(Relation),
    Type(Type),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

// Parse a logical replication message.
// See https://www.postgresql.org/docs/current/static/protocol-logicalrep-message-formats.html
pub fn parse(src: &[u8]) -> Result<LogicalReplicationMessage, String> {
    let msg_type = src[0] as char;
    let mut buf = &src[1..];
    match msg_type {
        'B' => Ok(LogicalReplicationMessage::Begin(Begin {
            lsn: buf.get_u64(),
            timestamp: buf.get_timestamp(),
            xid: buf.get_i32(),
        })),
        'C' => Ok(LogicalReplicationMessage::Commit(Commit {
            flags: buf.get_u8(),
            lsn: buf.get_u64(),
            transaction_lsn: buf.get_u64(),
            timestamp: buf.get_timestamp(),
        })),
        'O' => Ok(LogicalReplicationMessage::Origin(Origin {
            lsn: buf.get_u64(),
            name: buf.get_string(),
        })),
        'R' => Ok(LogicalReplicationMessage::Relation(Relation {
            id: buf.get_u32(),
            namespace: buf.get_string(),
            name: buf.get_string(),
            replica: buf.get_u8(),
            columns: buf.get_columns(),
        })),
        'Y' => Ok(LogicalReplicationMessage::Type(Type {
            id: buf.get_u32(),
            namespace: buf.get_string(),
            name: buf.get_string(),
        })),
        'I' => Ok(LogicalReplicationMessage::Insert(Insert {
            relation_id: buf.get_u32(),
            new: buf.get_bool(),
            row: buf.get_tupledata(),
        })),
        'U' => {
            let relation_id = buf.get_u32();
            let key = buf.get_rowinfo('K');
            let old = buf.get_rowinfo('O');
            let old_row: Option<Vec<Tuple>> = None;
            if key || old {
                let _old_row = buf.get_tupledata();
            }
            let new = buf.get_bool();
            let row = buf.get_tupledata();

            Ok(LogicalReplicationMessage::Update(Update {
                relation_id: relation_id,
                key: key,
                old: old,
                old_row: old_row,
                new: new,
                row: row,
            }))
        }
        'D' => Ok(LogicalReplicationMessage::Delete(Delete {
            relation_id: buf.get_u32(),
            key: buf.get_rowinfo('K'),
            old: buf.get_rowinfo('O'),
            row: buf.get_tupledata(),
        })),
        _ => Err(format!("Unknown message type {}", msg_type)),
    }
}
