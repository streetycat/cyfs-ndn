use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    path::PathBuf,
};

use serde_json::Value;

use crate::ObjId;

// is_ref: true if the `line` is a reference object for `line_index`, false if the `line` is the real content at `line_index`.
pub enum LineIndex {
    Real(u64), // the real index of a line.
    Ref(u64),  // a index of a line that is a reference to another line.
}

pub enum Line {
    Index {
        obj_start_index: LineIndex,
        obj_ids: Vec<ObjId>,
    },
    ObjHeader {
        obj_index: LineIndex,
        id: ObjId,
        header: Value,
    },
    Obj {
        id: ObjId,
        obj: Value,
    },
    ObjArray {
        id: ObjId,
        header: Option<Value>,
        content: ObjArrayLine,
    },
    ObjMap {
        id: ObjId,
        header: Option<Value>,
        content: ObjMapLine,
    },
}

pub enum ObjArrayLine {
    Memory(Vec<ObjId>),
    File(PathBuf),
    Diff {
        base_array: ObjId,
        actions: Vec<ObjArrayDiffAction>,
    },
}

// all pos(type: u64) for base_array
pub enum ObjArrayDiffAction {
    Append(Vec<ObjId>),
    Truncate(u64),
    InsertAt(ObjArrayDiffActionInsertAt),
    InsertHead(Vec<ObjId>),
    InsertAtMultiple(Vec<ObjArrayDiffActionInsertAt>),
    Replace(ObjArrayDiffActionReplace),
    ReplaceMultiple(Vec<ObjArrayDiffActionReplace>),
    RemoveAt(u64),
    RemoveRange(Range<u64>),
}

pub struct ObjArrayDiffActionInsertAt {
    pos: u64,
    ids: Vec<ObjId>,
}

pub struct ObjArrayDiffActionReplace {
    pos: u64,
    count: u64,
    ids: Vec<ObjId>,
}

pub enum ObjMapLine {
    Memory(HashMap<String, ObjId>),
    File(PathBuf),
    Diff {
        base_map: ObjId,
        actions: Vec<ObjMapDiffAction>,
    },
}

// all pos(type: u64) for base_array
pub enum ObjMapDiffAction {
    Set(String, ObjId),
    SetMultiple(HashMap<String, ObjId>),
    Remove(String),
    RemoveMultiple(HashSet<String>),
}
