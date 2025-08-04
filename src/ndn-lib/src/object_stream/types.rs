use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    path::PathBuf,
};

use serde_json::Value;

use crate::ObjId;

pub type LineIndex = u64;

pub enum LineIndexWithRelation {
    Real(LineIndex), // the real index of a line.
    Ref(LineIndex),  // a index of a line that is a reference to another line.
}

pub enum Line {
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
    Index {
        obj_start_index: LineIndex,
        obj_ids: Vec<ObjId>,
    },
    ObjHeader {
        obj_index: LineIndex,
        id: ObjId,
        header: Value,
    },
    StreamHeader {
        app: Option<AppInfo>,
        provider_url: Option<String>,
    },
    EndCheck {
        indexes: Vec<LineIndex>,
        ranges: Vec<Range<LineIndex>>,
        ids: Vec<ObjId>,
    },
}

pub enum ObjArraySubObjIndex {
    Index(LineIndex),
    Range(Range<LineIndex>),
}

pub enum ObjArrayLine {
    Memory(Vec<ObjId>),
    File(PathBuf),
    Lines(Vec<ObjArraySubObjIndex>),
    Diff {
        base_array: ObjId,
        actions: Vec<ObjArrayDiffAction>,
    },
}

pub enum ObjIdType {
    Id(ObjId),
    Index(LineIndex),
}

pub enum ObjIdArrayType {
    Id(Vec<ObjId>),
    Index(Vec<LineIndex>),
}

// all pos(type: u64) for base_array
pub enum ObjArrayDiffAction {
    Append(Vec<ObjId>),
    AppendWithIndex(Vec<LineIndex>),
    Truncate(u64),
    InsertAt(ObjArrayDiffActionInsertAt<ObjId>),
    InsertAtWithIndex(ObjArrayDiffActionInsertAt<LineIndex>),
    InsertHead(Vec<ObjId>),
    InsertHeadWithIndex(Vec<LineIndex>),
    InsertAtMultiple(Vec<ObjArrayDiffActionInsertAt<ObjId>>),
    InsertAtMultipleWithIndex(Vec<ObjArrayDiffActionInsertAt<LineIndex>>),
    Replace(ObjArrayDiffActionReplace<ObjId>),
    ReplaceWithIndex(ObjArrayDiffActionReplace<LineIndex>),
    ReplaceMultiple(Vec<ObjArrayDiffActionReplace<ObjId>>),
    ReplaceMultipleWithIndex(Vec<ObjArrayDiffActionReplace<LineIndex>>),
    RemoveAt(u64),
    RemoveRange(Range<u64>),
}

pub struct ObjArrayDiffActionInsertAt<T> {
    pos: u64,
    ids: Vec<T>,
}

pub struct ObjArrayDiffActionReplace<T> {
    pos: u64,
    count: u64,
    ids: Vec<T>,
}

pub enum ObjMapLine {
    Memory(HashMap<String, ObjId>),
    MemoryWithIndex(HashMap<String, LineIndex>),
    File(PathBuf),
    Diff {
        base_map: ObjId,
        actions: Vec<ObjMapDiffAction>,
    },
}

// all pos(type: u64) for base_array
pub enum ObjMapDiffAction {
    Set(String, ObjId),
    SetWithIndex(String, LineIndex),
    SetMultiple(HashMap<String, ObjId>),
    SetMultipleWithIndex(HashMap<String, LineIndex>),
    Remove(String),
    RemoveMultiple(HashSet<String>),
}

pub struct AppInfo {
    pub id: ObjId,
    pub name: String,
}
