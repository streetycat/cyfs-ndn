use std::{collections::HashMap, ops::Range};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tide::sessions::Session;
use tokio::sync::oneshot;

use crate::{ChunkId, NdnResult, ObjId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowIndex(u64);

pub enum PipelineRow {
    ObjId(ObjId),
    Obj {
        id: ObjId,
        data: Value,
        depend_objs: Vec<ObjVec>, // some objects may depend on other objects
    },
    ObjArray {
        // object array
        id: ObjId,
        header: Value,
        data: Vec<ObjVec>,
    },
    ObjMap {
        // object map
        id: ObjId,
        header: Value,
        data: ObjMapDataType,
    },
    ObjMapItem(ObjMapRow),
    Provider(ProviderInfo), // first line
    TargetObj {
        indexes: Vec<RowIndex>,
        ranges: Vec<Range<RowIndex>>,
        ids: Vec<ObjId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeqInArray {
    Seq(u64),
    Range(Range<u64>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjVec {
    ObjId(Vec<ObjId>),
    RowIndex(Vec<RowIndex>),
    RowRange(Range<RowIndex>),
    SubInArray {
        array_id: ObjId,
        seqs: Vec<SeqInArray>,
    },
    SubInMap {
        map_id: ObjId,
        keys: Vec<String>,
    },
}

pub enum ObjMapRow {
    ObjId(HashMap<String, ObjId>),
    RowIndex(HashMap<String, RowIndex>),
    RowRange {
        row_ranges: Vec<Range<RowIndex>>,
        keys: Vec<String>,
    },
    SubInArray {
        array_id: ObjId,
        seqs: Vec<SeqInArray>,
        keys: Vec<String>,
    },
    SubInMap {
        map_id: ObjId,
        keys: Vec<(String, Option<String>)>, // (key, new_key), None means same key
    },
}

pub enum ObjMapDataType {
    Inside(Vec<ObjMapRow>),
    RowIndex(Vec<RowIndex>),
    RowRange(Vec<Range<RowIndex>>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInfo {
    app: Option<AppInfo>,
    url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppInfo {
    pub id: ObjId,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EndReason {
    Abort,
    Complete,
}

#[async_trait::async_trait]
pub trait IRowReader {
    async fn next_row(&mut self) -> NdnResult<Option<PipelineRow>>;
}

pub enum SubObjKey {
    Key(String),
    Seq(u64),
}

#[async_trait::async_trait]
pub trait IReaderController {
    /// cache_ndn_mgr_id: the NDN manager ID used for caching objects during reading
    /// is_obj_perfect: async function to check if an object is perfect (exists and valid)
    ///  - Parameters:
    ///    - &ObjId: the object ID to check
    ///    - Option<&SubObjKey>: optional sub-object key (for maps or arrays)
    ///    - Option<&Value>: optional object value (if already fetched), maybe it depends on some other objects in logic;
    ///         if Option<&SubObjKey> is None, the Value is the whole object of ObjId; if Some, the Value is the sub-object value
    ///  - Returns:
    ///    - Ok(ObjId): if the object is perfect
    ///    - Err(Vec<ObjId>): if the object is not perfect, returns a list of missing or invalid object IDs
    /// Returns a row reader for reading rows from the object pipeline
    async fn row_reader(
        &mut self,
        cache_ndn_mgr_id: String,
        is_obj_perfect: impl AsyncFn(&ObjId, Option<&SubObjKey>, Option<&Value>) -> Result<ObjId, Vec<ObjId>>
            + Send,
    ) -> NdnResult<impl IRowReader>;
    async fn wait_rebuild_target_object(
        &mut self,
        cache_ndn_mgr_id: String,
        is_obj_perfect: impl AsyncFn(&ObjId, Option<&SubObjKey>, Option<&Value>) -> Result<ObjId, Vec<ObjId>>
            + Send,
    ) -> NdnResult<oneshot::Receiver<NdnResult<Iterator<Item = ObjId>>>>;
    async fn end(self, clean_cache: bool) -> NdnResult<()>;
    async fn cache_ndn_mgr_id(&self) -> NdnResult<&str>;
}

#[async_trait::async_trait]
pub trait ISessionReader: IReaderController {
    async fn object_by_id(
        &mut self,
        id: &crate::ObjId,
        waker: oneshot::Sender<NdnResult<Value>>,
    ) -> NdnResult<()>;
    async fn ignore(&mut self, index: RowIndex, with_children: bool) -> NdnResult<()>;
    async fn ignore_by_id(&mut self, id: ObjId, with_children: bool) -> NdnResult<()>;
    async fn end(self, reason: EndReason) -> NdnResult<()>;
    fn session_id(&self) -> &str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionReaderEvent {
    NeedObject(Vec<ObjId>),
    IgnoreObject {
        objs: Vec<ObjVec>,
        with_children: bool,
    },
    End(EndReason),
}

#[async_trait::async_trait]
pub trait ISessionReaderEventWaiter {
    async fn wait(&mut self) -> NdnResult<SessionReaderEvent>;
}

pub struct RowIndexGenerator {
    last_index: RowIndex,
}

impl RowIndexGenerator {
    pub fn new(last_index: Option<RowIndex>) -> Self {
        RowIndexGenerator {
            last_index: last_index.unwrap_or(RowIndex(0)),
        }
    }

    pub fn next(&mut self) -> RowIndex {
        self.last_index.0 += 1; // increment the index for the next call
        self.last_index
    }

    pub fn increment(index: RowIndex, count: u64) -> RowIndex {
        RowIndex(index.0 + count)
    }
}

const PRIORITY_CHECK: u32 = 0; // priority for checking objects
const DEFAULT_PRIORITY_SIMPLE: u32 = u32::MAX >> 1; // default priority for writing objects
const DEFAULT_PRIORITY_COLLECTION: u32 = 1; // default priority for writing object collections
const DEFAULT_PRIORITY_MAP: u32 = DEFAULT_PRIORITY_COLLECTION; // default priority for writing object maps
const DEFAULT_PRIORITY_ARRAY: u32 = DEFAULT_PRIORITY_COLLECTION; // default priority for writing rebuild objects

#[async_trait::async_trait]
pub trait ISessionWriter {
    // Writes an object to the object pipeline.
    // `obj_id` and `obj_value` is the object to write;
    // Returns a Result indicating success or failure.
    async fn write_object(
        &mut self,
        obj_id: ObjId,
        obj_value: Value,
        row_index: RowIndex,
        depend_objs: Vec<ObjVec>, // some objects may depend on other objects
        priority: Option<u32>,
    ) -> crate::NdnResult<()>;
    async fn write_obj_id(
        &mut self,
        obj_id: ObjId,
        row_index: RowIndex,
        key: Option<String>,
        priority: Option<u32>,
    ) -> crate::NdnResult<()>;
    async fn write_object_array(
        &mut self,
        obj_array_id: ObjId,
        header: Value,
        data: Vec<ObjVec>,
        row_index: RowIndex,
        priority: Option<u32>,
    ) -> crate::NdnResult<()>;
    async fn write_object_map(
        &mut self,
        obj_map_id: ObjId,
        header: Value,
        data: Vec<ObjMapRow>,
        row_index: RowIndex,
        priority: Option<u32>,
    ) -> crate::NdnResult<()>;
    async fn set_target_objects(
        &mut self,
        indexes: Vec<RowIndex>,
        ranges: Vec<Range<RowIndex>>,
        ids: Vec<ObjId>,
        row_index: RowIndex,
    ) -> crate::NdnResult<()>;
    async fn last_index(&self) -> NdnResult<Option<RowIndex>>;
}

pub type SessionToken = String;

pub struct SessionRowReader {}

#[async_trait::async_trait]
impl IRowReader for SessionRowReader {
    async fn next_row(&mut self) -> NdnResult<Option<PipelineRow>> {
        // This is a pseudo implementation, as the actual logic is not defined.
        Ok(None)
    }
}

// #[async_trait::async_trait]
// impl ISessionReader for SessionReader {
//     async fn object_by_id(
//         &mut self,
//         _id: &crate::ObjId,
//         _waker: oneshot::Sender<NdnResult<Value>>,
//     ) -> NdnResult<()> {
//         unimplemented!()
//     }

//     async fn ignore(&mut self, _index: RowIndex, _with_children: bool) -> NdnResult<()> {
//         unimplemented!()
//     }

//     async fn ignore_by_id(&mut self, _id: ObjId, _with_children: bool) -> NdnResult<()> {
//         unimplemented!()
//     }

//     async fn end(self, _reason: EndReason) -> NdnResult<()> {
//         unimplemented!()
//     }
//     fn session_id(&self) -> &str {
//         "pseudo_session_reader"
//     }
// }

pub struct SessionReaderController {}

impl SessionReaderController {
    pub fn session_id(&self) -> &str {
        unimplemented!()
    }
    pub fn session_token(&self) -> &str {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl IReaderController for SessionReaderController {
    async fn row_reader(
        &self,
        _cache_ndn_mgr_id: String,
        _is_obj_perfect: impl AsyncFn(&ObjId, Option<&SubObjKey>, Option<&Value>) -> Result<ObjId, Vec<ObjId>>
            + Send,
    ) -> NdnResult<SessionRowReader> {
        unimplemented!()
    }

    async fn wait_rebuild_target_object(
        &self,
        _cache_ndn_mgr_id: String,
        _is_obj_perfect: impl AsyncFn(&ObjId, Option<&SubObjKey>, Option<&Value>) -> Result<ObjId, Vec<ObjId>>
            + Send,
    ) -> NdnResult<oneshot::Receiver<NdnResult<Iterator<Item = ObjId>>>> {
        unimplemented!()
    }

    async fn end(self, _clean_cache: bool) -> NdnResult<()> {
        unimplemented!()
    }

    async fn cache_ndn_mgr_id(&self) -> NdnResult<&str> {
        unimplemented!()
    }
}

pub enum SessionRequest {
    Read(SessionWriter),
    Write(SessionReaderController),
}

pub struct SessionServiceListener {}

impl SessionServiceListener {
    pub fn new() -> Self {
        SessionServiceListener {}
    }

    pub async fn listen(&mut self) -> NdnResult<SessionRequest> {
        unimplemented!()
    }
}

pub struct SessionWriterBuilder {}

impl SessionWriterBuilder {
    pub fn new() -> Self {
        SessionWriterBuilder {}
    }

    pub async fn build(
        &mut self,
        session_id: String,
        reader_url: String,
        token: SessionToken,
    ) -> NdnResult<(SessionWriter, Option<RowIndex>)> {
        unimplemented!()
    }
}

pub struct SessionWriter {}

impl SessionWriter {
    pub fn target_obj_ids(&self) -> Iterator<Item = ObjId> {
        unimplemented!()
    }
    pub fn hold_obj_ids(&self) -> Iterator<Item = ObjId> {
        unimplemented!()
    }
    pub async fn end(self, reason: EndReason) -> NdnResult<()> {
        unimplemented!()
    }
    pub fn session_id(&self) -> &str {
        unimplemented!()
    }
    pub fn session_token(&self) -> &str {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl ISessionWriter for SessionWriter {
    async fn write_object(
        &mut self,
        _obj_id: ObjId,
        _obj_value: Value,
        _row_index: RowIndex,
        _depend_objs: Vec<ObjVec>,
        _priority: Option<u32>,
    ) -> crate::NdnResult<()> {
        unimplemented!()
    }
    async fn write_obj_id(
        &mut self,
        _obj_id: ObjId,
        _row_index: RowIndex,
        _key: Option<String>,
        _priority: Option<u32>,
    ) -> crate::NdnResult<()> {
        unimplemented!()
    }
    async fn write_object_array(
        &mut self,
        _obj_array_id: ObjId,
        _header: Value,
        _data: Vec<ObjVec>,
        _row_index: RowIndex,
        _priority: Option<u32>,
    ) -> crate::NdnResult<()> {
        unimplemented!()
    }
    async fn write_object_map(
        &mut self,
        _obj_map_id: ObjId,
        _header: Value,
        _data: Vec<ObjMapRow>,
        _row_index: RowIndex,
        _priority: Option<u32>,
    ) -> crate::NdnResult<()> {
        unimplemented!()
    }
    async fn set_target_objects(
        &mut self,
        _indexes: Vec<RowIndex>,
        _ranges: Vec<Range<RowIndex>>,
        _ids: Vec<ObjId>,
        _row_index: RowIndex,
    ) -> crate::NdnResult<()> {
        unimplemented!()
    }
    async fn last_index(&self) -> NdnResult<Option<RowIndex>> {
        unimplemented!()
    }
}

impl ISessionReaderEventWaiter for SessionWriter {
    async fn wait(&mut self) -> NdnResult<SessionReaderEvent> {
        unimplemented!()
    }
}

mod demo {
    use crate::{
        build_named_object_by_json, NamedDataMgr, ObjectMap, ObjectMapBuilder, OBJ_TYPE_DIR,
    };

    use super::*;

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct DirObject {
        pub name: String,
        pub content: String, //ObjectMapId
        #[serde(default)]
        #[serde(skip_serializing_if = "is_default")]
        pub exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub meta: Option<serde_json::Value>,
        pub owner: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub create_time: Option<u64>,
        #[serde(flatten)]
        pub extra_info: HashMap<String, Value>,
    }

    impl DirObject {
        pub fn gen_obj_id(&self) -> (ObjId, String) {
            build_named_object_by_json(
                OBJ_TYPE_DIR,
                &serde_json::to_value(self).expect("json::value from DirObject failed"),
            )
        }
    }

    const DEMO_READER_CACHE_NDN_MGR_ID: &str = "demo_reader_cache_mgr";
    pub async fn start_backup_service_app() -> NdnResult<()> {
        let mut listener = SessionServiceListener::new();

        let check_token = async |token: &str| -> bool { true };

        let read_proc = async |controller: SessionReaderController| -> NdnResult<()> {
            let token = controller.session_token();

            if check_token(token).await {
                eprintln!("Invalid token: {}", token);
                controller.end(EndReason::Abort).await?;
                return Err(NdnResult::from("Invalid token"));
            }

            let waiter = controller
                .wait_rebuild_target_object(
                    DEMO_READER_CACHE_NDN_MGR_ID.to_string(),
                    async |obj_id, sub_key, obj_json| {
                        let target_obj_id = match sub_key {
                            Some(key) => match key {
                                SubObjKey::Key(k) => todo!("find in map {}", k),
                                SubObjKey::Seq(seq) => todo!("find in array {}", seq),
                            },
                            None => obj_id.clone(),
                        };

                        let obj_json = match obj_json {
                            Some(v) => {
                                eprintln!(
                                    "Check object: {}, sub_key: {:?}, value: {}",
                                    obj_id,
                                    sub_key,
                                    v.to_string()
                                );
                                v.clone()
                            }
                            None => {
                                eprintln!(
                                    "Check object: {}, sub_key: {:?}, no value",
                                    obj_id, sub_key
                                );
                                NamedDataMgr::get_object(
                                    Some(DEMO_READER_CACHE_NDN_MGR_ID),
                                    &target_obj_id,
                                    None,
                                )
                                .await
                                .map_err(|err| vec![target_obj_id])?
                            }
                        };
                        // todo: check the obj-json is perfect or not
                        Ok(target_obj_id)
                    },
                )
                .await?;
            let target_obj_ids = waiter.recv().await?;
            loop {
                let dir_obj_id = target_obj_ids.next()?;
                let dir_obj_json = if let Some(id) = dir_obj_id {
                    NamedDataMgr::get_object(Some(DEMO_READER_CACHE_NDN_MGR_ID), &id, None).await?
                } else {
                    break;
                };
                let dir_obj = serde_json::from_value::<DirObject>(dir_obj_json)
                    .map_err(|e| NdnResult::from(e))?;
                NamedDataMgr::pub_object_to_file(
                    Some(DEMO_READER_CACHE_NDN_MGR_ID),
                    dir_obj,
                    OBJ_TYPE_DIR,
                    format!("/{}/{}", controller.session_id(), dir_obj.name),
                    "pipeline_demo_user",
                    "pipeline_demo_app",
                )
                .await?;
            }
        };

        let write_proc = async |writer: SessionWriter| -> NdnResult<()> {
            let token = writer.session_token();

            if check_token(token).await {
                eprintln!("Invalid token: {}", token);
                writer.end(EndReason::Abort).await?;
                return Err(NdnResult::from("Invalid token"));
            }

            let last_index = writer.last_index().await?;
            let mut index_gen = RowIndexGenerator::new(last_index);
            let hold_obj_ids = writer.hold_obj_ids().collect::<Vec<_>>();
            eprintln!("Hold obj ids: {:?}", hold_obj_ids);
            let mut target_obj_ids = writer.target_obj_ids().collect::<Queue<_>>();

            if last_index.is_none() {
                writer
                    .set_target_objects(
                        vec![],
                        vec![],
                        writer.target_obj_ids().collect(),
                        index_gen.next(),
                    )
                    .await?;
            }

            loop {
                let target_obj_id = if let Some(id) = target_obj_ids.pop() {
                    id
                } else {
                    break;
                };

                let obj = NamedDataMgr::get_object(
                    Some(DEMO_READER_CACHE_NDN_MGR_ID),
                    &target_obj_id,
                    None,
                )
                .await?;
                match target_obj_id.obj_type() {
                    OBJ_TYPE_DIR => {
                        let dir_obj = serde_json::from_value::<DirObject>(obj)
                            .map_err(|e| NdnResult::from(e))?;
                        eprintln!("Dir object: {:?}", dir_obj);
                        let dir_index = index_gen.next();
                        let content_index = index_gen.next();
                        let obj_map_id = ObjId::try_from(dir_obj.content.as_str())?;
                        writer
                            .write_object(
                                target_obj_id.clone(),
                                serde_json::to_value(&dir_obj).map_err(|e| NdnResult::from(e))?,
                                dir_index,
                                vec![ObjVec::RowIndex(vec![content_index])], // depend on content object (obj-map)
                                Some(DEFAULT_PRIORITY_COLLECTION),
                            )
                            .await?;
                        let obj_map_body_json = NamedDataMgr::get_object(
                            Some(DEMO_READER_CACHE_NDN_MGR_ID),
                            &obj_map_id,
                            None,
                        )
                        .await?;
                        let obj_map = ObjectMapBuilder::open(&obj_map_body_json)
                            .map_err(|e| NdnResult::from(e))?
                            .build()
                            .await?;

                        let sub_keys = obj_map.iter().map(|(key, obj_id)| key).collect::<Vec<_>>();
                        writer
                            .write_object_map(
                                obj_map_id,
                                obj_map_body_json,
                                vec![ObjMapRow::RowRange {
                                    row_ranges: vec![Range {
                                        start: index_gen.increment(content_index, 1),
                                        end: index_gen
                                            .increment(content_index, sub_keys.len() as u64 + 1),
                                    }],
                                    keys: sub_keys.clone(),
                                }],
                                content_index,
                                None,
                            )
                            .await?;
                        for sub_key in sub_keys {
                            let sub_obj_id = obj_map.get(&sub_key).unwrap();
                            let sub_index = index_gen.next();
                            writer
                                .write_obj_id(
                                    sub_obj_id.clone(),
                                    sub_index,
                                    None,
                                    Some(DEFAULT_PRIORITY_SIMPLE),
                                )
                                .await?;
                            target_obj_ids.push(sub_obj_id);
                        }
                    }
                    OBJ_TYPE_FILE => {
                        let file_obj = serde_json::from_value::<FileObject>(obj)
                            .map_err(|e| NdnResult::from(e))?;
                        eprintln!("File object: {:?}", file_obj);
                        todo!("write file object");
                        todo!("write chunk list");
                        todo!("write chunk-ids one by one");
                    }
                    _ => {
                        unreachable!("Unsupported object type: {:?}", target_obj_id);
                        // if target_obj_id.is_chunk() {
                        //     let chunk_id = ChunkId::from(&target_obj_id);
                        //     let chunk_data = NamedDataMgr::get_chunk(
                        //         Some(DEMO_READER_CACHE_NDN_MGR_ID),
                        //         &chunk_id,
                        //     )
                        //     .await?;
                        //     eprintln!("Chunk data: {:?}", chunk_data);
                        // } else if target_obj_id.is_chunk_list() {
                        //     eprintln!("Unknown object type: {:?}", target_obj_id);
                        // } else if target_obj_id.is_map() {
                        //     eprintln!("Map object: {:?}", target_obj_id);
                        // } else if target_obj_id.is_array() {
                        //     eprintln!("Array object: {:?}", target_obj_id);
                        // } else {
                        //     eprintln!("Unknown object type: {:?}", target_obj_id);
                        // }
                    }
                }
            }
        };

        loop {
            let mut request = listener.listen().await?;

            tokio::task::spawn(async move {
                match request {
                    SessionRequest::Write(reader_controller) => {
                        if let Err(e) = read_proc(reader_controller).await {
                            eprintln!("Error in reader controller: {:?}", e);
                        }
                    }
                    SessionRequest::Read(writer) => {
                        // You can handle the writer case here if needed
                        if let Err(e) = write_proc(writer).await {
                            eprintln!("Error in writer: {:?}", e);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    pub async fn file_system_to_ndn<
        S: Storage + 'static,
        FDR: FileSystemDirReader + 'static,
        FFR: FileSystemFileReader + 'static,
        F: FileSystemReader<FDR, FFR> + 'static,
        NW: NdnWriter + 'static,
        PW: crate::packed_obj_pipeline::demo::Writer + Clone + 'static,
    >(
        path: Option<&Path>, // None for continue
        ndn_writer: NW,
        mut pipeline_writer: PW,
        reader: F,
        storage_db: S,
        chunk_size: u64,
    ) -> NdnResult<S::ItemId> {
        let insert_new_item = async |item: FileSystemItem,
                                     parent_path: &Path,
                                     storage_db: &S,
                                     depth: u64,
                                     parent_item_id: Option<S::ItemId>|
               -> NdnResult<StorageItemDetail<S::ItemId>> {
                todo!()
                /// Insert a new item into storage, return the inserted item detail
                /// Dir: insert as Scanning
                /// File: insert as Hashing, and insert all chunks as Hashing
        };

        if let Some(path) = path {
            let none_path = PathBuf::from("");
            let fs_item = reader.info(path).await?;
            // insert root item
            insert_new_item(
                fs_item,
                path.parent().unwrap_or(none_path.as_path()),
                &storage_db,
                0,
                None,
            )
            .await?;
        }

        loop {
            match storage_db
                .get_uncomplete_dir_or_file_with_max_depth_min_child_seq()
                .await?
            {
                Some(mut item_detail) => {
                    let mut item_status = item_detail.status.clone();
                    loop {
                        match item_status.clone() {
                            ItemStatus::New | ItemStatus::Scanning => {
                                // It's a directory, list its children and insert them.
                                // translate status to Hashing
                            }
                            ItemStatus::Hashing => {
                                match &item_detail.item {
                                    StorageItem::Dir(dir_obj) => {
                                        info!(
                                            "hash dir: {}, item_id: {:?}",
                                            item_detail.item.name().check_name(),
                                            item_detail.item_id
                                        );
                                        /// build object map for directory
                                        /// translate status to Transfer, and get diff items from storage_db(there may be some history saved in storage_db)
                                        /// build 2 rows in pipeline_writer if it's new object not exist in server:
                                        /// 1. object map object(find all row index of sub items in storage_db, and build the row)
                                        /// 2. dir object(it's content is the object map id, depend on object map row)
                                    }
                                    StorageItem::File(file_item) => {
                                        /// list all chunks of the file, calculate chunk ids, if it's new object not exist in server:
                                        /// 1. build row and write to pipeline_writer.
                                        /// 2. push chunk to server by ndn_writer.
                                        /// 2. translate status to Complete.

                                        /// build chunk list for file
                                        /// translate status to Transfer, and get diff items from storage_db(there may be some history saved in storage_db)
                                        /// build 2 rows for pipeline_writer:
                                        /// 1. chunk list object(find all row index of chunks in storage_db, and build the row)
                                        /// 2. file object(it's content is the chunk list id, depend on chunk list row)
                                    }
                                    StorageItem::Chunk(_) => {
                                        // chunk should be handled in file transfer
                                        unreachable!(
                                            "expect file or dir, got: {:?} in history storage",
                                            item_detail.item.item_type()
                                        );
                                    }
                                }
                            }
                            ItemStatus::Transfer(transfer_desc) => {
                                // load rows build in hashing status, write to pipeline_writer
                                // translate status to Complete
                            }
                            ItemStatus::Complete(_) => {
                                info!(
                                    "complete item: {:?}, path: {}, depth: {}",
                                    item_detail.item.name(),
                                    item_detail.parent_path.display(),
                                    item_detail.depth
                                );
                                break;
                            }
                        }
                    }
                }
                None => break,
            }
        }

        Ok(root_item.item_id)
    }

    pub async fn backup_client_app(plan_id: &str) -> NdnResult<()> {
        let storage_db = open("backup_client_storage_db").await?;
        let mut prev_task_version = None;
        let backup_task = if let Some(last_task) = storage_db.get_last_task(plan_id).await? {
            if last_task.is_complete() {
                prev_task_version = Some(last_task.version);
                None
            } else {
                prev_task_version = Some(last_task.prev_version);
                last_task
            }
        } else {
            None
        };
        
        let (backup_task, dir_path) = match backup_task {
            Some(task) => {
                eprintln!("Continue backup task: {:?}", task);
                // continue the backup task
                (task, None)
            }
            None => {
                let snapshot_path = make_snapshot_path(plan_id);
                let pipeline_session_id = random();
                let task = storage_db.create_new_task(plan_id, snapshot_path, pipeline_session_id, prev_task_version).await?;
                (task, Some(snapshot_path))
            }
        };

        let task_storage_db = storage_db;
        let token = refresh_token(backup_task.auth_info.clone()).await?;
        let local_url = format!("ndn/{}", backup_task.ndn_mgr_id);
        let ndn_writer = create_ndn_writer(backup_task.backup_server_url.clone()).await?;
        // load from disk by backup_task.pipeline_session_id if it's not new task
        let (pipeline_writer, last_row_index) = SessionWriterBuilder::new().build(backup_task.pipeline_session_id.clone(), local_url, token).await?;
        let row_index_gen = RowIndexGenerator::new(last_row_index);

        file_system_to_ndn(
            dir_path,
            ndn_writer,
            pipeline_writer,
            create_fs_reader().await?,
            task_storage_db,
            backup_task.chunk_size,
        ).await?;

        // pipeline build complete now, we should wait the reader complete
        loop {
            match pipeline_writer.wait().await? {
                SessionReaderEvent::NeedObject(obj_ids) => {
                    // ignore it, or find the object and write it.
                },
                SessionReaderEvent::IgnoreObject { objs, with_children } => {
                    // ignore, the object should ignore by writer
                },
                SessionReaderEvent::End(end_reason) => {
                    break; // complete
                },
            }
        }
    }
}
