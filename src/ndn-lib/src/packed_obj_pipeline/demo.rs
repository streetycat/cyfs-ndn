// # 典型场景

// 不同设备之间超大目录数据同步。

// # 评价标准

// 1. 完成同步的时间
// 2. 实际网络操作次数和流量消耗
// 3. 双方磁盘操作次数

// # 基本方案

// 1. 深度遍历源目录，逐个分解出各子对象(Chunk/File/Directory)；每个子对象构造时，其依赖的子对象已经完成构造。
// 2. 原则上顺次传输各个构造完成的子对象，在传输过程中，目标设备如果发现该对象已经存在，应该终止该对象的传输。
// 3. 若在遍历过程中发现容器对象，立即插队传输容器头部，目标设备若发现容器对象已存在，则应终止该容器对象的传输，若发现其某些子对象已存在，也终止这些子对象的传输。
//     * 小对象无需终止，其实只有Chunk和容器对象是大对象，而Chunk的传输使用ndn_client.push_chunk实现。
// 4. 源设备最后应该发送一个衡量同步完成的方法
//     * 某个/某些指定的对象在目标设备上重建完成

// # PackedObjPipeline

// 对上述过程进行抽象，提供基础工具批量重建对象。

// 1. 把所有对象按行存储/传输，每行独立语义，这是一个`Pipeline`，可能是个文件，也可能是个网络连接...。
// 2. 逐行赋予一个顺次递增的`Index`，但可以不按顺序写入或读出。
// 3. 除了顺序读取，还可以读取指定`Index`的行，也可以读取指定`ObjId`的对象。
// 5. 一个`Pipeline`如果包含了其重建所需的所有条件(子对象)，则通过读取这个`Pipeline`即可完成重建，这是一个完备的`Pipeline`；
//    如果这个`Pipeline`需要依赖特定应用才能完成重建，它应该指定明确的`AppId`;
//    如果这个`Pipeline`需要额外依赖其构造者设备上的某些状态，则应该指定一个`provider_url`。

// ## 跨端的ObjectPipeline

// 以最前面目录同步为场景，`Pipeline`的`Reader`应该能给`Writer`适当的反馈，以调整`write`的顺序，甚至忽略部分内容。

// ## 细节优化:

// 1. cache:
//     * 在内存缓存若干可能要马上传输的Chunk，避免反复从磁盘读取。
// 2. ObjId比较大，必要时候使用`ObjectPipeline.line_index`在网络上交换信息，甚至通过`Range<LineIndex>`来批量表达对象

// 下面的实现代码是上述设计的实现demo

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    path::PathBuf,
};
use tokio::sync::oneshot;

use serde_json::Value;

use crate::{
    packed_obj_pipeline::demo::{
        dir_backup::{DirObject, NdnReader, StorageItemName},
        dir_restore::ChildInfo,
    },
    ChunkId, FileObject, NdnResult, ObjId, OBJ_TYPE_DIR, OBJ_TYPE_FILE,
};

// The index of a line in the object pipeline.
// It is a 64-bit unsigned integer, which allows for a large number of lines.
// This index is used to identify the position of a line in the pipeline and can be used
// to access specific lines or objects within the pipeline.
pub type LineIndex = u64;

// The LineIndexWithRelation enum represents the relationship between a line and index in the object pipeline.
// A line can either be a line at a real index or a virtual line reference to a line index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LineIndexWithRelation {
    Real(LineIndex), // the real index of a line.
    Ref(LineIndex),  // a index of a line that is a reference to another line.
}

impl LineIndexWithRelation {
    pub fn is_real(&self) -> bool {
        matches!(self, LineIndexWithRelation::Real(_))
    }

    pub fn is_ref(&self) -> bool {
        matches!(self, LineIndexWithRelation::Ref(_))
    }

    pub fn index(&self) -> LineIndex {
        match self {
            LineIndexWithRelation::Real(idx) => *idx,
            LineIndexWithRelation::Ref(idx) => *idx,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CollectionHeader {
    Index(LineIndex),
    Header { id: ObjId, header: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    app: Option<AppInfo>,
    provider_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Line {
    Chunk(ChunkId), // chunk id
    Obj {
        // simple object
        id: ObjId,
        obj: Value,
    },
    ObjArray {
        // object array
        header: CollectionHeader,
        content: ObjArrayLine,
    },
    ObjMap {
        // object map
        header: CollectionHeader,
        content: ObjMapLine,
    },
    Index {
        // index only
        obj_start_index: LineIndex, // index
        obj_ids: Vec<ObjId>,
        obj_header_start_index: Option<LineIndex>, // index
    },
    ObjHeader {
        // header only
        obj_index: LineIndex,
        id: ObjId,
        header: Value,
    },
    Header(Header), // first line
    RebuildObj {
        indexes: Vec<LineIndex>,
        ranges: Vec<Range<LineIndex>>,
        ids: Vec<ObjId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjArraySubObjIndex {
    Index(LineIndex),
    Range(Range<LineIndex>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjArrayLine {
    Memory(Vec<ObjId>),
    File(PathBuf),
    Lines(Vec<ObjArraySubObjIndex>),
    Diff {
        base_array: ObjId,
        actions: Vec<ObjArrayDiffAction>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjIdType {
    Id(ObjId),
    Index(LineIndex),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjIdArrayType {
    Id(Vec<ObjId>),
    Index(Vec<LineIndex>),
}

// all pos(type: u64) for base_array
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjArrayDiffActionInsertAt<T> {
    pos: u64,
    ids: Vec<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjArrayDiffActionReplace<T> {
    pos: u64,
    count: u64,
    ids: Vec<T>,
}

impl ObjArrayLine {
    fn list_children(&self) -> (Vec<LineIndex>, Vec<ObjId>) {
        match self {
            ObjArrayLine::Memory(ids) => (Vec::new(), ids.clone()),
            ObjArrayLine::File(_) => (Vec::new(), Vec::new()),
            ObjArrayLine::Lines(lines) => {
                let mut indexes = Vec::new();
                for item in lines {
                    match item {
                        ObjArraySubObjIndex::Index(idx) => indexes.push(*idx),
                        ObjArraySubObjIndex::Range(range) => {
                            indexes.extend(range.clone());
                        }
                    }
                }
                (indexes, Vec::new())
            }
            ObjArrayLine::Diff {
                base_array,
                actions,
            } => {
                let mut indexes = Vec::new();
                let mut ids = vec![base_array.clone()];
                for action in actions {
                    match action {
                        ObjArrayDiffAction::Append(ids_vec) => ids.extend(ids_vec.clone()),
                        ObjArrayDiffAction::AppendWithIndex(indexes_vec) => {
                            indexes.extend(indexes_vec.clone())
                        }
                        ObjArrayDiffAction::InsertAt(insert_at) => {
                            ids.extend(insert_at.ids.clone());
                        }
                        ObjArrayDiffAction::InsertAtWithIndex(insert_at_with_index) => {
                            indexes.extend(insert_at_with_index.ids.clone());
                        }
                        ObjArrayDiffAction::InsertHead(ids_vec) => ids.extend(ids_vec.clone()),
                        ObjArrayDiffAction::InsertHeadWithIndex(indexes_vec) => {
                            indexes.extend(indexes_vec.clone())
                        }
                        ObjArrayDiffAction::InsertAtMultiple(insert_at_multiple) => {
                            for insert in insert_at_multiple {
                                ids.extend(insert.ids.clone());
                            }
                        }
                        ObjArrayDiffAction::InsertAtMultipleWithIndex(
                            insert_at_multiple_with_index,
                        ) => {
                            for insert in insert_at_multiple_with_index {
                                indexes.extend(insert.ids.clone());
                            }
                        }
                        ObjArrayDiffAction::Replace(replace) => {
                            ids.extend(replace.ids.clone());
                        }
                        ObjArrayDiffAction::ReplaceWithIndex(replace_with_index) => {
                            indexes.extend(replace_with_index.ids.clone());
                        }
                        ObjArrayDiffAction::ReplaceMultiple(replace_multiple) => {
                            for replace in replace_multiple {
                                ids.extend(replace.ids.clone());
                            }
                        }
                        ObjArrayDiffAction::ReplaceMultipleWithIndex(
                            replace_multiple_with_index,
                        ) => {
                            for replace in replace_multiple_with_index {
                                indexes.extend(replace.ids.clone());
                            }
                        }
                        ObjArrayDiffAction::RemoveAt(pos) => {
                            // Remove at pos does not affect indexes or ids
                        }
                        ObjArrayDiffAction::RemoveRange(range) => {
                            // Remove range does not affect indexes or ids
                        }
                        ObjArrayDiffAction::Truncate(_) => {
                            // Truncate does not affect indexes or ids
                        }
                    }
                }
                (indexes, ids)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjMapDiffAction {
    Set(String, ObjId),
    SetWithIndex(String, LineIndex),
    SetMultiple(HashMap<String, ObjId>),
    SetMultipleWithIndex(HashMap<String, LineIndex>),
    Remove(String),
    RemoveMultiple(HashSet<String>),
}

impl ObjMapLine {
    fn list_children(&self) -> (Vec<LineIndex>, Vec<ObjId>) {
        match self {
            ObjMapLine::Memory(map) => (Vec::new(), map.values().cloned().collect()),
            ObjMapLine::MemoryWithIndex(map) => (map.values().cloned().collect(), Vec::new()),
            ObjMapLine::File(_) => (Vec::new(), Vec::new()),
            ObjMapLine::Diff { base_map, actions } => {
                let mut indexes = Vec::new();
                let mut ids = vec![base_map.clone()];
                for action in actions {
                    match action {
                        ObjMapDiffAction::Set(_, id) => ids.push(id.clone()),
                        ObjMapDiffAction::SetWithIndex(_, index) => indexes.push(*index),
                        ObjMapDiffAction::SetMultiple(map) => ids.extend(map.values().cloned()),
                        ObjMapDiffAction::SetMultipleWithIndex(map) => {
                            indexes.extend(map.values().cloned())
                        }
                        ObjMapDiffAction::Remove(_) => {}
                        ObjMapDiffAction::RemoveMultiple(keys) => {
                            // Removing keys does not affect indexes or ids
                        }
                    }
                }
                (indexes, ids)
            }
        }
    }
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
pub trait Reader {
    async fn next_line(&mut self) -> NdnResult<Option<(Line, LineIndexWithRelation)>>;
    async fn line_at(
        &mut self,
        index: LineIndex,
        waker: Option<oneshot::Sender<NdnResult<Option<Line>>>>,
    ) -> NdnResult<()>;
    async fn object_by_id(
        &mut self,
        id: &crate::ObjId,
        for_index: LineIndex,
        waker: Option<oneshot::Sender<NdnResult<Option<Line>>>>,
    ) -> NdnResult<()>;
}

#[async_trait::async_trait]
pub trait WriterFeedback {
    async fn ignore(&mut self, index: LineIndex, with_children: bool) -> NdnResult<()>;
    async fn ignore_by_id(&mut self, id: ObjId, with_children: bool) -> NdnResult<()>;
    async fn end(self, reason: EndReason) -> NdnResult<()>;
}

#[async_trait::async_trait]
pub trait ReaderListener {
    async fn wait(&mut self) -> NdnResult<ReaderEvent>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReaderEvent {
    NeedLine(Vec<LineIndex>),
    NeedObject(Vec<(ObjId, LineIndex)>),
    IgnoreLine(Vec<LineIndex>),
    IgnoreObject(Vec<ObjId>),
    End(EndReason),
}

#[async_trait::async_trait]
pub trait Writer {
    // Writes an object to the object pipeline.
    // `obj_id` and `obj_value` is the object to write;
    // `ref_line_index` should be `Some(index)` if this object is a reference to another line instead a real line, otherwise `None`.
    // Returns a Result indicating success or failure.
    async fn write_object(
        &mut self,
        obj_id: ObjId,
        obj_value: Value,
        ref_line_index: Option<LineIndex>,
    ) -> crate::NdnResult<LineIndexWithRelation>;
    async fn write_chunk(&mut self, chunk_id: ChunkId) -> crate::NdnResult<LineIndex>;
    async fn write_object_array(
        &mut self,
        obj_array_id: ObjId,
        header: Value,
        content: ObjArrayLine,
        ref_line_index: Option<LineIndex>,
        priority: Option<u32>, // write priority in network, 1 is the highest priority, u32::MAX is the lowest priority.
    ) -> crate::NdnResult<(LineIndexWithRelation, Option<LineIndex>)>; // <content_index, header_index>
    async fn write_object_map(
        &mut self,
        obj_map_id: ObjId,
        header: Value,
        content: ObjMapLine,
        ref_line_index: Option<LineIndex>,
        priority: Option<u32>,
    ) -> crate::NdnResult<(LineIndexWithRelation, Option<LineIndex>)>; // <content_index, header_index>

    // set objects waiting rebuild, the rebuilder should check all the objects to check it success.
    // call once only.
    async fn set_rebuild_objects(
        &mut self,
        ids: impl Iterator<Item = &ObjId> + Send,
    ) -> crate::NdnResult<()>;
}

mod pipeline_sim {
    use crate::NdnError;

    // 这是一个pipeline模拟器，示意一个简单的pipeline的工作过程；
    // 这个pipeline维持一个写队列和一个读队列，启动一个`task`，它每隔1ms(参数配置)把写队列中的下一个(参数配置)没有设定`read`和`ignore`标记的元素复制出来收集到一个`Vec`中，并写入到读队列中，并为其在写队列中设定一个`read`标志；
    // 写队列可以通过`write_object`/`write_object_array`/`write_object_map`/`set_rebuild_objects`方法添加元素，每接收一行立即为其递增分配一个`LineIndex`，如果参数带入了`ref_index`，不分配新的`LineIndex`，只记录为`LineIndexWithRelation::Ref`;
    //          * 如果`ref_line_index`是`Some(index)`，则其由`Reader`要求写入对象，非实际打包的元素，直接插入读队列，不放入写队列，并设定`read`标志，不用等待；
    //      如果通过`write_object_array`/`write_object_map`接收到容器类型行：
    //          * 如果`ref_line_index`是`None`，则将其分为`ObjArray`/`ObjMap`和`ObjHeader`行，并分别分配一个`LineIndex`，让两个对象的`index`字段相互指向对方，原子性(加锁)插入写队列，并直接发送`ObjHeader`行，并设定`read`标志，不用等待；
    //          * 如果`ref_line_index`是`Some(index)`，则其由`Reader`要求写入对象，非实际打包的元素，直接插入读队列，不放入写队列，并设定`read`标志，不用等待；
    //      如果通过`set_rebuild_objects`收到`RebuildObj`行，立即发送它，并设定`read`标志，不用等待
    // 提供`next_line`方法按顺序消费读队列中的元素，如果读队列是空则添加一个等待事件，当读队列中有数据进入时唤醒这些等待事件；
    // 提供`ignore`/`ignore_by_id`方法忽略写队列中的某些元素，被忽略的元素在写队列中被标记为`ignored`;
    //     如果`with_children`为`true`，则意味着这个对象是`ObjArray`/`ObjMap`，找到该对象并找到各相关行，忽略这些相关行；
    // 提供`line_at`/`object_by_id`要求写队列立即发送指定元素到读队列中，由`next_line`接口返回, 并设定`read`标志， 如果设定了等待事件`Receiver`，在`next_line`接口读取到它时唤醒对应的事件；
    use super::*;
    use bincode::de;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use tokio::sync::mpsc::{self, Receiver, Sender};
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};

    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::oneshot;

    const DEFAULT_TICK_MS: u64 = 1;
    const DEFAULT_BATCH_PER_TICK: usize = 4;

    #[derive(Clone)]
    struct ObjRecord {
        id: ObjId,
        content_index: LineIndex,
        header_index: Option<LineIndex>, // for container types
    }

    struct WriteEntry {
        line: Line,
        index: LineIndex,
        read: bool,
        ignored: bool,
    }

    struct Inner {
        next_index: AtomicU64,

        // All lines that may be scheduled (excluding Ref lines inserted directly into read queue)
        write_queue: Vec<WriteEntry>,

        // Output queue (already scheduled lines)
        read_queue: VecDeque<(Line, LineIndexWithRelation)>,

        // Mapping id -> content/header indices
        id_map: HashMap<ObjId, ObjRecord>,

        // Flag end reason
        end_reason: Option<EndReason>,

        // Reader events channel (from reader to writer) - minimal use in demo
        event_tx: mpsc::Sender<ReaderEvent>,

        wakers_for_index: HashMap<LineIndex, Vec<oneshot::Sender<NdnResult<Option<Line>>>>>,
        wakers_for_id: HashMap<ObjId, Vec<oneshot::Sender<NdnResult<Option<Line>>>>>,
    }

    impl Inner {
        fn alloc_index(&self) -> LineIndex {
            self.next_index.fetch_add(1, Ordering::SeqCst)
        }

        fn push_write_entry(&mut self, line: Line, index: LineIndex) {
            self.write_queue.push(WriteEntry {
                line,
                index,
                read: false,
                ignored: false,
            });
        }

        fn schedule_line_if_needed(&mut self, idx: usize) {
            if let Some(entry) = self.write_queue.get_mut(idx) {
                if entry.read || entry.ignored {
                    return;
                }
                entry.read = true;
                self.read_queue
                    .push_back((entry.line.clone(), LineIndexWithRelation::Real(entry.index)));
            }
        }

        fn schedule_specific_index(&mut self, index: LineIndex) {
            if let Some(pos) = self.write_queue.iter().position(|e| e.index == index) {
                self.schedule_line_if_needed(pos);
            }
        }

        fn mark_ignored(&mut self, index: LineIndex) -> Option<&WriteEntry> {
            if let Some(entry) = self.write_queue.iter_mut().find(|e| e.index == index) {
                entry.ignored = true;
                Some(entry)
            } else {
                None
            }
        }

        fn ignore(&mut self, index: LineIndex, with_children: bool) -> NdnResult<()> {
            let rec_index = self
                .id_map
                .iter()
                .find(|(_, rec)| rec.content_index == index || rec.header_index == Some(index))
                .map(|(_, rec)| (rec.header_index, rec.content_index));

            if let Some((header_index, content_index)) = rec_index {
                if let Some(header_idx) = header_index {
                    self.mark_ignored(header_idx);
                }
                let ignore_content = self.mark_ignored(content_index);
                if with_children && header_index.is_some() {
                    if let Some(content) = ignore_content {
                        if let Line::ObjArray { content, .. } = &content.line {
                            // If it's an ObjArray, ignore all children
                            let (child_indexes, child_ids) = content.list_children();
                            for child_index in child_indexes {
                                self.ignore(child_index, true);
                            }
                            for child_id in child_ids {
                                self.ignore_by_id(child_id, true);
                            }
                        } else if let Line::ObjMap { content, .. } = &content.line {
                            // If it's an ObjMap, ignore all children
                            let (child_indexes, child_ids) = content.list_children();
                            for child_index in child_indexes {
                                self.ignore(child_index, true);
                            }
                            for child_id in child_ids {
                                self.ignore_by_id(child_id, true);
                            }
                        }
                    }
                }
            }

            Ok(())
        }

        fn ignore_by_id(&mut self, id: ObjId, with_children: bool) -> NdnResult<()> {
            let idx = self
                .id_map
                .get(&id)
                .map(|rec| (rec.header_index, rec.content_index));

            if let Some((header_index, content_index)) = idx {
                if let Some(header_idx) = header_index {
                    self.mark_ignored(header_idx);
                }
                let ignore_content = self.mark_ignored(content_index);
                if with_children && header_index.is_some() {
                    if let Some(content) = ignore_content {
                        if let Line::ObjArray { content, .. } = &content.line {
                            // If it's an ObjArray, ignore all children
                            let (child_indexes, child_ids) = content.list_children();
                            for child_index in child_indexes {
                                self.ignore(child_index, true);
                            }
                            for child_id in child_ids {
                                self.ignore_by_id(child_id, true);
                            }
                        } else if let Line::ObjMap { content, .. } = &content.line {
                            // If it's an ObjMap, ignore all children
                            let (child_indexes, child_ids) = content.list_children();
                            for child_index in child_indexes {
                                self.ignore(child_index, true);
                            }
                            for child_id in child_ids {
                                self.ignore_by_id(child_id, true);
                            }
                        }
                    }
                }
            }

            Ok(())
        }
    }

    struct SimPipeline {
        inner: Arc<Mutex<Inner>>,
        notify: Arc<tokio::sync::Notify>,
        tick_ms: u64,
        batch: usize,
    }

    impl SimPipeline {
        fn new() -> (SimWriter, SimReader, SimWriterFeedback, SimReaderListener) {
            let (event_tx, event_rx) = mpsc::channel(32);
            let inner = Inner {
                next_index: AtomicU64::new(0),
                write_queue: Vec::new(),
                read_queue: VecDeque::new(),
                id_map: HashMap::new(),
                end_reason: None,
                event_tx,
                wakers_for_index: HashMap::new(),
                wakers_for_id: HashMap::new(),
            };
            let inner = Arc::new(Mutex::new(inner));
            let notify = Arc::new(tokio::sync::Notify::new());
            let pipeline = Self {
                inner: inner.clone(),
                notify: notify.clone(),
                tick_ms: DEFAULT_TICK_MS,
                batch: DEFAULT_BATCH_PER_TICK,
            };
            pipeline.spawn_scheduler();
            (
                SimWriter {
                    inner: inner.clone(),
                    notify: notify.clone(),
                },
                SimReader {
                    inner: inner.clone(),
                    notify: notify.clone(),
                },
                SimWriterFeedback {
                    inner: inner.clone(),
                    notify: notify.clone(),
                },
                SimReaderListener { rx: event_rx },
            )
        }

        fn spawn_scheduler(&self) {
            let inner = self.inner.clone();
            let notify = self.notify.clone();
            let tick = self.tick_ms;
            let batch = self.batch;
            tokio::spawn(async move {
                loop {
                    {
                        let mut guard = inner.lock().await;
                        if guard.end_reason.is_some() && guard.read_queue.is_empty() {
                            // No more scheduling needed
                            break;
                        }

                        let mut produced = 0;
                        for idx in 0..guard.write_queue.len() {
                            if produced >= batch {
                                break;
                            }
                            if !guard.write_queue[idx].read && !guard.write_queue[idx].ignored {
                                guard.schedule_line_if_needed(idx);
                                produced += 1;
                            }
                        }
                        if produced > 0 {
                            notify.notify_waiters();
                        }
                    }
                    sleep(Duration::from_millis(tick)).await;
                }
            });
        }
    }

    #[derive(Clone)]
    pub struct SimWriter {
        inner: Arc<Mutex<Inner>>,
        notify: Arc<tokio::sync::Notify>,
    }

    #[derive(Clone)]
    pub struct SimWriterFeedback {
        inner: Arc<Mutex<Inner>>,
        notify: Arc<tokio::sync::Notify>,
    }

    #[derive(Clone)]
    pub struct SimReader {
        inner: Arc<Mutex<Inner>>,
        notify: Arc<tokio::sync::Notify>,
    }

    pub struct SimReaderListener {
        rx: mpsc::Receiver<ReaderEvent>,
    }

    #[async_trait::async_trait]
    impl Writer for SimWriter {
        async fn write_object(
            &mut self,
            obj_id: ObjId,
            obj_value: Value,
            ref_line_index: Option<LineIndex>,
        ) -> crate::NdnResult<LineIndexWithRelation> {
            let mut guard = self.inner.lock().await;
            if let Some(idx) = ref_line_index {
                // Reference line, do not allocate new index, just return Ref.
                guard.read_queue.push_back((
                    Line::Obj {
                        id: obj_id,
                        obj: obj_value,
                    },
                    LineIndexWithRelation::Ref(idx),
                ));
                self.notify.notify_waiters();
                return Ok(LineIndexWithRelation::Ref(idx));
            }

            let index = guard.alloc_index();
            let line = Line::Obj {
                id: obj_id.clone(),
                obj: obj_value.clone(),
            };
            guard.push_write_entry(line.clone(), index);
            guard.id_map.insert(
                obj_id.clone(),
                ObjRecord {
                    id: obj_id.clone(),
                    content_index: index,
                    header_index: None,
                },
            );
            Ok(LineIndexWithRelation::Real(index))
        }

        async fn write_chunk(&mut self, chunk_id: ChunkId) -> crate::NdnResult<LineIndex> {
            let mut guard = self.inner.lock().await;
            let index = guard.alloc_index();
            let line = Line::Chunk(chunk_id.clone());
            guard.push_write_entry(line, index);
            guard.id_map.insert(
                chunk_id.to_obj_id(),
                ObjRecord {
                    id: chunk_id.to_obj_id(),
                    content_index: index,
                    header_index: None,
                },
            );
            Ok(index)
        }

        async fn write_object_array(
            &mut self,
            obj_array_id: ObjId,
            header: Value,
            content: ObjArrayLine,
            ref_line_index: Option<LineIndex>,
            priority: Option<u32>,
        ) -> crate::NdnResult<(LineIndexWithRelation, Option<LineIndex>)> {
            let mut guard = self.inner.lock().await;

            if let Some(idx) = ref_line_index {
                guard.read_queue.push_back((
                    Line::ObjArray {
                        header: CollectionHeader::Header {
                            id: obj_array_id,
                            header,
                        },
                        content,
                    },
                    LineIndexWithRelation::Ref(idx),
                ));
                self.notify.notify_waiters();
                return Ok((LineIndexWithRelation::Ref(idx), None));
            }
            // Split into ObjArray(content) + ObjHeader
            let content_index = guard.alloc_index();
            let header_index = guard.alloc_index();

            let content_line = Line::ObjArray {
                header: CollectionHeader::Index(header_index),
                content,
            };
            let header_line = Line::ObjHeader {
                obj_index: content_index,
                id: obj_array_id.clone(),
                header,
            };

            // Insert: ensure header is immediately schedulable (spec: send header first)
            guard.push_write_entry(content_line, content_index);
            guard.push_write_entry(header_line.clone(), header_index);

            // Mark header as already read => schedule instantly
            if let Some(entry) = guard
                .write_queue
                .iter_mut()
                .find(|e| e.index == header_index)
            {
                if !entry.read && !entry.ignored {
                    entry.read = true;
                    guard
                        .read_queue
                        .push_back((header_line, LineIndexWithRelation::Real(header_index)));
                    self.notify.notify_waiters();
                }
            }

            guard.id_map.insert(
                obj_array_id.clone(),
                ObjRecord {
                    id: obj_array_id,
                    content_index,
                    header_index: Some(header_index),
                },
            );

            Ok((
                LineIndexWithRelation::Real(content_index),
                Some(header_index),
            ))
        }

        async fn write_object_map(
            &mut self,
            obj_map_id: ObjId,
            header: Value,
            content: ObjMapLine,
            ref_line_index: Option<LineIndex>,
            priority: Option<u32>,
        ) -> crate::NdnResult<(LineIndexWithRelation, Option<LineIndex>)> {
            let mut guard = self.inner.lock().await;

            if let Some(idx) = ref_line_index {
                guard.read_queue.push_back((
                    Line::ObjMap {
                        header: CollectionHeader::Header {
                            id: obj_map_id,
                            header,
                        },
                        content,
                    },
                    LineIndexWithRelation::Ref(idx),
                ));
                self.notify.notify_waiters();
                return Ok((LineIndexWithRelation::Ref(idx), None));
            }
            let content_index = guard.alloc_index();
            let header_index = guard.alloc_index();

            let content_line = Line::ObjMap {
                header: CollectionHeader::Index(header_index),
                content: content.clone(),
            };
            let header_line = Line::ObjHeader {
                obj_index: content_index,
                id: obj_map_id.clone(),
                header: header.clone(),
            };

            guard.push_write_entry(content_line, content_index);
            guard.push_write_entry(header_line.clone(), header_index);

            if let Some(entry) = guard
                .write_queue
                .iter_mut()
                .find(|e| e.index == header_index)
            {
                if !entry.read && !entry.ignored {
                    entry.read = true;
                    guard
                        .read_queue
                        .push_back((header_line, LineIndexWithRelation::Real(header_index)));
                    self.notify.notify_waiters();
                }
            }

            guard.id_map.insert(
                obj_map_id.clone(),
                ObjRecord {
                    id: obj_map_id,
                    content_index,
                    header_index: Some(header_index),
                },
            );

            Ok((
                LineIndexWithRelation::Real(content_index),
                Some(header_index),
            ))
        }

        async fn set_rebuild_objects(
            &mut self,
            ids: impl Iterator<Item = &ObjId> + Send,
        ) -> crate::NdnResult<()> {
            let mut guard = self.inner.lock().await;
            let indexes = Vec::<LineIndex>::new();
            let ranges = Vec::<Range<LineIndex>>::new();
            let ids_vec = ids.cloned().collect::<Vec<_>>();
            let line = Line::RebuildObj {
                indexes,
                ranges,
                ids: ids_vec,
            };
            // Directly schedule (no index for rebuild line)
            guard
                .read_queue
                .push_back((line, LineIndexWithRelation::Real(u64::MAX)));
            self.notify.notify_waiters();
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl WriterFeedback for SimWriterFeedback {
        async fn ignore(&mut self, index: LineIndex, with_children: bool) -> NdnResult<()> {
            let mut guard = self.inner.lock().await;
            guard.ignore(index, with_children)
        }

        async fn ignore_by_id(&mut self, id: ObjId, with_children: bool) -> NdnResult<()> {
            let mut guard = self.inner.lock().await;
            guard.ignore_by_id(id, with_children)
        }

        async fn end(self, reason: EndReason) -> NdnResult<()> {
            let mut guard = self.inner.lock().await;
            guard.end_reason = Some(reason);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Reader for SimReader {
        async fn next_line(&mut self) -> NdnResult<Option<(Line, LineIndexWithRelation)>> {
            loop {
                {
                    let mut guard = self.inner.lock().await;
                    if let Some(item) = guard.read_queue.pop_front() {
                        // Notify any wakers for this index
                        if let LineIndexWithRelation::Real(index) = item.1 {
                            if let Some(wakers) = guard.wakers_for_index.remove(&index) {
                                for waker in wakers {
                                    let _ = waker.send(Ok(Some(item.0.clone())));
                                }
                            }
                        }
                        match &item.0 {
                            Line::Obj { id, .. } => {
                                // Notify any wakers for this object ID
                                if let Some(wakers) = guard.wakers_for_id.remove(id) {
                                    for waker in wakers {
                                        let _ = waker.send(Ok(Some(item.0.clone())));
                                    }
                                }
                            }
                            Line::ObjArray { header, .. } => {
                                match header {
                                    CollectionHeader::Header { id, .. } => {
                                        // Notify any wakers for this object ID
                                        if let Some(wakers) = guard.wakers_for_id.remove(id) {
                                            for waker in wakers {
                                                let _ = waker.send(Ok(Some(item.0.clone())));
                                            }
                                        }
                                    }
                                    CollectionHeader::Index(index) => {
                                        let obj_id = guard
                                            .write_queue
                                            .iter()
                                            .find(|e| e.index == *index)
                                            .map(|entry| {
                                                match &entry.line {
                                                    Line::ObjHeader { id, .. } => {
                                                        // Notify any wakers for this object ID
                                                        Some(id.clone())
                                                    }
                                                    _ => None,
                                                }
                                            });
                                        if let Some(Some(id)) = obj_id {
                                            if let Some(wakers) = guard.wakers_for_id.remove(&id) {
                                                for waker in wakers {
                                                    let _ = waker.send(Ok(Some(item.0.clone())));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Line::ObjMap { header, .. } => {
                                match header {
                                    CollectionHeader::Header { id, .. } => {
                                        // Notify any wakers for this object ID
                                        if let Some(wakers) = guard.wakers_for_id.remove(id) {
                                            for waker in wakers {
                                                let _ = waker.send(Ok(Some(item.0.clone())));
                                            }
                                        }
                                    }
                                    CollectionHeader::Index(index) => {
                                        let obj_id = guard
                                            .write_queue
                                            .iter()
                                            .find(|e| e.index == *index)
                                            .map(|entry| {
                                                match &entry.line {
                                                    Line::ObjHeader { id, .. } => {
                                                        // Notify any wakers for this object ID
                                                        Some(id.clone())
                                                    }
                                                    _ => None,
                                                }
                                            });
                                        if let Some(Some(id)) = obj_id {
                                            if let Some(wakers) = guard.wakers_for_id.remove(&id) {
                                                for waker in wakers {
                                                    let _ = waker.send(Ok(Some(item.0.clone())));
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            _ => {
                                // no waker needed
                            }
                        }
                        return Ok(Some(item));
                    }
                    if guard.end_reason.is_some() {
                        // Signal end with an empty rebuild line
                        return Err(NdnError::InvalidState("ended".to_string()));
                    }
                }
                self.notify.notified().await;
            }
        }

        async fn line_at(
            &mut self,
            index: LineIndex,
            waker: Option<oneshot::Sender<NdnResult<Option<Line>>>>,
        ) -> NdnResult<()> {
            {
                let mut guard = self.inner.lock().await;
                if let Some(waker) = waker {
                    guard.wakers_for_index.entry(index).or_default().push(waker);
                }
                guard.schedule_specific_index(index);
                self.notify.notify_waiters();
            }
            Ok(())
        }

        async fn object_by_id(
            &mut self,
            id: &crate::ObjId,
            for_index: LineIndex,
            waker: Option<oneshot::Sender<NdnResult<Option<Line>>>>,
        ) -> NdnResult<()> {
            {
                let mut guard = self.inner.lock().await;
                if let Some(content_index) = guard.id_map.get(id).map(|rec| rec.content_index) {
                    if let Some(waker) = waker {
                        guard
                            .wakers_for_id
                            .entry(id.clone())
                            .or_default()
                            .push(waker);
                    }
                    guard.schedule_specific_index(content_index);
                    self.notify.notify_waiters();
                } else {
                    if let Some(waker) = waker {
                        // If not found, send an error
                        let _ = waker.send(Err(NdnError::NotFound(format!(
                            "Object with id {} not found",
                            id
                        ))));
                    }
                }

                // TODO: 这里的object也可能需要由应用层从其他方式获取，先不实现了
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ReaderListener for SimReaderListener {
        async fn wait(&mut self) -> NdnResult<ReaderEvent> {
            // TODO: 监听`reader`方的控制请求
            unimplemented!();
        }
    }

    // Public helper to construct the simulation pipeline.
    pub fn new_pipeline_sim() -> (SimWriter, SimReader, SimWriterFeedback, SimReaderListener) {
        SimPipeline::new()
    }
}

mod dir_backup {
    use std::{
        collections::HashMap,
        io::SeekFrom,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use http_types::content;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use tokio::net::unix::pipe;

    use crate::{
        build_named_object_by_json,
        packed_obj_pipeline::demo::{pipeline_sim, LineIndex, ObjArrayLine, ObjMapLine},
        ChunkHasher, ChunkId, ChunkListBody, ChunkListBuilder, FileObject, HashMethod,
        NamedDataMgr, NdnError, NdnResult, ObjId, TrieObjectMap, TrieObjectMapBody,
        TrieObjectMapBuilder, OBJ_TYPE_CHUNK_LIST, OBJ_TYPE_DIR, OBJ_TYPE_FILE,
    };
    use buckyos_kit::is_default;

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

    #[derive(Clone)]
    pub struct FileStorageItem {
        pub obj: FileObject,
        pub chunk_size: Option<u64>,
    }

    impl std::fmt::Debug for FileStorageItem {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FileStorageItem: {}", self.obj.name)
        }
    }

    #[derive(Clone, Debug)]
    pub struct ChunkItem {
        pub seq: u64,                  // sequence number in file
        pub offset: SeekFrom,          // offset in the file
        pub chunk_id: Option<ChunkId>, // chunk id
    }

    #[derive(Clone, Debug)]
    pub enum StorageItem {
        Dir(DirObject),
        File(FileStorageItem),
        Chunk(ChunkItem), // (seq, offset, ChunkId)
    }

    #[derive(Debug, Clone)]
    pub enum StorageItemName {
        Name(String),
        ChunkSeq(u64), // seq
    }

    impl StorageItemName {
        pub fn check_name(&self) -> &str {
            match self {
                StorageItemName::Name(name) => name.as_str(),
                StorageItemName::ChunkSeq(_) => panic!("expect name"),
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub enum StorageItemNameRef<'a> {
        Name(&'a str),
        ChunkSeq(u64), // seq
    }

    impl<'a> StorageItemNameRef<'a> {
        pub fn check_name(&self) -> &str {
            match self {
                StorageItemNameRef::Name(name) => *name,
                StorageItemNameRef::ChunkSeq(_) => panic!("expect name"),
            }
        }
    }

    impl StorageItem {
        pub fn is_dir(&self) -> bool {
            matches!(self, StorageItem::Dir(_))
        }
        pub fn is_file(&self) -> bool {
            matches!(self, StorageItem::File(_))
        }
        pub fn is_chunk(&self) -> bool {
            matches!(self, StorageItem::Chunk(_))
        }
        pub fn check_dir(&self) -> &DirObject {
            match self {
                StorageItem::Dir(dir) => dir,
                _ => panic!("expect dir"),
            }
        }
        pub fn check_file(&self) -> &FileStorageItem {
            match self {
                StorageItem::File(file) => file,
                _ => panic!("expect file"),
            }
        }
        pub fn check_chunk(&self) -> &ChunkItem {
            match self {
                StorageItem::Chunk(chunk) => chunk,
                _ => panic!("expect chunk"),
            }
        }
        pub fn name(&self) -> StorageItemNameRef<'_> {
            match self {
                StorageItem::Dir(dir) => StorageItemNameRef::Name(dir.name.as_str()),
                StorageItem::File(file) => StorageItemNameRef::Name(file.obj.name.as_str()),
                StorageItem::Chunk(chunk_item) => StorageItemNameRef::ChunkSeq(chunk_item.seq),
            }
        }
        pub fn item_type(&self) -> &str {
            match self {
                StorageItem::Dir(_) => "dir",
                StorageItem::File(_) => "file",
                StorageItem::Chunk(_) => "chunk",
            }
        }
    }

    pub type PathDepth = u64;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ContainerLineIndex {
        pub index: LineIndex,
        pub header_index: Option<LineIndex>,
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub struct TransferStatusDesc {
        pub obj_id: ObjId,
        pub content_header: Option<Value>,
        pub content_line: Option<ContentLine>, // If it's a ObjArray or ObjMap, return the content line
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub struct CompleteStatusDesc {
        pub obj_id: ObjId,
        pub content_header: Option<Value>,
        pub content_line: Option<ContentLine>, // If it's a ObjArray or ObjMap, return the content line
        pub line_index: LineIndex,
        pub container_line_index: Option<ContainerLineIndex>,
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub enum ItemStatus {
        New,
        Scanning,
        Hashing,
        Transfer(TransferStatusDesc),
        Complete(CompleteStatusDesc),
    }

    impl std::fmt::Debug for ItemStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ItemStatus::New => write!(f, "New"),
                ItemStatus::Scanning => write!(f, "Scanning"),
                ItemStatus::Hashing => write!(f, "Hashing"),
                ItemStatus::Transfer(desc) => {
                    write!(f, "Transfer({}", desc.obj_id.to_string())
                }
                ItemStatus::Complete(desc) => {
                    write!(f, "Complete({})", desc.obj_id.to_string())
                }
            }
        }
    }

    impl ItemStatus {
        pub fn is_new(&self) -> bool {
            matches!(self, ItemStatus::New)
        }
        pub fn is_scanning(&self) -> bool {
            matches!(self, ItemStatus::Scanning)
        }
        pub fn is_hashing_or_transfer(&self) -> bool {
            matches!(self, ItemStatus::Hashing | ItemStatus::Transfer(_))
        }
        pub fn is_complete(&self) -> bool {
            matches!(self, ItemStatus::Complete(_))
        }
        pub fn is_hashing(&self) -> bool {
            matches!(self, ItemStatus::Hashing)
        }
        pub fn is_transfer(&self) -> bool {
            matches!(self, ItemStatus::Transfer(_))
        }
        pub fn get_obj_id(&self) -> Option<&ObjId> {
            match self {
                ItemStatus::Transfer(desc) => Some(&desc.obj_id),
                ItemStatus::Complete(desc) => Some(&desc.obj_id),
                _ => None,
            }
        }
        pub fn check_transfer(&self) -> &TransferStatusDesc {
            match self {
                ItemStatus::Transfer(desc) => desc,
                _ => panic!("expect TransferStatusDesc"),
            }
        }
        pub fn check_complete(&self) -> &CompleteStatusDesc {
            match self {
                ItemStatus::Complete(desc) => desc,
                _ => panic!("expect CompleteStatusDesc"),
            }
        }
    }

    pub trait StrorageCreator<S: Storage<ItemId = Self::ItemId>>:
        AsyncFn(String) -> NdnResult<S> + Send + Sync + Sized
    {
        type ItemId: Send + Sync + Clone + std::fmt::Debug + Eq + std::hash::Hash + Sized;
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub enum ContentLine {
        Chunk(ChunkId),    // chunk id
        Obj(ObjId, Value), // ObjId, content
        Array(ObjArrayLine),
        Map(ObjMapLine),
    }

    impl ContentLine {
        pub fn is_chunk(&self) -> bool {
            matches!(self, ContentLine::Chunk(_))
        }
        pub fn is_obj(&self) -> bool {
            matches!(self, ContentLine::Obj(_, _))
        }
        pub fn is_array(&self) -> bool {
            matches!(self, ContentLine::Array(_))
        }
        pub fn is_map(&self) -> bool {
            matches!(self, ContentLine::Map(_))
        }
        pub fn check_array(self) -> ObjArrayLine {
            match self {
                ContentLine::Array(array) => array,
                _ => panic!("expect ObjArrayLine"),
            }
        }
        pub fn check_map(self) -> ObjMapLine {
            match self {
                ContentLine::Map(map) => map,
                _ => panic!("expect ObjMapLine"),
            }
        }
    }

    pub struct StorageItemDetail<IT> {
        pub item_id: IT,
        pub item: StorageItem,
        pub parent_id: IT,
        pub status: ItemStatus,
        pub parent_path: PathBuf,
        pub depth: PathDepth,
    }

    #[derive(Clone)]
    pub enum ItemSelectKey<IT> {
        ItemId(IT),
        Child(IT, StorageItemName), // <parent_item_id, child_name>
    }

    #[async_trait::async_trait]
    pub trait Storage: Send + Sync + Sized + Clone {
        type ItemId: Send + Sync + Clone + std::fmt::Debug + Eq + std::hash::Hash + Sized;

        async fn create_new_item(
            &self,
            item: &StorageItem,
            depth: PathDepth,
            parent_path: &Path,
            parent_item_id: Option<Self::ItemId>,
        ) -> NdnResult<StorageItemDetail<Self::ItemId>>;
        async fn begin_transaction(&self) -> NdnResult<()>;
        async fn commit_transaction(&self) -> NdnResult<()>;
        async fn rollback_transaction(&self) -> NdnResult<()>;
        async fn begin_hash(&self, key: &ItemSelectKey<Self::ItemId>) -> NdnResult<()>;
        async fn begin_transfer(
            &self,
            key: &ItemSelectKey<Self::ItemId>,
            obj_id: &ObjId,
            content_header: Option<&Value>,
        ) -> NdnResult<Option<ContentLine>>; // return the content line if it's a ObjArray or ObjMap, otherwise None>;
        async fn complete(
            &self,
            key: &ItemSelectKey<Self::ItemId>,
            line_index: LineIndex,
            container_line_index: Option<ContainerLineIndex>,
        ) -> NdnResult<()>;
        async fn get_root(&self) -> NdnResult<StorageItemDetail<Self::ItemId>>;
        async fn get_item(
            &self,
            key: &ItemSelectKey<Self::ItemId>,
        ) -> NdnResult<StorageItemDetail<Self::ItemId>>;
        async fn get_uncomplete_dir_or_file_with_max_depth_min_child_seq(
            &self,
        ) -> NdnResult<Option<StorageItemDetail<Self::ItemId>>>; // to continue scan

        async fn list_children_order_by_child_seq(
            &self,
            parent_key: &ItemSelectKey<Self::ItemId>,
            offset: Option<u64>,
            limit: Option<u64>,
        ) -> NdnResult<(Vec<StorageItemDetail<Self::ItemId>>, PathBuf)>;
    }

    pub enum FileSystemItem {
        Dir(DirObject),
        File(FileObject),
    }

    impl FileSystemItem {
        fn name(&self) -> &str {
            match self {
                FileSystemItem::Dir(dir_object) => dir_object.name.as_str(),
                FileSystemItem::File(file_object) => file_object.name.as_str(),
            }
        }
    }

    #[async_trait::async_trait]
    pub trait FileSystemReader<D: FileSystemDirReader, F: FileSystemFileReader>:
        Send + Sync + Sized + Clone
    {
        async fn info(&self, path: &std::path::Path) -> NdnResult<FileSystemItem>;
        async fn open_dir(&self, path: &std::path::Path) -> NdnResult<D>;
        async fn open_file(&self, path: &std::path::Path) -> NdnResult<F>;
    }

    #[async_trait::async_trait]
    pub trait FileSystemDirReader: Send + Sync + Sized {
        async fn next(&self, limit: Option<u64>) -> NdnResult<Vec<FileSystemItem>>;
    }

    #[async_trait::async_trait]
    pub trait FileSystemFileReader: Send + Sync + Sized {
        async fn read_chunk(&self, offset: SeekFrom, limit: Option<u64>) -> NdnResult<Vec<u8>>;
    }

    #[async_trait::async_trait]
    pub trait FileSystemWriter<W: FileSystemFileWriter>: Send + Sync + Sized {
        async fn create_dir_all(&self, dir_path: &Path) -> NdnResult<()>;
        async fn create_dir(&self, dir: &DirObject, parent_path: &Path) -> NdnResult<()>;
        async fn open_file(&self, file_path: &Path) -> NdnResult<W>;
    }

    #[async_trait::async_trait]
    pub trait FileSystemFileWriter: Send + Sync + Sized {
        async fn length(&self) -> NdnResult<u64>;
        async fn write_chunk(&self, chunk_data: &[u8], offset: SeekFrom) -> NdnResult<()>;
        async fn truncate(&self, offset: SeekFrom) -> NdnResult<()>;
        async fn read_chunk(&self, offset: SeekFrom, limit: u64) -> NdnResult<Vec<u8>>;
    }

    #[async_trait::async_trait]
    pub trait NdnWriter: Send + Sync + Sized + Clone {
        // async fn push_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<Vec<ObjId>>; // lost child-obj-list
        async fn push_chunk(
            &self,
            chunk_id: &ChunkId,
            file_path: &Path,
            offset: SeekFrom,
            chunk_size: u64,
            chunk_data: Option<Vec<u8>>,
        ) -> NdnResult<()>;
        // async fn push_container(&self, container_id: &ObjId) -> NdnResult<Vec<ObjId>>; // lost child-obj-list
        async fn ignore(&self, obj_id: &ObjId) -> NdnResult<()>;
    }

    #[async_trait::async_trait]
    pub trait NdnReader: Send + Sync + Sized {
        // async fn get_object(&self, obj_id: &ObjId) -> NdnResult<Value>;
        async fn get_chunk(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>>;
        // async fn get_container(&self, container_id: &ObjId) -> NdnResult<Value>;
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
        storage: S,
        chunk_size: u64,
    ) -> NdnResult<S::ItemId> {
        let insert_new_item = async |item: FileSystemItem,
                                     parent_path: &Path,
                                     storage: &S,
                                     depth: u64,
                                     parent_item_id: Option<S::ItemId>|
               -> NdnResult<StorageItemDetail<S::ItemId>> {
            let detail = match item {
                FileSystemItem::Dir(dir_object) => {
                    let detail = storage
                        .create_new_item(
                            &StorageItem::Dir(dir_object),
                            depth,
                            parent_path,
                            parent_item_id,
                        )
                        .await?;
                    detail
                }
                FileSystemItem::File(file_object) => {
                    let file_path = parent_path.join(file_object.name.as_str());
                    let file_size = file_object.size;
                    let file_reader = reader
                        .open_file(parent_path.join(file_object.name.as_str()).as_path())
                        .await?;
                    let mut detail = storage
                        .create_new_item(
                            &StorageItem::File(FileStorageItem {
                                obj: file_object,
                                chunk_size: Some(chunk_size),
                            }),
                            depth,
                            parent_path,
                            parent_item_id.clone(),
                        )
                        .await?;
                    let chunk_size = match &detail.item {
                        StorageItem::File(file_storage_item) => {
                            file_storage_item.chunk_size.unwrap_or(chunk_size)
                        }
                        _ => Err(NdnError::InvalidObjType(format!(
                            "expect file, got: {:?} in history storage",
                            detail.item.item_type()
                        )))?,
                    };
                    for i in 0..(file_size + chunk_size - 1) / chunk_size {
                        let offset = SeekFrom::Start(i * chunk_size);
                        let chunk_item_detail = storage
                            .create_new_item(
                                &StorageItem::Chunk(ChunkItem {
                                    seq: i as u64,
                                    offset,
                                    chunk_id: None,
                                }),
                                depth + 1,
                                file_path.as_path(),
                                Some(detail.item_id.clone()),
                            )
                            .await?;
                        storage
                            .begin_hash(&ItemSelectKey::ItemId(chunk_item_detail.item_id))
                            .await?;
                    }

                    storage
                        .begin_hash(&ItemSelectKey::ItemId(detail.item_id.clone()))
                        .await?;
                    detail.status = ItemStatus::Hashing;
                    detail
                }
            };
            Ok(detail)
        };

        if let Some(path) = path {
            let none_path = PathBuf::from("");
            let fs_item = reader.info(path).await?;
            insert_new_item(
                fs_item,
                path.parent().unwrap_or(none_path.as_path()),
                &storage,
                0,
                None,
            )
            .await?;
        }

        loop {
            match storage
                .get_uncomplete_dir_or_file_with_max_depth_min_child_seq()
                .await?
            {
                Some(mut item_detail) => {
                    let mut item_status = item_detail.status.clone();
                    loop {
                        match item_status.clone() {
                            ItemStatus::New | ItemStatus::Scanning => {
                                info!(
                                    "new/scanning item: {:?}, parent_path: {}, depth: {}",
                                    item_detail.item.name(),
                                    item_detail.parent_path.display(),
                                    item_detail.depth
                                );
                                assert!(
                                    item_detail.item.is_dir(),
                                    "Only directory will be New/Scanning in storage, File/Chunk has finished scanning when it insert into storage, got: {:?} in history storage",
                                    item_detail.item.item_type()
                                );
                                let dir = item_detail.item.check_dir();
                                info!(
                                    "scan dir: {}, item_id: {:?}",
                                    dir.name,
                                    item_detail.item_id.clone()
                                );
                                let dir_path = item_detail.parent_path.join(dir.name.as_str());
                                let dir_reader = reader.open_dir(dir_path.as_path()).await?;

                                storage.begin_transaction().await?;
                                let mut first_child = None;
                                loop {
                                    let items = dir_reader.next(Some(64)).await?;
                                    let is_finish = items.len() < 64;
                                    for item in items {
                                        let child = insert_new_item(
                                            item,
                                            dir_path.as_path(),
                                            &storage,
                                            item_detail.depth + 1,
                                            Some(item_detail.item_id.clone()),
                                        )
                                        .await?;

                                        first_child.get_or_insert(child);
                                    }
                                    if is_finish {
                                        break;
                                    }
                                }
                                storage
                                    .begin_hash(&ItemSelectKey::ItemId(item_detail.item_id.clone()))
                                    .await?;
                                storage.commit_transaction().await?;
                                // TODO: rollback if error

                                if let Some(first_child) = first_child {
                                    item_detail = first_child;
                                    item_status = item_detail.status.clone();
                                } else {
                                    item_status = ItemStatus::Hashing;
                                }
                            }
                            ItemStatus::Hashing => {
                                info!(
                                    "hashing item: {:?}, path: {}, depth: {}",
                                    item_detail.item.name(),
                                    item_detail.parent_path.display(),
                                    item_detail.depth
                                );

                                match &item_detail.item {
                                    StorageItem::Dir(dir_obj) => {
                                        info!(
                                            "hash dir: {}, item_id: {:?}",
                                            item_detail.item.name().check_name(),
                                            item_detail.item_id
                                        );

                                        let mut dir_obj = dir_obj.clone();

                                        let mut dir_obj_map_builder =
                                            TrieObjectMapBuilder::new(HashMethod::Sha256, None)
                                                .await?;

                                        let mut dir_children_batch_index = 0;
                                        let dir_children_batch_limit = 64;

                                        loop {
                                            let (children, _parent_path) = storage
                                                .list_children_order_by_child_seq(
                                                    &ItemSelectKey::ItemId(
                                                        item_detail.item_id.clone(),
                                                    ),
                                                    Some(
                                                        dir_children_batch_index
                                                            * dir_children_batch_limit,
                                                    ),
                                                    Some(dir_children_batch_limit),
                                                )
                                                .await?;
                                            dir_children_batch_index += 1;
                                            let is_dir_ready =
                                                (children.len() as u64) < dir_children_batch_limit;
                                            for child_detail in children {
                                                assert_eq!(
                                                        child_detail.depth,
                                                        item_detail.depth + 1,
                                                        "child item depth should be one more than dir depth."
                                                    );
                                                dir_obj_map_builder.put_object(
                                                    child_detail.item.name().check_name(),
                                                    child_detail.status.get_obj_id().expect(
                                                        "child dir item should have obj id.",
                                                    ),
                                                )?;
                                            }
                                            if is_dir_ready {
                                                break;
                                            }
                                        }

                                        let dir_obj_map = dir_obj_map_builder.build().await?;
                                        let (dir_obj_map_id, dir_obj_map_str) =
                                            dir_obj_map.calc_obj_id();

                                        dir_obj.content = dir_obj_map_id.to_string();
                                        let (dir_obj_id, dir_obj_str) = dir_obj.gen_obj_id();

                                        let dir_map_header =
                                            serde_json::to_value(dir_obj_map.body()).unwrap();

                                        let dir_map_diff_line = storage
                                            .begin_transfer(
                                                &ItemSelectKey::ItemId(item_detail.item_id.clone()),
                                                &dir_obj_id,
                                                Some(&dir_map_header),
                                            )
                                            .await?
                                            .expect("expect content line for dir object");

                                        item_status = ItemStatus::Transfer(TransferStatusDesc {
                                            obj_id: dir_obj_id,
                                            content_header: Some(dir_map_header),
                                            content_line: Some(dir_map_diff_line),
                                        });
                                    }
                                    StorageItem::File(file_item) => {
                                        info!(
                                            "hash file: {}, item_id: {:?}",
                                            file_item.obj.name, item_detail.item_id
                                        );
                                        // file is hashed, set status to Transfer
                                        let file_path = item_detail
                                            .parent_path
                                            .join(file_item.obj.name.as_str());
                                        let file_reader =
                                            reader.open_file(file_path.as_path()).await?;

                                        let mut chunk_list_builder =
                                            ChunkListBuilder::new(HashMethod::Sha256)
                                                .with_total_size(file_item.obj.size);
                                        let mut chunk_count_in_list = 0;
                                        let file_chunk_size = file_item
                                            .chunk_size
                                            .expect("chunk size should be fix for file");
                                        chunk_list_builder =
                                            chunk_list_builder.with_fixed_size(file_chunk_size);

                                        let chunk_count = (file_item.obj.size + file_chunk_size
                                            - 1)
                                            / file_chunk_size;
                                        let mut batch_count = 0;
                                        let batch_limit = 64;
                                        loop {
                                            let (chunk_items, _) = storage
                                                .list_children_order_by_child_seq(
                                                    &ItemSelectKey::ItemId(
                                                        item_detail.item_id.clone(),
                                                    ),
                                                    Some(batch_count * batch_limit),
                                                    Some(batch_limit),
                                                )
                                                .await?;
                                            batch_count += 1;
                                            let is_file_ready =
                                                (chunk_items.len() as u64) < batch_limit;
                                            for chunk_item_detail in chunk_items {
                                                let chunk_item =
                                                    chunk_item_detail.item.check_chunk();
                                                let chunk_size =
                                                    if chunk_item.seq == chunk_count - 1 {
                                                        file_item.obj.size % file_chunk_size
                                                    } else {
                                                        file_chunk_size
                                                    };
                                                assert_eq!(
                                                        chunk_item_detail.depth,
                                                        item_detail.depth + 1,
                                                        "chunk depth should be one more than file depth."
                                                    );

                                                // hash chunk and transfer it
                                                let chunk_id = match chunk_item_detail.status {
                                                    ItemStatus::New | ItemStatus::Scanning => {
                                                        unreachable!(
                                                                "expect chunk to be hashed, got: {:?} in history storage",
                                                                chunk_item_detail.status
                                                            );
                                                    }
                                                    ItemStatus::Hashing => {
                                                        info!(
                                                                "hashing chunk: {}, seq: {}, path: {}, depth: {}",
                                                                chunk_item_detail.item.name().check_name(),
                                                                chunk_item.seq,
                                                                chunk_item_detail.parent_path.display(),
                                                                chunk_item_detail.depth
                                                            );
                                                        let chunk_data = file_reader
                                                            .read_chunk(
                                                                chunk_item.offset,
                                                                Some(chunk_size),
                                                            )
                                                            .await?;
                                                        let hasher = ChunkHasher::new(None)
                                                            .expect("hash failed.");
                                                        let hash =
                                                            hasher.calc_from_bytes(&chunk_data);
                                                        let chunk_id = ChunkId::from_mix_hash_result_by_hash_method(
                                                                chunk_size,
                                                                &hash,
                                                                HashMethod::Sha256,
                                                            )?;

                                                        let chunk_key = ItemSelectKey::ItemId(
                                                            chunk_item_detail.item_id.clone(),
                                                        );
                                                        storage
                                                            .begin_transfer(
                                                                &chunk_key,
                                                                &chunk_id.to_obj_id(),
                                                                None,
                                                            )
                                                            .await?;

                                                        let line_index = pipeline_writer
                                                            .write_chunk(chunk_id.clone())
                                                            .await?;

                                                        storage
                                                            .complete(&chunk_key, line_index, None)
                                                            .await?;
                                                        ndn_writer
                                                            .push_chunk(
                                                                &chunk_id,
                                                                file_path.as_path(),
                                                                chunk_item.offset,
                                                                chunk_size,
                                                                Some(chunk_data),
                                                            )
                                                            .await?;
                                                        chunk_id
                                                    }
                                                    ItemStatus::Transfer(transfer_status) => {
                                                        info!(
                                                                "transferring chunk: {}, seq: {}, path: {}, depth: {}",
                                                                chunk_item_detail.item.name().check_name(),
                                                                chunk_item.seq,
                                                                chunk_item_detail.parent_path.display(),
                                                                chunk_item_detail.depth
                                                            );
                                                        let chunk_id = ChunkId::from_obj_id(
                                                            &transfer_status.obj_id,
                                                        );

                                                        let chunk_data = file_reader
                                                            .read_chunk(
                                                                chunk_item.offset,
                                                                Some(chunk_size),
                                                            )
                                                            .await?;
                                                        let chunk_key = ItemSelectKey::ItemId(
                                                            chunk_item_detail.item_id.clone(),
                                                        );
                                                        let line_index = pipeline_writer
                                                            .write_chunk(chunk_id.clone())
                                                            .await?;

                                                        storage
                                                            .complete(&chunk_key, line_index, None)
                                                            .await?;
                                                        ndn_writer
                                                            .push_chunk(
                                                                &chunk_id,
                                                                file_path.as_path(),
                                                                chunk_item.offset,
                                                                chunk_size,
                                                                Some(chunk_data),
                                                            )
                                                            .await?;
                                                        chunk_id
                                                    }
                                                    ItemStatus::Complete(complete_status) => {
                                                        info!(
                                                                "complete chunk: {}, seq: {}, path: {}, depth: {}",
                                                                chunk_item_detail.item.name().check_name(),
                                                                chunk_item.seq,
                                                                chunk_item_detail.parent_path.display(),
                                                                chunk_item_detail.depth
                                                            );

                                                        let chunk_id = ChunkId::from_obj_id(
                                                            &complete_status.obj_id,
                                                        );
                                                        // nothing to do, chunk is already complete
                                                        chunk_id
                                                    }
                                                };

                                                assert!(
                                                    (chunk_item.seq == chunk_count - 1
                                                        && chunk_id
                                                            .get_length()
                                                            .expect("chunk id should fix size")
                                                            == file_item.obj.size % chunk_size)
                                                        || chunk_id
                                                            .get_length()
                                                            .expect("chunk id should fix size")
                                                            == chunk_size
                                                );
                                                assert_eq!(chunk_item.seq, chunk_count_in_list);
                                                assert_eq!(
                                                    chunk_item.offset,
                                                    SeekFrom::Start(
                                                        chunk_count_in_list
                                                            * file_item.chunk_size.unwrap_or(0)
                                                    )
                                                );
                                                chunk_list_builder
                                                    .append(chunk_id)
                                                    .expect("add chunk failed");
                                                chunk_count_in_list += 1;
                                            }
                                            if is_file_ready {
                                                break;
                                            }
                                        }

                                        let file_chunk_list = chunk_list_builder.build().await?;
                                        let (file_chunk_list_id, file_chunk_list_str) =
                                            file_chunk_list.calc_obj_id();

                                        let mut file_obj = file_item.obj.clone();
                                        file_obj.content = file_chunk_list_id.to_string();
                                        let (file_obj_id, file_obj_str) = file_obj.gen_obj_id();

                                        let chunk_list_header =
                                            serde_json::to_value(file_chunk_list.body()).unwrap();
                                        let chunk_list_diff_line = storage
                                            .begin_transfer(
                                                &ItemSelectKey::ItemId(item_detail.item_id.clone()),
                                                &file_obj_id,
                                                Some(&chunk_list_header),
                                            )
                                            .await?
                                            .expect(
                                                "file object should be ObjArray after hashing.",
                                            );

                                        item_status = ItemStatus::Transfer(TransferStatusDesc {
                                            obj_id: file_obj_id,
                                            content_header: Some(chunk_list_header),
                                            content_line: Some(chunk_list_diff_line),
                                        });
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
                                info!(
                                    "transferring item: {:?}, path: {}, depth: {}",
                                    item_detail.item.name(),
                                    item_detail.parent_path.display(),
                                    item_detail.depth
                                );

                                let obj_id = transfer_desc.obj_id;
                                let content_header = transfer_desc
                                    .content_header
                                    .expect("transfer status should have content header.");
                                let content_line = transfer_desc
                                    .content_line
                                    .expect("transfer status should have content line.");

                                let (content_index, obj_value, header_index) = match &item_detail
                                    .item
                                {
                                    StorageItem::Dir(dir_item) => {
                                        let dir_map_id =
                                            serde_json::from_value::<TrieObjectMapBody>(
                                                content_header.clone(),
                                            )
                                            .expect(
                                                "file chunk list id should be valid json value.",
                                            )
                                            .calc_obj_id()
                                            .0;

                                        let mut dir_obj = dir_item.clone();
                                        dir_obj.content = dir_map_id.to_string();

                                        let (content_index, header_index) = pipeline_writer
                                            .write_object_map(
                                                dir_map_id,
                                                content_header,
                                                content_line.check_map(),
                                                None,
                                                Some(item_detail.depth as u32),
                                            )
                                            .await?;
                                        (
                                            content_index,
                                            serde_json::to_value(&dir_obj)
                                                .expect("dir object should be valid json value."),
                                            header_index,
                                        )
                                    }
                                    StorageItem::File(file_item) => {
                                        let file_chunk_list_id = serde_json::from_value::<
                                            ChunkListBody,
                                        >(
                                            content_header.clone()
                                        )
                                        .expect("file chunk list id should be valid json value.")
                                        .calc_obj_id()
                                        .0;

                                        let mut file_obj = file_item.obj.clone();
                                        file_obj.content = file_chunk_list_id.to_string();
                                        let (content_index, header_index) = pipeline_writer
                                            .write_object_array(
                                                file_chunk_list_id,
                                                content_header,
                                                content_line.check_array(),
                                                None,
                                                Some(item_detail.depth as u32),
                                            )
                                            .await?;

                                        (
                                            content_index,
                                            serde_json::to_value(&file_obj)
                                                .expect("file object should be valid json value."),
                                            header_index,
                                        )
                                    }
                                    StorageItem::Chunk(chunk_item) => {
                                        // chunk should be handled in file transfer
                                        unreachable!(
                                            "expect file or dir, got: {:?} in history storage",
                                            item_detail.item.item_type()
                                        );
                                    }
                                };

                                let obj_index = pipeline_writer
                                    .write_object(obj_id, obj_value, None)
                                    .await?;

                                storage
                                    .complete(
                                        &ItemSelectKey::ItemId(item_detail.item_id.clone()),
                                        obj_index.index(),
                                        Some(ContainerLineIndex {
                                            index: content_index.index(),
                                            header_index,
                                        }),
                                    )
                                    .await?;
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

        

        let root_item = storage.get_root().await?;
        assert_eq!(root_item.depth, 0, "root item depth should be 0.");
        if let Some(path) = path {
            match root_item.item.name() {
                StorageItemNameRef::Name(name) => assert_eq!(
                    root_item.parent_path.join(name).as_path(),
                    path,
                    "root item parent path should match the scan path."
                ),
                StorageItemNameRef::ChunkSeq(_) => unreachable!(
                    "root item name should not be ChunkSeq, got: {:?}",
                    root_item.item.name()
                ),
            }
        }

        Ok(root_item.item_id)
    }
}

mod dir_restore {
    use std::{
        fs,
        io::SeekFrom,
        path::{Path, PathBuf},
        time::Duration,
    };

    use crate::{
        build_named_object_by_json, chunk, copy_chunk,
        packed_obj_pipeline::demo::{
            dir_backup::{
                ContainerLineIndex, DirObject, FileSystemFileWriter, FileSystemWriter, NdnReader,
                StorageItemName,
            },
            CollectionHeader, Line, LineIndex, LineIndexWithRelation, ObjArrayLine,
            ObjArraySubObjIndex, ObjMapLine, Reader,
        },
        ChunkHasher, ChunkId, ChunkListBody, ChunkListBuilder, FileObject, HashMethod, NdnError,
        NdnResult, ObjId, OBJ_TYPE_DIR, OBJ_TYPE_FILE,
    };

    pub enum RestoreItemSelectKey<IT> {
        ItemId(IT),
        FsItemId(IT),
        FsChild(IT, StorageItemName), // <parent_fs_item_id, child_name>
    }

    pub enum RestoreItemStatus {
        New,
        CheckHashing, // all obj-ids of child are ready
        Transfer(PathBuf),
        Complete(PathBuf),
    }

    impl RestoreItemStatus {
        pub fn is_new(&self) -> bool {
            matches!(self, RestoreItemStatus::New)
        }
        pub fn is_check_hashing(&self) -> bool {
            matches!(self, RestoreItemStatus::CheckHashing)
        }
        pub fn is_transfer(&self) -> bool {
            matches!(self, RestoreItemStatus::Transfer(_))
        }
        pub fn is_complete(&self) -> bool {
            matches!(self, RestoreItemStatus::Complete(_))
        }
        pub fn check_check_hashing(&self) {
            match self {
                RestoreItemStatus::New => {
                    unreachable!("expect item to be in CheckHashing status, got New status");
                }
                RestoreItemStatus::CheckHashing => {
                    // nothing to do here
                }
                RestoreItemStatus::Transfer(_) => {
                    unreachable!("expect item to be in CheckHashing status, got Transfer status");
                }
                RestoreItemStatus::Complete(path) => {
                    unreachable!(
                        "expect item to be in CheckHashing status, got Complete status: {}",
                        path.display()
                    );
                }
            }
        }
        pub fn check_transfer(&self) -> &Path {
            match self {
                RestoreItemStatus::New => {
                    unreachable!("expect item to be in Transfer status, got New status");
                }
                RestoreItemStatus::CheckHashing => {
                    unreachable!("expect item to be in Transfer status, got CheckHashing status");
                }
                RestoreItemStatus::Transfer(path) => path,
                RestoreItemStatus::Complete(path) => {
                    unreachable!("expect item to be in Transfer status, got Complete status");
                }
            }
        }

        pub fn check_complete(&self) -> &Path {
            match self {
                RestoreItemStatus::New => {
                    unreachable!("expect item to be in Complete status, got New status");
                }
                RestoreItemStatus::CheckHashing => {
                    unreachable!("expect item to be in Complete status, got CheckHashing status");
                }
                RestoreItemStatus::Transfer(_) => {
                    unreachable!("expect item to be in Complete status, got Transfer status");
                }
                RestoreItemStatus::Complete(path) => path,
            }
        }

        pub fn save_path(&self) -> Option<&Path> {
            match self {
                RestoreItemStatus::New => None,
                RestoreItemStatus::CheckHashing => None,
                RestoreItemStatus::Transfer(path) => Some(path),
                RestoreItemStatus::Complete(path) => Some(path),
            }
        }
    }

    pub struct LineStorageItem<IT> {
        pub item_id: IT,
        pub fs_item_id: Option<IT>,
        pub line: Option<(Line, LineIndex)>,
        pub header_line: Option<(Line, LineIndex)>,
        pub obj_id: Option<ObjId>,
        pub obj_status: RestoreItemStatus,
        pub status: RestoreItemStatus,
    }

    pub struct ChildInfo<IT> {
        pub name: Option<StorageItemName>,
        pub child_line_index: Option<LineIndex>,
        pub child_obj_id: Option<ObjId>,
        pub child_item_id: Option<IT>,
    }

    #[async_trait::async_trait]
    pub trait RestoreStorage: Clone + Send + Sync {
        type ItemId: std::fmt::Debug + Clone + Send + Sync + 'static;

        async fn create_new_line_item(
            &self,
            line: Option<(Line, LineIndex)>,
            header_line: Option<(Line, LineIndex)>,
            obj_id: Option<ObjId>,
        ) -> NdnResult<LineStorageItem<Self::ItemId>>;

        async fn add_children(
            &self,
            parent_item_id: Option<&Self::ItemId>, // None for root item
            children: &[ChildInfo<Self::ItemId>],
        ) -> NdnResult<Self::ItemId>;

        async fn begin_transfer(
            &self,
            key: &RestoreItemSelectKey<Self::ItemId>,
            save_path: &Path,
        ) -> NdnResult<Option<ContainerLineIndex>>;

        async fn complete(&self, key: &RestoreItemSelectKey<Self::ItemId>) -> NdnResult<()>;

        async fn set_transfer_to_complete_with_all_children_complete(&self) -> NdnResult<u64>; // changed lines count

        async fn select_children_check_hashing_with_parent_transfer(
            &self,
            offset: Option<u64>,
            limit: Option<u64>,
        ) -> NdnResult<Vec<(LineStorageItem<Self::ItemId>, LineStorageItem<Self::ItemId>)>>; // <item, parent-item>

        async fn select_chunklist_transfer(
            &self,
            offset: Option<u64>,
            limit: Option<u64>,
        ) -> NdnResult<Vec<LineStorageItem<Self::ItemId>>>;

        async fn list_children_order_by_child_seq(
            &self,
            key: &RestoreItemSelectKey<Self::ItemId>,
            offset: Option<u64>,
            limit: Option<u64>,
        ) -> NdnResult<Vec<LineStorageItem<Self::ItemId>>>;

        async fn get_root(&self) -> NdnResult<Option<LineStorageItem<Self::ItemId>>>;

        async fn begin_transaction(&self) -> NdnResult<()>;
        async fn commit_transaction(&self) -> NdnResult<()>;
        async fn rollback_transaction(&self) -> NdnResult<()>;
    }

    pub async fn ndn_to_file_system<
        S: RestoreStorage + 'static,
        FFW: FileSystemFileWriter + 'static,
        F: FileSystemWriter<FFW> + Clone + 'static,
        PR: Reader + Clone + Send + Sync + 'static,
        NR: NdnReader + Clone + 'static,
    >(
        dir_path_root_obj_id: Option<(&Path, &ObjId)>, // <root-path, root-obj-id>, None for continue
        writer: F,
        mut pipeline_reader: PR,
        ndn_reader: NR,
        storage: S,
    ) -> NdnResult<()> {
        if let Some((path, root_obj_id)) = dir_path_root_obj_id {
            info!(
                "scan path: {}, root obj id: {}",
                path.display(),
                root_obj_id
            );
            writer.create_dir_all(path).await?;
            storage
                .add_children(
                    None,
                    &[ChildInfo {
                        name: None,
                        child_line_index: None,
                        child_obj_id: Some(root_obj_id.clone()),
                        child_item_id: None,
                    }],
                )
                .await?;
        }

        let pipeline_task_handle = {
            let storage = storage.clone();
            let mut pipeline_reader = pipeline_reader.clone();

            let mut read_pipeline = async move |storage: S| -> NdnResult<()> {
                info!("start ndn to file system task...");

                let create_new_item = async |line: Line, line_index: LineIndex| -> NdnResult<()> {
                    /**
                     * 1. 把行添加进数据库
                     * 2. 如果这个行是个容器，把容器里的内容展开，把这些子对象和这个行建立父子关系
                     * 3. 如果这个行是个文件，把行解析成FileObject，FileObject.content是一个ChunkList的obj-id，把这个obj-id添加为这个行的子对象
                     * 4. 如果这个行是个目录，把行解析成DirObject，DirObject.content是一个TrieObjectMap的obj-id，把这个obj-id添加为这个行的子对象
                     * 5. 如果这个行是个header，把该行传入header_line，否则传入line参数
                     */
                    match line {
                        Line::Chunk(chunk_id) => {
                            // 1. 保存行
                            let obj_id = chunk_id.to_obj_id();
                            let _item = storage
                                .create_new_line_item(
                                    Some((Line::Chunk(chunk_id.clone()), line_index)),
                                    None,
                                    Some(obj_id),
                                )
                                .await?;
                            // 2. Chunk 没有子对象
                        }
                        Line::Obj { id, obj } => {
                            // 1. 保存对象行
                            let file_or_dir_obj_id = id.clone();
                            let item = storage
                                .create_new_line_item(
                                    Some((
                                        Line::Obj {
                                            id: id.clone(),
                                            obj: obj.clone(),
                                        },
                                        line_index,
                                    )),
                                    None,
                                    Some(file_or_dir_obj_id.clone()),
                                )
                                .await?;

                            // 3 / 4. 如果是文件或目录，解析其 content，加入一个子对象引用
                            let mut content_obj_id = None;
                            if id.obj_type.as_str() == OBJ_TYPE_FILE {
                                let file_obj = serde_json::from_value::<FileObject>(obj)
                                    .map_err(|err| NdnError::DecodeError(err.to_string()))?;
                                content_obj_id = Some(ObjId::try_from(file_obj.content.as_str())?);
                            } else if id.obj_type.as_str() == OBJ_TYPE_DIR {
                                let dir_obj = serde_json::from_value::<DirObject>(obj)
                                    .map_err(|err| NdnError::DecodeError(err.to_string()))?;
                                content_obj_id = Some(ObjId::try_from(dir_obj.content.as_str())?);
                            }

                            if let Some(content_obj_id) = content_obj_id {
                                // 2. 添加子对象关系
                                storage
                                    .add_children(
                                        Some(&item.item_id),
                                        &[ChildInfo {
                                            name: None,
                                            child_line_index: None,
                                            child_obj_id: Some(content_obj_id),
                                            child_item_id: None,
                                        }],
                                    )
                                    .await?;
                            }
                        }
                        Line::ObjArray { header, content } => {
                            // 取得容器自身 obj_id（如果已经在该行里）
                            let mut obj_id_opt: Option<ObjId> = None;
                            if let CollectionHeader::Header { id, .. } = &header {
                                obj_id_opt = Some(id.clone());
                            }
                            let item = storage
                                .create_new_line_item(
                                    Some((
                                        Line::ObjArray {
                                            header: header.clone(),
                                            content: content.clone(),
                                        },
                                        line_index,
                                    )),
                                    None,
                                    obj_id_opt.clone(),
                                )
                                .await?;

                            // 2. 展开子对象
                            let child_infos = match content {
                                ObjArrayLine::Memory(obj_list) => {
                                    let mut child_infos = Vec::with_capacity(obj_list.len());
                                    for seq in 0..obj_list.len() {
                                        let child_obj_id = obj_list
                                            .get(seq)
                                            .expect("obj_list should have enough items")
                                            .clone();
                                        child_infos.push(ChildInfo {
                                            name: Some(StorageItemName::ChunkSeq(seq as u64)),
                                            child_line_index: None,
                                            child_obj_id: Some(child_obj_id),
                                            child_item_id: None,
                                        });
                                    }
                                    child_infos
                                }
                                ObjArrayLine::File(file_obj_list) => {
                                    unimplemented!("FileObjList not implemented in demo");
                                }
                                ObjArrayLine::Lines(lines) => {
                                    let mut child_infos = Vec::with_capacity(lines.len());
                                    let mut next_seq = 0;
                                    for sub_obj_index in lines {
                                        match sub_obj_index {
                                            ObjArraySubObjIndex::Index(child_index) => {
                                                child_infos.push(ChildInfo {
                                                    name: Some(StorageItemName::ChunkSeq(next_seq)),
                                                    child_line_index: Some(child_index),
                                                    child_obj_id: None,
                                                    child_item_id: None,
                                                });
                                                next_seq += 1;
                                            }
                                            ObjArraySubObjIndex::Range(range) => {
                                                for child_index in range {
                                                    child_infos.push(ChildInfo {
                                                        name: Some(StorageItemName::ChunkSeq(
                                                            next_seq,
                                                        )),
                                                        child_line_index: Some(child_index),
                                                        child_obj_id: None,
                                                        child_item_id: None,
                                                    });
                                                    next_seq += 1;
                                                }
                                            }
                                        }
                                    }
                                    child_infos
                                }
                                ObjArrayLine::Diff {
                                    base_array,
                                    actions,
                                } => {
                                    unimplemented!("ObjArrayLine::Diff not implemented in demo");
                                }
                            };

                            if !child_infos.is_empty() {
                                storage
                                    .add_children(Some(&item.item_id), &child_infos)
                                    .await?;
                            }
                        }
                        Line::ObjMap { header, content } => {
                            // 取得容器自身 obj_id（如果已经在该行里）
                            let mut obj_id_opt: Option<ObjId> = None;
                            if let CollectionHeader::Header { id, .. } = &header {
                                obj_id_opt = Some(id.clone());
                            }
                            let item = storage
                                .create_new_line_item(
                                    Some((
                                        Line::ObjMap {
                                            header: header.clone(),
                                            content: content.clone(),
                                        },
                                        line_index,
                                    )),
                                    None,
                                    obj_id_opt.clone(),
                                )
                                .await?;

                            // 2. 展开子对象
                            let child_infos = match content {
                                ObjMapLine::Memory(obj_map) => {
                                    let mut child_infos = Vec::with_capacity(obj_map.len());
                                    for (key, obj_id) in obj_map.iter() {
                                        child_infos.push(ChildInfo {
                                            name: Some(StorageItemName::Name(key.clone())),
                                            child_line_index: None,
                                            child_obj_id: Some(obj_id.clone()),
                                            child_item_id: None,
                                        });
                                    }
                                    child_infos
                                }
                                ObjMapLine::File(file_obj_map) => {
                                    unimplemented!("FileObjMap not implemented in demo");
                                }
                                ObjMapLine::MemoryWithIndex(lines) => {
                                    let mut child_infos = Vec::with_capacity(lines.len());
                                    for (key, sub_obj_index) in lines {
                                        child_infos.push(ChildInfo {
                                            name: Some(StorageItemName::Name(key.clone())),
                                            child_line_index: Some(sub_obj_index),
                                            child_obj_id: None,
                                            child_item_id: None,
                                        });
                                    }
                                    child_infos
                                }
                                ObjMapLine::Diff { base_map, actions } => {
                                    unimplemented!("ObjMapLine::Diff not implemented in demo");
                                }
                            };

                            if !child_infos.is_empty() {
                                storage
                                    .add_children(Some(&item.item_id), &child_infos)
                                    .await?;
                            }
                        }
                        Line::ObjHeader {
                            obj_index,
                            id,
                            header,
                        } => {
                            // 5. 头部行：以 header_line 方式存储
                            let _item = storage
                                .create_new_line_item(
                                    None,
                                    Some((
                                        Line::ObjHeader {
                                            obj_index,
                                            id: id.clone(),
                                            header: header.clone(),
                                        },
                                        line_index,
                                    )),
                                    Some(id.clone()),
                                )
                                .await?;
                            // 子对象关系由对应内容行处理
                        }
                        Line::Index { .. } => {
                            // 索引行：当前 demo 不处理，简单保存
                            // nothing to do here
                        }
                        Line::Header(_h) => {
                            // Pipeline 顶层头部：保存，无子对象
                            // TODO: check type
                        }
                        Line::RebuildObj { .. } => {
                            // 重建指示行：这里不入库或简单忽略
                            // TODO: check root
                        }
                    }

                    Ok(())
                };

                loop {
                    let line = pipeline_reader.next_line().await?;
                    match line {
                        Some((line, line_index)) => {
                            storage.begin_transaction().await?;
                            match line_index {
                                LineIndexWithRelation::Real(line_index) => {
                                    create_new_item(line, line_index).await?;
                                }
                                LineIndexWithRelation::Ref(line_index) => {
                                    info!(
                                        "line index is a reference: {:?}, line: {:?}",
                                        line_index, line
                                    );
                                    // 可能是向writer主动请求的对象，暂时先不支持
                                    continue;
                                }
                            }
                            storage.commit_transaction().await?;
                            // TODO: rollback on error
                        }
                        None => break,
                    }
                }

                Ok(())
            };

            tokio::spawn(async move { read_pipeline(storage).await })
        };

        let fs_dir_build_task_handle = {
            let storage = storage.clone();
            let pipeline_reader = pipeline_reader.clone();
            let ndn_reader = ndn_reader.clone();

            async fn transfer_file<
                S: RestoreStorage + 'static,
                FFW: FileSystemFileWriter + 'static,
                F: FileSystemWriter<FFW> + 'static,
                NR: NdnReader + Clone + 'static,
            >(
                storage: S,
                ndn_reader: NR,
                writer: F,
            ) -> NdnResult<()> {
                // TODO: in batch
                let chunklist_list = storage.select_chunklist_transfer(None, None).await?;
                for chunk_list_item in chunklist_list {
                    match &chunk_list_item.obj_status {
                        RestoreItemStatus::New => {
                            // nothing to do here
                            unreachable!(
                                "chunklist item should not be in New status for transfer child, item id: {:?}",
                                chunk_list_item.item_id
                            );
                        }
                        RestoreItemStatus::CheckHashing => {
                            // nothing to do here
                            unreachable!(
                                "chunklist item should not be in CheckHashing status for transfer child, item id: {:?}",
                                chunk_list_item.item_id
                            );
                        }
                        RestoreItemStatus::Transfer(save_path) => {
                            info!(
                                "transfer chunklist: {:?}, path: {}",
                                chunk_list_item.obj_id,
                                save_path.display()
                            );
                            if let Some((line, content_index)) = &chunk_list_item.line {
                                let chunk_list_header =
                                    if let Line::ObjArray { header, content } = line {
                                        match header {
                                            CollectionHeader::Header { id, header } => {
                                                // check if the id matches the chunk list obj_id
                                                header
                                            }
                                            CollectionHeader::Index(_) => {
                                                if let Line::ObjHeader {
                                                    obj_index,
                                                    id,
                                                    header,
                                                } = &chunk_list_item
                                                    .header_line
                                                    .as_ref()
                                                    .expect("header line should exist")
                                                    .0
                                                {
                                                    // check if the obj_index matches the chunk list obj_id
                                                    assert_eq!(obj_index, content_index);
                                                    header
                                                } else {
                                                    unreachable!(
                                                        "expect chunk list header to be ObjHeader"
                                                    );
                                                }
                                            }
                                        }
                                    } else {
                                        unreachable!("expect chunk list line, got: {:?}", line);
                                    };

                                let chunk_list = ChunkListBuilder::open(chunk_list_header.clone())
                                    .await?
                                    .build()
                                    .await?;

                                if let Some(parent) = save_path.parent() {
                                    info!("create dir: {}", parent.display());
                                    writer.create_dir_all(parent).await?;
                                }
                                let file_writer = writer.open_file(save_path.as_path()).await?;
                                let mut file_size = file_writer.length().await?;
                                let mut write_pos = 0;
                                let mut write_chunk_seq = 0;
                                // check existed chunks
                                if file_size > 0 {
                                    for chunk_seq in 0..chunk_list.len() {
                                        let chunk_id = chunk_list
                                            .get_chunk(chunk_seq as usize)
                                            .expect("chunk id should exist")
                                            .expect("chunk id should not be None");
                                        let chunk_size = chunk_id
                                            .get_length()
                                            .expect("chunk id should have length");
                                        if write_pos + chunk_size > file_size {
                                            if write_pos < file_size {
                                                file_writer
                                                    .truncate(SeekFrom::Start(write_pos))
                                                    .await?;
                                            }
                                            break;
                                        }
                                        let chunk_data = file_writer
                                            .read_chunk(
                                                std::io::SeekFrom::Start(write_pos),
                                                chunk_size,
                                            )
                                            .await?;
                                        let mut hasher = ChunkHasher::new_with_hash_method(
                                            chunk_id.chunk_type.to_hash_method()?,
                                        )
                                        .expect("hash failed.");
                                        assert!(
                                            chunk_id.chunk_type.is_mix(),
                                            "chunk data length should match the chunk id length.",
                                        );
                                        let chunk_id_from_file = hasher
                                            .calc_mix_chunk_id_from_bytes(chunk_data.as_slice())?;
                                        if chunk_id_from_file != chunk_id {
                                            file_writer
                                                .truncate(SeekFrom::Start(write_pos))
                                                .await?;
                                            break;
                                        }
                                        write_chunk_seq = chunk_seq;
                                        write_pos += chunk_size;
                                    }
                                }

                                for chunk_seq in write_chunk_seq..chunk_list.len() {
                                    let chunk_id = chunk_list
                                        .get_chunk(chunk_seq as usize)
                                        .expect("chunk id should exist")
                                        .expect("chunk id should not be None");
                                    let chunk_size =
                                        chunk_id.get_length().expect("chunk id should have length");
                                    let chunk_data = ndn_reader.get_chunk(&chunk_id).await?;
                                    assert_eq!(
                                        chunk_data.len() as u64,
                                        chunk_size,
                                        "chunk data length should match the chunk id length."
                                    );
                                    file_writer
                                        .write_chunk(
                                            chunk_data.as_slice(),
                                            SeekFrom::Start(write_pos),
                                        )
                                        .await?;
                                    write_pos += chunk_size;
                                }
                                info!(
                                    "chunklist transfer complete: {}, size: {}",
                                    save_path.display(),
                                    write_pos
                                );

                                let target_path = chunk_list_item.status.check_transfer();
                                if save_path.as_path() != target_path {
                                    // if the save path is not the final path, we need to move it
                                    fs::copy(save_path.as_path(), target_path)
                                        .map_err(|err| NdnError::IoError(err.to_string()))?;
                                }

                                storage
                                    .complete(&RestoreItemSelectKey::FsItemId(
                                        chunk_list_item.fs_item_id.clone().unwrap(),
                                    ))
                                    .await?;
                                storage
                                    .set_transfer_to_complete_with_all_children_complete()
                                    .await?;
                            } else {
                                unreachable!(
                                    "object line should be some when it is in Transfer status."
                                );
                            }
                        }
                        RestoreItemStatus::Complete(path) => {
                            info!("complete chunklist transfer: {}", path.display());
                            // copy it to the final path
                            let target_path = chunk_list_item.status.check_transfer();
                            fs::copy(path, target_path)
                                .map_err(|err| NdnError::IoError(err.to_string()))?;
                            storage
                                .complete(&RestoreItemSelectKey::FsItemId(
                                    chunk_list_item.fs_item_id.clone().unwrap(),
                                ))
                                .await?;
                            storage
                                .set_transfer_to_complete_with_all_children_complete()
                                .await?;
                        }
                    }
                }
                Ok(())
            };

            async fn check_hash<S: RestoreStorage + 'static>(storage: S) -> NdnResult<()> {
                info!("check hash for items in CheckHashing status...");

                let items = storage
                    .select_children_check_hashing_with_parent_transfer(None, None)
                    .await?;
                for (item, parent_item) in items.iter() {
                    item.status.check_check_hashing();
                    let parent_path = parent_item.status.check_transfer();

                    if let Some((line, _)) = &item.line {
                        match line {
                            Line::Obj { id, obj } => {
                                if id.obj_type.as_str() == OBJ_TYPE_FILE {
                                    let file_obj = serde_json::from_value::<FileObject>(
                                        obj.clone(),
                                    )
                                    .map_err(|err| NdnError::DecodeError(err.to_string()))?;
                                    let (file_id, _) = file_obj.gen_obj_id();
                                    if &file_id != id {
                                        return Err(NdnError::InvalidId(format!(
                                            "file id mismatch: {}, expected: {}",
                                            file_id, id
                                        )));
                                    }
                                    storage
                                        .begin_transfer(
                                            &RestoreItemSelectKey::FsItemId(
                                                item.fs_item_id.clone().unwrap(),
                                            ),
                                            &parent_path.join(file_obj.name.as_str()).as_path(),
                                        )
                                        .await?;
                                } else if id.obj_type.as_str() == OBJ_TYPE_DIR {
                                    let dir_obj = serde_json::from_value::<DirObject>(obj.clone())
                                        .map_err(|err| NdnError::DecodeError(err.to_string()))?;
                                    let (dir_obj_id, _) = dir_obj.gen_obj_id();
                                    if &dir_obj_id != id {
                                        return Err(NdnError::InvalidId(format!(
                                            "dir id mismatch: {}, expected: {}",
                                            dir_obj_id, id
                                        )));
                                    }
                                    storage
                                        .begin_transfer(
                                            &RestoreItemSelectKey::FsItemId(
                                                item.fs_item_id.clone().unwrap(),
                                            ),
                                            &parent_path.join(dir_obj.name.as_str()).as_path(),
                                        )
                                        .await?;
                                } else {
                                    warn!(
                                        "unsupported object type for check hashing: {}, item id: {:?}",
                                        id.obj_type, item.item_id
                                    );
                                }
                            }
                            Line::Chunk(chunk_id) => {
                                // nothing to do here, chunk id is already in the item
                            }
                            Line::ObjArray { header, content } => {
                                // ObjArrayLine: check the header and content
                                let obj_array_id = item.obj_id.as_ref().unwrap();
                                let chunk_list_body = match header {
                                    CollectionHeader::Header { id, header } => {
                                        assert_eq!(
                                            id, obj_array_id,
                                            "obj array id mismatch: {}, expected: {}",
                                            id, obj_array_id
                                        );
                                        header
                                    }
                                    CollectionHeader::Index(_) => {
                                        if let Line::ObjHeader {
                                            obj_index,
                                            id,
                                            header,
                                        } = &item
                                            .header_line
                                            .as_ref()
                                            .expect("header line should exist")
                                            .0
                                        {
                                            assert_eq!(
                                                id, obj_array_id,
                                                "obj array id mismatch: {}, expected: {}",
                                                id, obj_array_id
                                            );
                                            header
                                        } else {
                                            unreachable!(
                                                "expect chunk list header to be Header, got: {:?}",
                                                item.header_line
                                            )
                                        }
                                    }
                                };

                                let chunk_list_body = serde_json::from_value::<ChunkListBody>(
                                    chunk_list_body.clone(),
                                )
                                .map_err(|err| NdnError::DecodeError(err.to_string()))?;
                                let mut chunk_list_builder =
                                    ChunkListBuilder::new(HashMethod::Sha256);
                                if let Some(fix_size) = chunk_list_body.fix_size {
                                    chunk_list_builder =
                                        chunk_list_builder.with_fixed_size(fix_size);
                                }
                                chunk_list_builder =
                                    chunk_list_builder.with_total_size(chunk_list_body.total_size);
                                let mut child_obj_ids_from_storage = Vec::new();
                                let child_obj_ids = match content {
                                    ObjArrayLine::Memory(obj_ids) => obj_ids,
                                    _ => {
                                        let children = storage
                                            .list_children_order_by_child_seq(
                                                &RestoreItemSelectKey::FsItemId(
                                                    item.fs_item_id
                                                        .clone()
                                                        .expect("fs item id should exist"),
                                                ),
                                                None,
                                                None,
                                            )
                                            .await?;
                                        child_obj_ids_from_storage = children
                                            .into_iter()
                                            .map(|child| {
                                                child.obj_id.expect("child obj id should exist")
                                            })
                                            .collect();
                                        &child_obj_ids_from_storage
                                    }
                                };

                                for obj_id in child_obj_ids {
                                    if obj_id.is_chunk() {
                                        chunk_list_builder.append(ChunkId::from_obj_id(obj_id))?;
                                    } else {
                                        return Err(NdnError::InvalidId(format!(
                                            "obj array item is not a chunk id: {}",
                                            obj_id
                                        )));
                                    }
                                }
                                let chunk_list = chunk_list_builder.build().await?;
                                let (chunk_list_id, _) = chunk_list.calc_obj_id();
                                if &chunk_list_id != obj_array_id {
                                    return Err(NdnError::InvalidId(format!(
                                        "obj array id mismatch: {}, expected: {}",
                                        chunk_list_id, obj_array_id
                                    )));
                                }
                                storage
                                    .begin_transfer(
                                        &RestoreItemSelectKey::FsItemId(
                                            item.fs_item_id.clone().unwrap(),
                                        ),
                                        parent_path, // the path is same as the parent path(FileObject)
                                    )
                                    .await?;
                            }
                            Line::ObjMap { header, content } => {
                                {
                                    // ObjMapLine: verify header (TrieObjectMapBody) id matches, then mark ready to transfer
                                    let obj_map_id = item
                                        .obj_id
                                        .as_ref()
                                        .expect("object map item should have its obj_id");
                                    let map_body_value = match header {
                                        CollectionHeader::Header { id, header } => {
                                            assert_eq!(
                                                id, obj_map_id,
                                                "obj map id mismatch: {}, expected: {}",
                                                id, obj_map_id
                                            );
                                            header
                                        }
                                        CollectionHeader::Index(_) => {
                                            if let Some((Line::ObjHeader { id, header, .. }, _)) =
                                                &item.header_line
                                            {
                                                assert_eq!(
                                                    id, obj_map_id,
                                                    "obj map id mismatch: {}, expected: {}",
                                                    id, obj_map_id
                                                );
                                                header
                                            } else {
                                                unreachable!(
                                                    "expect ObjHeader for map header line"
                                                );
                                            }
                                        }
                                    };

                                    let map_body =
                                        serde_json::from_value::<crate::TrieObjectMapBody>(
                                            map_body_value.clone(),
                                        )
                                        .map_err(|e| NdnError::DecodeError(e.to_string()))?;

                                    let (calc_id, _) = map_body.calc_obj_id();
                                    if &calc_id != obj_map_id {
                                        return Err(NdnError::InvalidId(format!(
                                            "obj map id mismatch: {}, expected: {}",
                                            calc_id, obj_map_id
                                        )));
                                    }

                                    // Rebuild TrieObjectMap from current children to double-check final ObjId consistency.
                                    {
                                        // Collect children (files/sub-dirs) of this directory map
                                        let children = storage
                                            .list_children_order_by_child_seq(
                                                &RestoreItemSelectKey::FsItemId(
                                                    item.fs_item_id.clone().expect(
                                                        "fs item id should exist for TrieObjectMap",
                                                    ),
                                                ),
                                                None,
                                                None,
                                            )
                                            .await?;

                                        // Build a new trie map from children's (name -> obj_id)
                                        let mut rebuild_builder = crate::TrieObjectMapBuilder::new(
                                            HashMethod::Sha256,
                                            None,
                                        )
                                        .await?;

                                        for child in children {
                                            // We only care about object lines (files/dirs). Skip if line not yet arrived.
                                            if let Some((Line::Obj { id, obj }, _)) = &child.line {
                                                if id.obj_type.as_str() == OBJ_TYPE_FILE {
                                                    let file_obj =
                                                        serde_json::from_value::<FileObject>(
                                                            obj.clone(),
                                                        )
                                                        .map_err(|e| {
                                                            NdnError::DecodeError(e.to_string())
                                                        })?;
                                                    rebuild_builder
                                                        .put_object(file_obj.name.as_str(), id)?;
                                                } else if id.obj_type.as_str() == OBJ_TYPE_DIR {
                                                    let dir_obj =
                                                        serde_json::from_value::<DirObject>(
                                                            obj.clone(),
                                                        )
                                                        .map_err(|e| {
                                                            NdnError::DecodeError(e.to_string())
                                                        })?;
                                                    rebuild_builder
                                                        .put_object(dir_obj.name.as_str(), id)?;
                                                } else {
                                                    // Ignore other object types (chunks or unsupported)
                                                }
                                            } else {
                                                // If any child object line not ready yet, skip strict verification for now.
                                                // (Could defer verification until all children available.)
                                                continue;
                                            }
                                        }

                                        let rebuilt_map = rebuild_builder.build().await?;
                                        let (rebuilt_id, _) = rebuilt_map.calc_obj_id();
                                        if &rebuilt_id != obj_map_id {
                                            return Err(NdnError::InvalidId(format!(
                                                "directory trie map id mismatch after rebuild: {}, expected: {}",
                                                rebuilt_id, obj_map_id
                                            )));
                                        }
                                    }

                                    // For a directory's TrieObjectMap, its save path is the parent directory path (same logic as chunk list for files)
                                    storage
                                        .begin_transfer(
                                            &RestoreItemSelectKey::FsItemId(
                                                item.fs_item_id
                                                    .clone()
                                                    .expect("fs item id should exist"),
                                            ),
                                            parent_path,
                                        )
                                        .await?;
                                }
                            }
                            Line::Index {
                                obj_start_index,
                                obj_ids,
                                obj_header_start_index,
                            } => {
                                unreachable!("Index line not implemented in demo");
                            }
                            Line::ObjHeader {
                                obj_index,
                                id,
                                header,
                            } => {
                                unreachable!("ObjHeader line not implemented in demo");
                            }
                            Line::Header(header) => {
                                unreachable!(
                                    "Header line should not be in CheckHashing status, item id: {:?}",
                                    item.item_id
                                );
                            }
                            Line::RebuildObj {
                                indexes,
                                ranges,
                                ids,
                            } => {
                                unreachable!("RebuildObj line not implemented in demo");
                            }
                        }
                    } else {
                        warn!(
                            "item line should be some, but got None, item id: {:?}",
                            item.item_id
                        );
                    }
                }
                Ok(())
            };

            let rebuild_dir = async |storage: S, ndn_reader: NR, writer: F| -> NdnResult<()> {
                loop {
                    transfer_file(storage.clone(), ndn_reader.clone(), writer.clone()).await?;
                    check_hash(storage.clone()).await?;
                    let root_item = storage.get_root().await?;
                    if let Some(root_item) = root_item {
                        if root_item.status.is_complete() {
                            info!("root item is complete, exit the loop.");
                            break Ok(());
                        } else {
                            info!("root item is not complete, continue to check.");
                        }
                    }
                    // TODO: wakeup when new line read
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            };
            tokio::spawn(async move { rebuild_dir(storage, ndn_reader, writer.clone()).await })
        };

        pipeline_task_handle.await.expect("pipeline task abort")?;
        fs_dir_build_task_handle
            .await
            .expect("fs dir build task abort");

        Ok(())
    }
}

mod backup_app {

    use std::{
        collections::{btree_map::Range, HashMap, VecDeque},
        io::SeekFrom,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use rusqlite::{params, OptionalExtension};
    use tokio::{
        io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
        sync::Mutex,
        task::JoinHandle,
    };

    use crate::{
        packed_obj_pipeline::{
            self,
            demo::{
                dir_backup::{
                    file_system_to_ndn, FileSystemDirReader, FileSystemFileReader,
                    FileSystemFileWriter, FileSystemItem, FileSystemReader, FileSystemWriter,
                    NdnWriter,
                },
                pipeline_sim, EndReason, LineIndex, ObjArrayLine, ObjArraySubObjIndex, ObjMapLine,
                Reader, ReaderEvent, ReaderListener,
            },
        },
        ChunkId, FileObject, NamedDataMgr, NdnError, NdnResult, ObjId, OBJ_TYPE_DIR, OBJ_TYPE_FILE,
    };

    use super::dir_backup::{
        ContainerLineIndex, ContentLine, DirObject, ItemSelectKey, ItemStatus, PathDepth, Storage,
        StorageItem, StorageItemDetail, StorageItemName, StorageItemNameRef,
    };

    fn ser_json<T: serde::Serialize>(v: &T) -> NdnResult<String> {
        serde_json::to_string(v).map_err(|e| NdnError::DecodeError(e.to_string()))
    }

    fn de_json<T: for<'de> serde::Deserialize<'de>>(s: &str) -> NdnResult<T> {
        serde_json::from_str(s).map_err(|e| NdnError::DecodeError(e.to_string()))
    }

    fn content_line_to_json(cl: &ContentLine) -> NdnResult<serde_json::Value> {
        Ok(match cl {
            ContentLine::Chunk(id) => {
                serde_json::json!({"t":"chunk","id": id.to_string()})
            }
            ContentLine::Obj(id, v) => {
                serde_json::json!({"t":"obj","id": id,"v": v})
            }
            ContentLine::Array(a) => {
                serde_json::json!({"t":"arr","v": a})
            }
            ContentLine::Map(m) => {
                serde_json::json!({"t":"map","v": m})
            }
        })
    }

    fn content_line_from_json(v: &serde_json::Value) -> NdnResult<ContentLine> {
        let t = v
            .get("t")
            .and_then(|x| x.as_str())
            .ok_or_else(|| NdnError::DecodeError("missing t".into()))?;
        match t {
            "chunk" => {
                let id_str = v
                    .get("id")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| NdnError::DecodeError("missing id".into()))?;
                let obj_id = ObjId::try_from(id_str)?;
                Ok(ContentLine::Chunk(crate::ChunkId::from_obj_id(&obj_id)))
            }
            "obj" => {
                let id_v = v
                    .get("id")
                    .ok_or_else(|| NdnError::DecodeError("missing id".into()))?;
                let id: ObjId = serde_json::from_value(id_v.clone())
                    .map_err(|e| NdnError::DecodeError(e.to_string()))?;
                let val = v
                    .get("v")
                    .ok_or_else(|| NdnError::DecodeError("missing v".into()))?
                    .clone();
                Ok(ContentLine::Obj(id, val))
            }
            "arr" => {
                let arr_v = v
                    .get("v")
                    .ok_or_else(|| NdnError::DecodeError("missing v".into()))?
                    .clone();
                let line: ObjArrayLine = serde_json::from_value(arr_v)
                    .map_err(|e| NdnError::DecodeError(e.to_string()))?;
                Ok(ContentLine::Array(line))
            }
            "map" => {
                let map_v = v
                    .get("v")
                    .ok_or_else(|| NdnError::DecodeError("missing v".into()))?
                    .clone();
                let line: ObjMapLine = serde_json::from_value(map_v)
                    .map_err(|e| NdnError::DecodeError(e.to_string()))?;
                Ok(ContentLine::Map(line))
            }
            _ => Err(NdnError::DecodeError("unknown content line".into())),
        }
    }

    #[derive(Clone)]
    pub struct SqliteStorage {
        inner: Arc<std::sync::Mutex<rusqlite::Connection>>,
        tx_flag: Arc<std::sync::Mutex<bool>>,
    }

    impl SqliteStorage {
        pub async fn open(path: impl AsRef<Path>) -> NdnResult<Self> {
            let path_buf = path.as_ref().to_path_buf();
            let conn = tokio::task::spawn_blocking(move || {
                let conn = rusqlite::Connection::open(path_buf)
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                conn.execute_batch(
                    r#"
                    PRAGMA journal_mode=WAL;
                    CREATE TABLE IF NOT EXISTS items(
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      parent_id INTEGER,
                      item_type TEXT NOT NULL,
                      name TEXT,
                      chunk_seq INTEGER,
                      data_json TEXT,      -- DirObject/FileObject/Chunk meta
                      status_kind TEXT NOT NULL,
                      status_json TEXT NOT NULL,
                      parent_path TEXT NOT NULL,
                      depth INTEGER NOT NULL,
                      obj_id TEXT,
                      content_header_json TEXT,
                      content_line_json TEXT,
                      line_index INTEGER,
                      container_content_index INTEGER,
                      container_header_index INTEGER
                    );
                    CREATE INDEX IF NOT EXISTS idx_items_parent ON items(parent_id);
                    "#,
                )
                .map_err(|e| NdnError::IoError(e.to_string()))?;
                Ok::<_, NdnError>(conn)
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;

            Ok(Self {
                inner: Arc::new(std::sync::Mutex::new(conn)),
                tx_flag: Arc::new(std::sync::Mutex::new(false)),
            })
        }

        pub async fn get_items_by_line_index(
            &self,
            line_index: &[LineIndex],
        ) -> NdnResult<Vec<StorageItemDetail<i64>>> {
            let conn = self.inner.clone();
            let mut conn_guard = conn.lock().unwrap();
            let mut stmt = conn_guard.prepare(
                "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                        obj_id,content_header_json,content_line_json,line_index,
                        container_content_index,container_header_index
                    FROM items WHERE line_index IN (?1)",
            )
            .map_err(|e| NdnError::IoError(e.to_string()))?;

            let line_index_str: Vec<String> = line_index.iter().map(|x| x.to_string()).collect();
            let rows = stmt
                .query_map(params![line_index_str.join(",")], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<i64>>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                        r.get::<_, i64>(8)?,
                        r.get::<_, Option<String>>(9)?,
                        r.get::<_, Option<String>>(10)?,
                        r.get::<_, Option<String>>(11)?,
                        r.get::<_, Option<i64>>(12)?,
                        r.get::<_, Option<i64>>(13)?,
                        r.get::<_, Option<i64>>(14)?,
                    ))
                })
                .map_err(|e| NdnError::IoError(e.to_string()))?;

            let mut items = Vec::new();
            for row in rows {
                let (
                    id,
                    parent_id,
                    item_type,
                    name_opt,
                    chunk_seq_opt,
                    data_json,
                    status_json,
                    parent_path,
                    depth,
                    _obj_id,
                    _hdr_json,
                    _cl_json,
                    _line_index,
                    _c_content,
                    _c_header,
                ) = row.map_err(|err| NdnError::IoError(err.to_string()))?;
                let status = Self::deserialize_status(status_json.as_str())?;
                let item = match item_type.as_str() {
                    "dir" => {
                        let dir: DirObject = de_json(&data_json)?;
                        StorageItem::Dir(dir)
                    }
                    "file" => {
                        let file: FileObject = de_json(&data_json)?;
                        StorageItem::File(super::dir_backup::FileStorageItem {
                            obj: file,
                            chunk_size: None,
                        })
                    }
                    "chunk" => {
                        let meta_v: serde_json::Value = de_json(&data_json)?;
                        let seq = chunk_seq_opt.unwrap_or(0) as u64;
                        let offset = meta_v
                            .get("offset")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(seq * 0);
                        let chunk_id = meta_v
                            .get("chunk_id")
                            .and_then(|x| x.as_str())
                            .and_then(|s| ObjId::try_from(s).ok())
                            .map(|oid| crate::ChunkId::from_obj_id(&oid));
                        StorageItem::Chunk(super::dir_backup::ChunkItem {
                            seq,
                            offset: std::io::SeekFrom::Start(offset),
                            chunk_id,
                        })
                    }
                    _ => return Err(NdnError::InvalidData("unknown item_type".into())),
                };
                items.push(StorageItemDetail {
                    item_id: id,
                    item,
                    parent_id: parent_id.unwrap_or_default(),
                    status,
                    parent_path: PathBuf::from(parent_path),
                    depth: depth as u64,
                });
            }
            Ok(items)
        }

        fn status_kind(s: &ItemStatus) -> &'static str {
            match s {
                ItemStatus::New => "New",
                ItemStatus::Scanning => "Scanning",
                ItemStatus::Hashing => "Hashing",
                ItemStatus::Transfer(_) => "Transfer",
                ItemStatus::Complete(_) => "Complete",
            }
        }

        fn serialize_status(s: &ItemStatus) -> NdnResult<String> {
            ser_json(s)
        }

        fn deserialize_status(s: &str) -> NdnResult<ItemStatus> {
            de_json(s)
        }

        async fn row_to_detail(&self, rowid: i64) -> NdnResult<StorageItemDetail<i64>> {
            let conn = self.inner.clone();
            tokio::task::spawn_blocking(move || -> NdnResult<_> {
                let mut conn_guard = conn
                    .lock().unwrap();
                let mut stmt = conn_guard.prepare(
                    "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                            obj_id,content_header_json,content_line_json,line_index,
                            container_content_index,container_header_index
                        FROM items WHERE id=?1",
                )
                .map_err(|e| NdnError::IoError(e.to_string()))?;
                let row = stmt
                    .query_row(params![rowid], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Option<i64>>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, Option<String>>(5)?,
                            r.get::<_, String>(6)?,
                            r.get::<_, String>(7)?,
                            r.get::<_, i64>(8)?,
                            r.get::<_, Option<String>>(9)?,
                            r.get::<_, Option<String>>(10)?,
                            r.get::<_, Option<String>>(11)?,
                            r.get::<_, Option<i64>>(12)?,
                            r.get::<_, Option<i64>>(13)?,
                            r.get::<_, Option<i64>>(14)?,
                        ))
                    }).optional()
                    .map_err(|e| NdnError::IoError(e.to_string()))?
                    .ok_or_else(|| NdnError::NotFound("item not found".into()))?;
                let (
                    id,
                    parent_id,
                    item_type,
                    name_opt,
                    chunk_seq_opt,
                    data_json,
                    status_json,
                    parent_path,
                    depth,
                    _obj_id,
                    _hdr_json,
                    _cl_json,
                    _line_index,
                    _c_content,
                    _c_header,
                ) = row;
                let status = Self::deserialize_status(status_json.as_str())?;
                let item = match item_type.as_str() {
                    "dir" => {
                        let dir: DirObject = de_json(&data_json.unwrap_or_default())?;
                        StorageItem::Dir(dir)
                    }
                    "file" => {
                        let file: FileObject = de_json(&data_json.unwrap_or_default())?;
                        StorageItem::File(super::dir_backup::FileStorageItem {
                            obj: file,
                            chunk_size: None,
                        })
                    }
                    "chunk" => {
                        let meta_v: serde_json::Value = de_json(&data_json.unwrap_or_else(|| "{}".into()))?;
                        let seq = chunk_seq_opt.unwrap_or(0) as u64;
                        let offset = meta_v
                            .get("offset")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(seq * 0);
                        let chunk_id = meta_v
                            .get("chunk_id")
                            .and_then(|x| x.as_str())
                            .and_then(|s| ObjId::try_from(s).ok())
                            .map(|oid| crate::ChunkId::from_obj_id(&oid));
                        StorageItem::Chunk(super::dir_backup::ChunkItem {
                            seq,
                            offset: std::io::SeekFrom::Start(offset),
                            chunk_id,
                        })
                    }
                    _ => return Err(NdnError::InvalidData("unknown item_type".into())),
                };
                Ok(StorageItemDetail {
                    item_id: id,
                    item,
                    parent_id: parent_id.unwrap_or_default(),
                    status,
                    parent_path: PathBuf::from(parent_path),
                    depth: depth as u64,
                })
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?
        }
    }

    #[async_trait::async_trait]
    impl Storage for SqliteStorage {
        type ItemId = i64;

        async fn create_new_item(
            &self,
            item: &StorageItem,
            depth: PathDepth,
            parent_path: &Path,
            parent_item_id: Option<Self::ItemId>,
        ) -> NdnResult<StorageItemDetail<Self::ItemId>> {
            let item_clone = item.clone();
            let parent_path = parent_path.to_path_buf();
            let conn = self.inner.clone();
            let status = match item {
                StorageItem::Dir(_) => ItemStatus::New,
                StorageItem::File(_) => ItemStatus::New,
                StorageItem::Chunk(_) => ItemStatus::New,
            };
            let status_kind = Self::status_kind(&status).to_string();
            let status_json = Self::serialize_status(&status)?;
            let (item_type, name_opt, chunk_seq, data_json) = match &item_clone {
                StorageItem::Dir(d) => (
                    "dir",
                    Some(d.name.clone()),
                    None,
                    Some(ser_json(d)?), // store object
                ),
                StorageItem::File(f) => (
                    "file",
                    Some(f.obj.name.clone()),
                    None,
                    Some(ser_json(&f.obj)?),
                ),
                StorageItem::Chunk(c) => {
                    let mut meta = serde_json::Map::new();
                    if let Some(id) = &c.chunk_id {
                        meta.insert(
                            "chunk_id".into(),
                            serde_json::Value::String(id.to_obj_id().to_string()),
                        );
                    }
                    if let std::io::SeekFrom::Start(off) = c.offset {
                        meta.insert("offset".into(), serde_json::Value::Number(off.into()));
                    }
                    (
                        "chunk",
                        None,
                        Some(c.seq as i64),
                        Some(serde_json::Value::Object(meta).to_string()),
                    )
                }
            };
            let parent_path_str = parent_path.display().to_string();
            let id = tokio::task::spawn_blocking(move || -> NdnResult<i64> {
                let conn = conn.lock().unwrap();
                conn.execute(
                    "INSERT INTO items(parent_id,item_type,name,chunk_seq,data_json,status_kind,status_json,parent_path,depth)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    params![
                        parent_item_id,
                        item_type,
                        name_opt,
                        chunk_seq,
                        data_json,
                        status_kind,
                        status_json,
                        parent_path_str,
                        depth as i64
                    ],
                )
                .map_err(|e| NdnError::IoError(e.to_string()))?;
                Ok(conn.last_insert_rowid())
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;

            self.row_to_detail(id).await
        }

        async fn begin_transaction(&self) -> NdnResult<()> {
            let conn = self.inner.clone();
            let flag = self.tx_flag.clone();
            tokio::task::spawn_blocking(move || {
                let mut f = flag.lock().unwrap();
                if !*f {
                    conn.lock()
                        .unwrap()
                        .execute_batch("BEGIN IMMEDIATE")
                        .map_err(|e| NdnError::IoError(e.to_string()))?;
                    *f = true;
                }
                Ok::<_, NdnError>(())
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            Ok(())
        }

        async fn commit_transaction(&self) -> NdnResult<()> {
            let conn = self.inner.clone();
            let flag = self.tx_flag.clone();
            tokio::task::spawn_blocking(move || {
                let mut f = flag.lock().unwrap();
                if *f {
                    conn.lock()
                        .unwrap()
                        .execute_batch("COMMIT")
                        .map_err(|e| NdnError::IoError(e.to_string()))?;
                    *f = false;
                }
                Ok::<_, NdnError>(())
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            Ok(())
        }

        async fn rollback_transaction(&self) -> NdnResult<()> {
            let conn = self.inner.clone();
            let flag = self.tx_flag.clone();
            tokio::task::spawn_blocking(move || {
                let mut f = flag.lock().unwrap();
                if *f {
                    conn.lock()
                        .unwrap()
                        .execute_batch("ROLLBACK")
                        .map_err(|e| NdnError::IoError(e.to_string()))?;
                    *f = false;
                }
                Ok::<_, NdnError>(())
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            Ok(())
        }

        async fn begin_hash(&self, key: &ItemSelectKey<Self::ItemId>) -> NdnResult<()> {
            let key = key.clone();
            let conn = self.inner.clone();
            tokio::task::spawn_blocking(move || {
                let target_status = SqliteStorage::serialize_status(&ItemStatus::Hashing)
                    .expect("serialize status should not fail");
                let (sql, param) = match key {
                    ItemSelectKey::ItemId(id) => (
                        "UPDATE items SET status_kind='Hashing', status_json=?1 WHERE id=?2 AND (status_kind='New' OR status_kind='Scanning')",
                        params![target_status, id.clone()],
                    ),
                    ItemSelectKey::Child(parent_id, child_name) => {
                        match child_name {
                            StorageItemName::Name(name) => (
                                "UPDATE items SET status_kind='Hashing', status_json=?1 WHERE parent_id=?2 AND name=?3 AND (status_kind='New' OR status_kind='Scanning')",
                                params![target_status, parent_id.clone(), name.clone()],
                            ),
                            StorageItemName::ChunkSeq(seq) => (
                                "UPDATE items SET status_kind='Hashing', status_json=?1 WHERE parent_id=?2 AND chunk_seq=?3 AND (status_kind='New' OR status_kind='Scanning')",
                                params![target_status, parent_id.clone(), seq.clone()],
                            ),
                        }
                    }
                };

                let conn = conn.lock().unwrap();
                conn.execute(sql, param)
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                Ok::<_, NdnError>(())
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            Ok(())
        }

        async fn begin_transfer(
            &self,
            key: &ItemSelectKey<Self::ItemId>,
            obj_id: &ObjId,
            content_header: Option<&serde_json::Value>,
        ) -> NdnResult<Option<ContentLine>> {
            /**
             * 1. 先把所有children检索出来，构造ContentLine，dir构造成ContentLine::ObjMap，file构造成ContentLine::ObjArray，chunk直接构造为ContentLine::Chunk，可直接由obj_id判定对象类型
             * 2. 再用构造出的ContentLine构造 ItemStatus::Transfer状态
             * 3. 更新items表的status_kind为Transfer，status_json为ItemStatus::Transfer的序列化结果
             * 4. 返回ContentLine
             */
            let (children, parent_path) = self
                .list_children_order_by_child_seq(key, None, None)
                .await?;
            let content_line = match obj_id.obj_type.as_str() {
                OBJ_TYPE_DIR => {
                    let mut sub_map = HashMap::new();
                    for child in children {
                        sub_map.insert(
                            child.item.name().check_name().to_string(),
                            child.status.check_complete().line_index,
                        );
                    }
                    ContentLine::Map(ObjMapLine::MemoryWithIndex(sub_map))
                }
                OBJ_TYPE_FILE => {
                    let mut sub_array = Vec::new();
                    let mut last_range = None;
                    for child in children {
                        let sub_index = child.status.check_complete().line_index;
                        match last_range.clone() {
                            Some((start, end)) => {
                                if end == sub_index {
                                    last_range = Some((start, end + 1));
                                } else {
                                    if start + 1 == end {
                                        sub_array.push(ObjArraySubObjIndex::Index(start));
                                    } else {
                                        sub_array.push(ObjArraySubObjIndex::Range(start..end));
                                    }
                                    last_range = None;
                                }
                            }
                            None => {
                                last_range = Some((sub_index, sub_index + 1));
                            }
                        }
                    }

                    ContentLine::Array(ObjArrayLine::Lines(sub_array))
                }
                _ if obj_id.is_chunk() => ContentLine::Chunk(ChunkId::from_obj_id(obj_id)),
                _ => return Err(NdnError::InvalidData("unknown object type".into())),
            };

            let content_line_json_str =
                serde_json::to_string(&content_line_to_json(&content_line)?).unwrap();
            let content_header_json_str = content_header.map(|v| serde_json::to_string(v).unwrap());

            let target_status = ItemStatus::Transfer(super::dir_backup::TransferStatusDesc {
                obj_id: obj_id.clone(),
                content_header: content_header.cloned(),
                content_line: Some(content_line.clone()),
            });

            let status_json = SqliteStorage::serialize_status(&target_status)?;
            let key = key.clone();

            let conn = self.inner.clone();
            tokio::task::spawn_blocking(move || {
                let (sql, params) = match key {
                    ItemSelectKey::ItemId(id) => (
                        "UPDATE items SET status_kind='Transfer', status_json=?1, content_header_json=?2, content_line_json=?3 WHERE id=?4 AND status_kind='Hashing'",
                        params![
                            status_json,
                            content_header_json_str.map_or(rusqlite::types::Value::Null, |v| rusqlite::types::Value::Text(v)),
                            content_line_json_str,
                            id.clone()
                        ],
                    ),
                    ItemSelectKey::Child(parent_id, child_name) => {
                        match child_name {
                            StorageItemName::Name(name) => (
                                "UPDATE items SET status_kind='Transfer', status_json=?1, content_header_json=?2, content_line_json=?3 WHERE parent_id=?4 AND name=?5 AND status_kind='Hashing'",
                                params![
                                    status_json,
                                    content_header_json_str.map_or(rusqlite::types::Value::Null, |v| rusqlite::types::Value::Text(v)),
                                    content_line_json_str,
                                    parent_id.clone(),
                                    name.clone()
                                ],
                            ),
                            StorageItemName::ChunkSeq(seq) => (
                                "UPDATE items SET status_kind='Transfer', status_json=?1, content_header_json=?2, content_line_json=?3 WHERE parent_id=?4 AND chunk_seq=?5 AND status_kind='Hashing'",
                                params![
                                    status_json,
                                    content_header_json_str.map_or(rusqlite::types::Value::Null, |v| rusqlite::types::Value::Text(v)),
                                    content_line_json_str,
                                    parent_id.clone(),
                                    seq.clone()
                                ],
                            ),
                        }
                    }
                };

                let conn_lk = conn.lock().unwrap();
                conn_lk
                    .execute(sql, params)
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                Ok::<_, NdnError>(())
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            Ok(Some(content_line))
        }

        async fn complete(
            &self,
            key: &ItemSelectKey<Self::ItemId>,
            line_index: packed_obj_pipeline::demo::LineIndex,
            container_line_index: Option<ContainerLineIndex>,
        ) -> NdnResult<()> {
            /**
             * 1. 状态必须从Transfer变为Complete，先从Transfer状态里读出CompleteStatusDesc需要的参数
             * 2. 更新items表的status_kind为Complete，status_json为CompleteStatusDesc的序列化结果
             */
            let item = self.get_item(key).await?;
            let status = match item.status {
                ItemStatus::Transfer(tsd) => {
                    ItemStatus::Complete(super::dir_backup::CompleteStatusDesc {
                        obj_id: tsd.obj_id,
                        content_header: tsd.content_header,
                        content_line: tsd.content_line,
                        line_index,
                        container_line_index,
                    })
                }
                _ => {
                    return Err(NdnError::InvalidData(
                        "complete called on non-Transfer item".into(),
                    ))
                }
            };
            let status_json = SqliteStorage::serialize_status(&status)?;
            let key = key.clone();
            let conn = self.inner.clone();
            tokio::task::spawn_blocking(move || {
                let (sql, params) = match key {
                    ItemSelectKey::ItemId(id) => (
                        "UPDATE items SET status_kind='Complete', status_json=?1 WHERE id=?2 AND status_kind='Transfer'",
                        params![status_json, id.clone()],
                    ),
                    ItemSelectKey::Child(parent_id, child_name) => {
                        match child_name {
                            StorageItemName::Name(name) => (
                                "UPDATE items SET status_kind='Complete', status_json=?1 WHERE parent_id=?2 AND name=?3 AND status_kind='Transfer'",
                                params![status_json, parent_id.clone(), name.clone()],
                            ),
                            StorageItemName::ChunkSeq(seq) => (
                                "UPDATE items SET status_kind='Complete', status_json=?1 WHERE parent_id=?2 AND chunk_seq=?3 AND status_kind='Transfer'",
                                params![status_json, parent_id.clone(), seq.clone()],
                            ),
                        }
                    }
                };

                let conn_lk = conn.lock().unwrap();
                conn_lk
                    .execute(sql, params)
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                Ok::<_, NdnError>(())
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            Ok(())
        }

        async fn get_root(&self) -> NdnResult<StorageItemDetail<Self::ItemId>> {
            let conn = self.inner.clone();
            let id_opt = tokio::task::spawn_blocking(move || -> NdnResult<Option<i64>> {
                let conn_lk = conn.lock().unwrap();
                let mut stmt = conn_lk
                    .prepare("SELECT id FROM items WHERE parent_id IS NULL ORDER BY id LIMIT 1")
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                let id: Option<i64> = stmt
                    .query_row([], |r| r.get(0))
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                Ok(id)
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            let id = id_opt.ok_or_else(|| NdnError::NotFound("root not found".into()))?;
            self.row_to_detail(id).await
        }

        async fn get_item(
            &self,
            key: &ItemSelectKey<Self::ItemId>,
        ) -> NdnResult<StorageItemDetail<Self::ItemId>> {
            let key = key.clone();
            let conn = self.inner.clone();
            tokio::task::spawn_blocking(move || -> NdnResult<_> {
                let (sql, params) = match key {
                    ItemSelectKey::ItemId(id) => (
                        "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                                obj_id,content_header_json,content_line_json,line_index,
                                container_content_index,container_header_index
                        FROM items WHERE id=?1",
                        params![id.clone()],
                    ),
                    ItemSelectKey::Child(parent_id, child_name) => {
                        match child_name {
                            StorageItemName::Name(name) => (
                                "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                                        obj_id,content_header_json,content_line_json,line_index,
                                        container_content_index,container_header_index
                                FROM items WHERE parent_id=?1 AND name=?2",
                                params![parent_id.clone(), name.clone()],
                            ),
                            StorageItemName::ChunkSeq(seq) => (
                                "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                                        obj_id,content_header_json,content_line_json,line_index,
                                        container_content_index,container_header_index
                                FROM items WHERE parent_id=?1 AND chunk_seq=?2",
                                params![parent_id.clone(), seq.clone()],
                            ),
                        }
                    }
                };

                let conn_lk = conn.lock().unwrap();
                let mut stmt = conn_lk
                    .prepare(sql)
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                let row = stmt
                    .query_row(params, |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Option<i64>>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, Option<String>>(5)?,
                            r.get::<_, String>(6)?,
                            r.get::<_, String>(7)?,
                            r.get::<_, i64>(8)?,
                            r.get::<_, Option<String>>(9)?,
                            r.get::<_, Option<String>>(10)?,
                            r.get::<_, Option<String>>(11)?,
                            r.get::<_, Option<i64>>(12)?,
                            r.get::<_, Option<i64>>(13)?,
                            r.get::<_, Option<i64>>(14)?,
                        ))
                    })
                    .optional()
                    .map_err(|e| NdnError::IoError(e.to_string()))?
                    .ok_or_else(|| NdnError::NotFound("item not found".into()))?;
                let (
                    id,
                    parent_id,
                    item_type,
                    name_opt,
                    chunk_seq_opt,
                    data_json,
                    status_json,
                    parent_path,
                    depth,
                    _obj_id,
                    _hdr_json,
                    _cl_json,
                    _line_index,
                    _c_content,
                    _c_header,
                ) = row;
                let status = SqliteStorage::deserialize_status(&status_json)?;
                let item = match item_type.as_str() {
                    "dir" => {
                        let dir: DirObject = de_json(&data_json.unwrap_or_default())?;
                        StorageItem::Dir(dir)
                    }
                    "file" => {
                        let file: FileObject = de_json(&data_json.unwrap_or_default())?;
                        StorageItem::File(super::dir_backup::FileStorageItem {
                            obj: file,
                            chunk_size: None,
                        })
                    }
                    "chunk" => {
                        let meta_v: serde_json::Value = de_json(&data_json.unwrap_or_else(|| "{}".into()))?;
                        let seq = chunk_seq_opt.unwrap_or(0) as u64;
                        let offset = meta_v
                            .get("offset")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(seq * 0);
                        let chunk_id = meta_v
                            .get("chunk_id")
                            .and_then(|x| x.as_str())
                            .and_then(|s| ObjId::try_from(s).ok())
                            .map(|oid| crate::ChunkId::from_obj_id(&oid));
                        StorageItem::Chunk(super::dir_backup::ChunkItem {
                            seq,
                            offset: std::io::SeekFrom::Start(offset),
                            chunk_id,
                        })
                    }
                    _ => return Err(NdnError::InvalidData("unknown item_type".into())),
                };
                Ok(StorageItemDetail {
                    item_id: id,
                    item,
                    parent_id: parent_id.unwrap_or_default(),
                    status,
                    parent_path: PathBuf::from(parent_path),
                    depth: depth as u64,
                })
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?
        }

        async fn get_uncomplete_dir_or_file_with_max_depth_min_child_seq(
            &self,
        ) -> NdnResult<Option<StorageItemDetail<Self::ItemId>>> {
            let conn = self.inner.clone();
            tokio::task::spawn_blocking(move || -> NdnResult<_> {
                let conn_lk = conn.lock().unwrap();
                let mut stmt = conn_lk.prepare(
                    "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                            obj_id,content_header_json,content_line_json,line_index,
                            container_content_index,container_header_index
                     FROM items WHERE status_kind!='Complete' AND item_type IN ('dir', 'file')
                     ORDER BY depth DESC, chunk_seq ASC, name ASC LIMIT 1",
                ).map_err(|e| NdnError::IoError(e.to_string()))?;
                let row = stmt.query_row([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<i64>>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                        r.get::<_, i64>(8)?,
                        r.get::<_, Option<String>>(9)?,
                        r.get::<_, Option<String>>(10)?,
                        r.get::<_, Option<String>>(11)?,
                        r.get::<_, Option<i64>>(12)?,
                        r.get::<_, Option<i64>>(13)?,
                        r.get::<_, Option<i64>>(14)?,
                    ))
                }).optional()
                .map_err(|e| NdnError::IoError(e.to_string()))?;

                if let Some(row) = row {
                    let (
                        id,
                        parent_id,
                        item_type,
                        name_opt,
                        chunk_seq_opt,
                        data_json,
                        status_json,
                        parent_path,
                        depth,
                        _obj_id,
                        _hdr_json,
                        _cl_json,
                        _line_index,
                        _c_content,
                        _c_header,
                    ) = row;
                    let status = SqliteStorage::deserialize_status(status_json.as_str())?;
                    let item = match item_type.as_str() {
                        "dir" => {
                            let dir: DirObject = de_json(&data_json.unwrap_or_default())?;
                            StorageItem::Dir(dir)
                        }
                        "file" => {
                            let file: FileObject = de_json(&data_json.unwrap_or_default())?;
                            StorageItem::File(super::dir_backup::FileStorageItem {
                                obj: file,
                                chunk_size: None,
                            })
                        }
                        "chunk" => {
                            unreachable!("get_uncomplete_dir_or_file_with_max_depth_min_child_seq should not return chunk item");
                        }
                        _ => return Err(NdnError::InvalidData("unknown item_type".into())),
                    };
                    Ok(Some(StorageItemDetail {
                        item_id: id,
                        item,
                        parent_id: parent_id.unwrap_or_default(),
                        status,
                        parent_path: PathBuf::from(parent_path),
                        depth: depth as u64,
                    }))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))?
        }

        async fn list_children_order_by_child_seq(
            &self,
            parent_key: &ItemSelectKey<Self::ItemId>,
            _offset: Option<u64>,
            _limit: Option<u64>,
        ) -> NdnResult<(Vec<StorageItemDetail<Self::ItemId>>, PathBuf)> {
            let parent_key = parent_key.clone();
            let conn = self.inner.clone();
            let (items, parent_path) = tokio::task::spawn_blocking(move || -> NdnResult<_> {
                let (sql, params) = match parent_key {
                    ItemSelectKey::ItemId(id) => (
                        "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                                obj_id,content_header_json,content_line_json,line_index,
                                container_content_index,container_header_index
                        FROM items WHERE parent_id=?1 ORDER BY chunk_seq ASC, name ASC",
                        params![id.clone()],
                    ),
                    ItemSelectKey::Child(parent_id, child_name) => {
                        match child_name {
                            StorageItemName::Name(name) => (
                                "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                                        obj_id,content_header_json,content_line_json,line_index,
                                        container_content_index,container_header_index
                                FROM items WHERE parent_id=?1 AND name=?2 ORDER BY chunk_seq ASC",
                                params![parent_id.clone(), name.clone()],
                            ),
                            StorageItemName::ChunkSeq(seq) => (
                                "SELECT id,parent_id,item_type,name,chunk_seq,data_json,status_json,parent_path,depth,
                                        obj_id,content_header_json,content_line_json,line_index,
                                        container_content_index,container_header_index
                                FROM items WHERE parent_id=?1 AND chunk_seq=?2 ORDER BY chunk_seq ASC",
                                params![parent_id.clone(), seq.clone()],
                            ),
                        }
                    }
                };
                let conn_lk = conn.lock().unwrap();
                let mut stmt = conn_lk
                    .prepare(sql)
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                let rows = stmt
                    .query_map(params, |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Option<i64>>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, Option<String>>(5)?,
                            r.get::<_, String>(6)?,
                            r.get::<_, String>(7)?,
                            r.get::<_, i64>(8)?,
                            r.get::<_, Option<String>>(9)?,
                            r.get::<_, Option<String>>(10)?,
                            r.get::<_, Option<String>>(11)?,
                            r.get::<_, Option<i64>>(12)?,
                            r.get::<_, Option<i64>>(13)?,
                            r.get::<_, Option<i64>>(14)?,
                        ))
                    })
                    .map_err(|e| NdnError::IoError(e.to_string()))?;
                let mut items = Vec::new();
                let mut parent_path = PathBuf::new();
                for row in rows {
                    let (
                        id,
                        parent_id,
                        item_type,
                        name_opt,
                        chunk_seq_opt,
                        data_json,
                        status_json,
                        parent_path_str,
                        depth,
                        _obj_id,
                        _hdr_json,
                        _cl_json,
                        _line_index,
                        _c_content,
                        _c_header,
                    ) = row.map_err(|err| NdnError::IoError(err.to_string()))?;
                    let status = SqliteStorage::deserialize_status(&status_json)?;
                    let item = match item_type.as_str() {
                        "dir" => {
                            let dir: DirObject = de_json(&data_json.unwrap_or_default())?;
                            StorageItem::Dir(dir)
                        }
                        "file" => {
                            let file: FileObject = de_json(&data_json.unwrap_or_default())?;
                            StorageItem::File(super::dir_backup::FileStorageItem {
                                obj: file,
                                chunk_size: None,
                            })
                        }
                        "chunk" => {
                            let meta_v: serde_json::Value =
                                de_json(&data_json.unwrap_or_else(|| "{}".into()))?;
                            let seq = chunk_seq_opt.unwrap_or(0) as u64;
                            let offset = meta_v
                                .get("offset")
                                .and_then(|x| x.as_u64())
                                .unwrap_or(seq * 0);
                            let chunk_id = meta_v
                                .get("chunk_id")
                                .and_then(|x| x.as_str())
                                .and_then(|s| ObjId::try_from(s).ok())
                                .map(|oid| crate::ChunkId::from_obj_id(&oid));
                            StorageItem::Chunk(super::dir_backup::ChunkItem {
                                seq,
                                offset: std::io::SeekFrom::Start(offset),
                                chunk_id,
                            })
                        }
                        _ => return Err(NdnError::InvalidData("unknown item_type".into())),
                    };
                    if parent_path_str.is_empty() {
                        parent_path = PathBuf::new();
                    } else {
                        parent_path = PathBuf::from(parent_path_str);
                    }
                    items.push(StorageItemDetail {
                        item_id: id,
                        item,
                        parent_id: parent_id.unwrap_or_default(),
                        status,
                        parent_path: parent_path.clone(),
                        depth: depth as u64,
                    });
                }
                Ok((items, parent_path))
            })
            .await
            .map_err(|e| NdnError::IoError(e.to_string()))??;
            Ok((items, parent_path))
        }
    }

    // ===== Local filesystem implementations of FileSystem* traits =====
    fn io_err<E: std::fmt::Display>(e: E) -> NdnError {
        NdnError::IoError(e.to_string())
    }

    #[derive(Clone)]
    pub struct LocalFsReader;

    #[derive(Clone)]
    pub struct LocalDirReader {
        reader: Arc<Mutex<tokio::fs::ReadDir>>,
    }

    #[derive(Clone)]
    pub struct LocalFileReader {
        path: PathBuf,
        file: Arc<Mutex<tokio::fs::File>>,
    }

    #[derive(Clone)]
    pub struct LocalFsWriter;

    #[derive(Clone)]
    pub struct LocalFileWriter {
        path: PathBuf,
        file: Arc<Mutex<tokio::fs::File>>,
    }

    impl LocalFsReader {
        pub fn new() -> Self {
            Self
        }
    }

    impl LocalFsWriter {
        pub fn new() -> Self {
            Self
        }
    }

    #[async_trait::async_trait]
    impl FileSystemDirReader for LocalDirReader {
        async fn next(&self, limit: Option<u64>) -> NdnResult<Vec<FileSystemItem>> {
            let limit = limit.unwrap_or(u64::MAX) as usize;
            if limit == 0 {
                return Ok(Vec::new());
            }
            let mut out = Vec::with_capacity(limit);
            loop {
                let mut reader = self.reader.lock().await;

                match reader.next_entry().await {
                    Ok(Some(entry)) => {
                        let path = entry.path();
                        let meta = tokio::fs::metadata(&path).await.map_err(io_err)?;
                        let name = path
                            .file_name()
                            .and_then(|s| s.to_str())
                            .ok_or_else(|| NdnError::InvalidData("invalid unicode name".into()))?
                            .to_string();

                        if meta.is_dir() {
                            out.push(FileSystemItem::Dir(DirObject {
                                name,
                                content: String::new(),
                                exp: 0,
                                meta: None,
                                owner: None,
                                create_time: None,
                                extra_info: HashMap::new(),
                            }));
                        } else if meta.is_file() {
                            out.push(FileSystemItem::File(FileObject::new(
                                name,
                                meta.len(),
                                String::new(),
                            )));
                        } else {
                            return Err(NdnError::InvalidData("unsupported fs entry type".into()));
                        }
                        if out.len() >= limit {
                            break; // Reached the limit
                        }
                    }
                    Ok(None) => break, // No more entries
                    Err(e) => return Err(io_err(e)),
                }
            }
            Ok(out)
        }
    }

    #[async_trait::async_trait]
    impl FileSystemFileReader for LocalFileReader {
        async fn read_chunk(&self, offset: SeekFrom, limit: Option<u64>) -> NdnResult<Vec<u8>> {
            let file = self.file.clone();
            let mut f = file.lock().await;
            f.seek(offset).await.map_err(io_err)?;
            let mut buf = Vec::new();
            match limit {
                Some(limit) => {
                    buf.resize(limit as usize, 0);
                    let n = f.read(&mut buf).await.map_err(io_err)?;
                    buf.truncate(n);
                }
                None => {
                    f.read_to_end(&mut buf).await.map_err(io_err)?;
                }
            }
            Ok(buf)
        }
    }

    #[async_trait::async_trait]
    impl FileSystemFileWriter for LocalFileWriter {
        async fn length(&self) -> NdnResult<u64> {
            tokio::fs::metadata(self.path.as_path())
                .await
                .map(|meta| meta.len())
                .map_err(|e| NdnError::IoError(e.to_string()))
        }

        async fn write_chunk(&self, chunk_data: &[u8], offset: SeekFrom) -> NdnResult<()> {
            let file = self.file.clone();
            let data = chunk_data.to_vec();
            let mut f = file.lock().await;
            f.seek(offset).await.map_err(io_err)?;
            f.write_all(&data).await.map_err(io_err)
        }

        async fn truncate(&self, offset: SeekFrom) -> NdnResult<()> {
            let mut f = self.file.lock().await;
            let len = match offset {
                SeekFrom::Start(pos) => pos,
                SeekFrom::Current(pos) => {
                    let current_pos = f.seek(SeekFrom::Current(0)).await.map_err(io_err)?;
                    if pos < 0 {
                        return Err(NdnError::InvalidData(
                            "negative offset not allowed in truncate".into(),
                        ));
                    }
                    (current_pos as i64 + pos) as u64
                }
                SeekFrom::End(pos) => {
                    let current_pos = f.seek(SeekFrom::End(0)).await.map_err(io_err)?;
                    if pos > 0 {
                        return Err(NdnError::InvalidData(
                            "positive offset not allowed in truncate".into(),
                        ));
                    }
                    (current_pos as i64 + pos) as u64
                }
            };
            f.set_len(len).await.map_err(io_err)
        }

        async fn read_chunk(&self, offset: SeekFrom, limit: u64) -> NdnResult<Vec<u8>> {
            let mut f = self.file.lock().await;
            f.seek(offset).await.map_err(io_err)?;
            let mut buf = vec![0; limit as usize];
            let n = f.read(&mut buf).await.map_err(io_err)?;
            buf.truncate(n);
            Ok(buf)
        }
    }

    #[async_trait::async_trait]
    impl FileSystemReader<LocalDirReader, LocalFileReader> for LocalFsReader {
        async fn info(&self, path: &Path) -> NdnResult<FileSystemItem> {
            let meta = tokio::fs::metadata(path).await.map_err(io_err)?;
            let name = path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| NdnError::InvalidData("invalid unicode name".into()))?
                .to_string();

            if meta.is_dir() {
                Ok(FileSystemItem::Dir(DirObject {
                    name,
                    content: String::new(),
                    exp: 0,
                    meta: None,
                    owner: None,
                    create_time: None,
                    extra_info: HashMap::new(),
                }))
            } else if meta.is_file() {
                Ok(FileSystemItem::File(FileObject::new(
                    name,
                    meta.len(),
                    String::new(),
                )))
            } else {
                Err(NdnError::InvalidData("unsupported fs entry type".into()))
            }
        }

        async fn open_dir(&self, path: &Path) -> NdnResult<LocalDirReader> {
            let dir_reader = tokio::fs::read_dir(path).await.map_err(io_err)?;

            Ok(LocalDirReader {
                reader: Arc::new(Mutex::new(dir_reader)),
            })
        }

        async fn open_file(&self, path: &Path) -> NdnResult<LocalFileReader> {
            let file = tokio::fs::File::open(path).await.map_err(io_err)?;

            Ok(LocalFileReader {
                path: path.to_path_buf(),
                file: Arc::new(Mutex::new(file)),
            })
        }
    }

    #[async_trait::async_trait]
    impl FileSystemWriter<LocalFileWriter> for LocalFsWriter {
        async fn create_dir_all(&self, dir_path: &Path) -> NdnResult<()> {
            tokio::fs::create_dir_all(dir_path).await.map_err(io_err)
        }

        async fn create_dir(&self, dir: &DirObject, parent_path: &Path) -> NdnResult<()> {
            let dir_path = parent_path.join(&dir.name);
            tokio::fs::create_dir(&dir_path).await.map_err(io_err)?;

            // Optionally, you can write metadata or other info to the directory
            // For now, we just create the directory
            Ok(())
        }

        async fn open_file(&self, file_path: &Path) -> NdnResult<LocalFileWriter> {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(file_path)
                .await
                .map_err(io_err)?;

            Ok(LocalFileWriter {
                path: file_path.to_path_buf(),
                file: Arc::new(Mutex::new(file)),
            })
        }
    }

    // Provide Default implementations if upstream expects Default
    impl Default for LocalFsReader {
        fn default() -> Self {
            Self::new()
        }
    }
    impl Default for LocalFsWriter {
        fn default() -> Self {
            Self::new()
        }
    }

    // 维护一个push_chunk的队列，每1ms写入一个chunk到NamedDataMgr
    struct WriteChunkOperation {
        chunk_id: ChunkId,
        file_path: PathBuf,
        offset: SeekFrom,
        chunk_size: u64,
        chunk_data: Option<Vec<u8>>,
    }

    #[derive(Clone)]
    pub struct MgrNdnWriter {
        ndn_mgr_id: String,
        push_chunk_queue: Arc<Mutex<VecDeque<WriteChunkOperation>>>,
    }

    #[derive(Clone)]
    pub struct MgrNdnReader {
        ndn_mgr_id: String,
    }

    impl MgrNdnWriter {
        pub fn new(ndn_mgr_id: String) -> Self {
            Self {
                ndn_mgr_id,
                push_chunk_queue: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        pub async fn start_writer_loop(&self) {
            let queue = self.push_chunk_queue.clone();
            let ndn_mgr_id = self.ndn_mgr_id.clone();
            tokio::spawn(async move {
                loop {
                    // Wait for 1ms before processing the next chunk
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

                    let mut queue_lock = queue.lock().await;
                    if let Some(op) = queue_lock.pop_front() {
                        if let Err(e) = Self::process_chunk(&ndn_mgr_id, op).await {
                            error!("Failed to process chunk: {}", e);
                        }
                    }
                }
            });
        }

        async fn read_file_range(
            file_path: &Path,
            offset: SeekFrom,
            size: u64,
        ) -> NdnResult<Vec<u8>> {
            let mut file = tokio::fs::File::open(file_path)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;
            file.seek(offset)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;
            let mut buf = vec![0u8; size as usize];
            let n = file
                .read(&mut buf)
                .await
                .map_err(|e| NdnError::IoError(e.to_string()))?;
            buf.truncate(n);
            Ok(buf)
        }

        async fn process_chunk(ndn_mgr_id: &str, op: WriteChunkOperation) -> NdnResult<()> {
            let mut chunk_from_file = Vec::new();
            let data = match op.chunk_data.as_ref() {
                Some(d) => d,
                None => {
                    chunk_from_file =
                        Self::read_file_range(&op.file_path, op.offset, op.chunk_size).await?;
                    &chunk_from_file
                }
            };

            // Try store to NamedDataMgr (best-effort). We ignore "already exists" style errors.
            // These method names are assumed; adjust to real NamedDataMgr API if different.
            let (mut writer, _) = NamedDataMgr::open_chunk_writer(
                Some(ndn_mgr_id),
                &op.chunk_id,
                data.len() as u64,
                0,
            )
            .await?;
            writer.write_all(data.as_slice()).await.map_err(|e| {
                NdnError::IoError(format!("Failed to write chunk {:?}: {}", op.chunk_id, e))
            })?;
            Ok(())
        }
    }

    impl MgrNdnReader {
        pub fn new(ndn_mgr_id: String) -> Self {
            Self { ndn_mgr_id }
        }
    }

    #[async_trait::async_trait]
    impl super::dir_backup::NdnWriter for MgrNdnWriter {
        async fn push_chunk(
            &self,
            chunk_id: &ChunkId,
            file_path: &Path,
            offset: SeekFrom,
            chunk_size: u64,
            chunk_data: Option<Vec<u8>>,
        ) -> NdnResult<()> {
            let op = WriteChunkOperation {
                chunk_id: chunk_id.clone(),
                file_path: file_path.to_path_buf(),
                offset,
                chunk_size,
                chunk_data,
            };
            let mut queue_lock = self.push_chunk_queue.lock().await;
            queue_lock.push_back(op);

            Ok(())
        }

        async fn ignore(&self, obj_id: &ObjId) -> NdnResult<()> {
            // remove chunk from write queue.
            let mut queue_lock = self.push_chunk_queue.lock().await;
            queue_lock.retain(|op| &op.chunk_id.to_obj_id() != obj_id);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl super::dir_backup::NdnReader for MgrNdnReader {
        async fn get_chunk(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>> {
            let (mut reader, _) = NamedDataMgr::open_chunk_reader(
                Some(self.ndn_mgr_id.as_str()),
                chunk_id,
                SeekFrom::Start(0),
                false,
            )
            .await
            .map_err(|e| {
                NdnError::IoError(format!("Failed to open chunk {:?}: {}", chunk_id, e))
            })?;
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.map_err(|e| {
                NdnError::IoError(format!("Failed to read chunk {:?}: {}", chunk_id, e))
            })?;
            Ok(buf)
        }
    }

    /**
     * 使用file_system_to_ndn把本地文件系统中的一个目录备份到ndn服务
     * 1. 启动一个packed_obj_pipeline
     * 2. 启动一个从packed_obj_pipeline读取的任务
     * 3. 启动一个任务使用file_system_to_ndn把本地文件系统中的一个目录备份到ndn服务
     * 4. 等待任务完成
     * 5. 退出进程
     */
    struct DummyReaderListener {
        finished: bool,
    }
    #[async_trait::async_trait]
    impl ReaderListener for DummyReaderListener {
        async fn wait(&mut self) -> NdnResult<ReaderEvent> {
            if self.finished {
                Ok(ReaderEvent::End(EndReason::Complete))
            } else {
                self.finished = true;
                Ok(ReaderEvent::End(EndReason::Complete))
            }
        }
    }

    pub async fn run_backup_demo(scan_path: impl AsRef<Path>) -> NdnResult<()> {
        // 1. 启动一个 packed_obj_pipeline
        let (mut pipeline_writer, mut pipeline_reader, writer_feedback, _unused_listener) =
            pipeline_sim::new_pipeline_sim();

        // 使用自定义的 DummyReaderListener 以避免未实现的 wait()
        let mut pipeline_reader_listener = DummyReaderListener { finished: false };

        // 2. 启动一个从 packed_obj_pipeline 读取的任务 (仅打印输出)
        let reader_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match pipeline_reader.next_line().await {
                    Ok(Some((line, rel_idx))) => {
                        info!("pipeline read line: {:?} @ {:?}", line, rel_idx);
                        // 这里可以处理读取到的行，比如打印或存储

                    }
                    Ok(None) => break,
                    Err(e) => {
                        info!("pipeline reader end: {}", e);
                        break;
                    }
                }
            }
        });

        // 准备本地文件系统读写实现与存储
        let fs_reader = super::backup_app::LocalFsReader::new();
        let storage_path = {
            let mut p = std::env::temp_dir();
            p.push("backup_demo.sqlite");
            p
        };
        let storage = super::backup_app::SqliteStorage::open(&storage_path).await?;

        // NDN 写入器
        let ndn_writer = super::backup_app::MgrNdnWriter::new("demo-ndn-mgr".to_string());
        ndn_writer.start_writer_loop().await;

        // 3. 启动备份任务 (file_system_to_ndn)
        let root_id = file_system_to_ndn(
            Some(scan_path.as_ref()),
            ndn_writer.clone(),
            pipeline_writer,
            fs_reader.clone(),
            storage.clone(),
            1024 * 256, // chunk size
        )
        .await?;

        info!("backup completed, root item id in storage: {:?}", root_id);

        // 4. 通知结束
        loop {
            let event = pipeline_reader_listener.wait().await?;
            match event {
                ReaderEvent::End(reason) => {
                    info!("Pipeline reader ended with reason: {:?}", reason);
                    break;
                }
                ReaderEvent::NeedLine(items) => todo!(),
                ReaderEvent::NeedObject(items) => todo!(),
                ReaderEvent::IgnoreLine(items) => {
                    let items = storage.get_items_by_line_index(items.as_slice()).await?;
                    for item in items {
                        if let Some(obj_id) = item.status.get_obj_id() {
                            info!("Ignoring object: {:?}", obj_id);
                            ndn_writer.ignore(obj_id).await?;
                        }
                    }
                }
                ReaderEvent::IgnoreObject(obj_ids) => {
                    for obj_id in obj_ids {
                        ndn_writer.ignore(&obj_id).await?;
                    }
                }
            }
        }

        // 5. 等待读取任务结束
        let _ = reader_task.await;

        Ok(())
    }
}
