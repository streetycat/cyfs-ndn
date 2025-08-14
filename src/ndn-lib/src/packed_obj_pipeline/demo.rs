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

use crate::{ChunkId, NdnResult, ObjId};

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

pub enum EndReason {
    Abort,
    Complete,
}

#[async_trait::async_trait]
pub trait Reader {
    async fn next_line(&mut self) -> NdnResult<(Line, LineIndexWithRelation)>;
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

    pub struct SimWriter {
        inner: Arc<Mutex<Inner>>,
        notify: Arc<tokio::sync::Notify>,
    }

    pub struct SimWriterFeedback {
        inner: Arc<Mutex<Inner>>,
        notify: Arc<tokio::sync::Notify>,
    }

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
        async fn next_line(&mut self) -> NdnResult<(Line, LineIndexWithRelation)> {
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
                        return Ok(item);
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
    pub fn new_pipeline_sim() -> (
        impl Writer,
        impl Reader,
        impl WriterFeedback,
        impl ReaderListener,
    ) {
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

    #[derive(Debug)]
    pub enum StorageItemName {
        Name(String),
        ChunkSeq(u64), // seq
    }

    impl StorageItemName {
        fn check_name(&self) -> &str {
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
        fn check_name(&self) -> &str {
            match self {
                StorageItemNameRef::Name(name) => *name,
                StorageItemNameRef::ChunkSeq(_) => panic!("expect name"),
            }
        }
    }

    impl StorageItem {
        fn is_dir(&self) -> bool {
            matches!(self, StorageItem::Dir(_))
        }
        fn is_file(&self) -> bool {
            matches!(self, StorageItem::File(_))
        }
        fn is_chunk(&self) -> bool {
            matches!(self, StorageItem::Chunk(_))
        }
        fn check_dir(&self) -> &DirObject {
            match self {
                StorageItem::Dir(dir) => dir,
                _ => panic!("expect dir"),
            }
        }
        fn check_file(&self) -> &FileStorageItem {
            match self {
                StorageItem::File(file) => file,
                _ => panic!("expect file"),
            }
        }
        fn check_chunk(&self) -> &ChunkItem {
            match self {
                StorageItem::Chunk(chunk) => chunk,
                _ => panic!("expect chunk"),
            }
        }
        fn name(&self) -> StorageItemNameRef<'_> {
            match self {
                StorageItem::Dir(dir) => StorageItemNameRef::Name(dir.name.as_str()),
                StorageItem::File(file) => StorageItemNameRef::Name(file.obj.name.as_str()),
                StorageItem::Chunk(chunk_item) => StorageItemNameRef::ChunkSeq(chunk_item.seq),
            }
        }
        fn item_type(&self) -> &str {
            match self {
                StorageItem::Dir(_) => "dir",
                StorageItem::File(_) => "file",
                StorageItem::Chunk(_) => "chunk",
            }
        }
    }

    pub type PathDepth = u64;

    #[derive(Clone, Debug)]
    pub struct ContainerLineIndex {
        pub index: LineIndex,
        pub header_index: Option<LineIndex>,
    }

    #[derive(Clone)]
    pub struct TransferStatusDesc {
        pub obj_id: ObjId,
        pub content_header: Option<Value>,
        pub content_line: Option<ContentLine>, // If it's a ObjArray or ObjMap, return the content line
    }

    #[derive(Clone)]
    pub struct CompleteStatusDesc {
        pub obj_id: ObjId,
        pub content_header: Option<Value>,
        pub content_line: Option<ContentLine>, // If it's a ObjArray or ObjMap, return the content line
        pub line_index: LineIndex,
        pub container_line_index: Option<ContainerLineIndex>,
    }

    #[derive(Clone)]
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
    }

    pub trait StrorageCreator<S: Storage<ItemId = Self::ItemId>>:
        AsyncFn(String) -> NdnResult<S> + Send + Sync + Sized
    {
        type ItemId: Send + Sync + Clone + std::fmt::Debug + Eq + std::hash::Hash + Sized;
    }

    #[derive(Clone)]
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
        pub parent_path: Option<PathBuf>,
        pub depth: PathDepth,
        pub is_last_child: bool,
    }

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
            is_last_child: bool,
        ) -> NdnResult<StorageItemDetail<Self::ItemId>>;
        async fn begin_transaction(&self) -> NdnResult<()>;
        async fn commit_transaction(&self) -> NdnResult<()>;
        // async fn remove_dir(&self, item_id: &Self::ItemId) -> NdnResult<u64>;
        // async fn remove_children(&self, item_id: &Self::ItemId) -> NdnResult<u64>;
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
        // async fn complete_chilren_exclude(
        //     &self,
        //     item_id: &Self::ItemId,
        //     exclude_item_obj_ids: &[ObjId],
        // ) -> NdnResult<Vec<Self::ItemId>>; // complete all children except the exclude items, return the completed item ids
        async fn get_item(
            &self,
            key: &ItemSelectKey<Self::ItemId>,
        ) -> NdnResult<StorageItemDetail<Self::ItemId>>;
        async fn get_uncomplete_dir_or_file_with_max_depth_min_child_seq(
            &self,
        ) -> NdnResult<Option<StorageItemDetail<Self::ItemId>>>; // to continue scan

        // async fn select_file_hashing_or_transfer(
        //     &self,
        // ) -> NdnResult<
        //     Option<(
        //         Self::ItemId,
        //         FileStorageItem,
        //         PathBuf,
        //         ItemStatus,
        //         PathDepth,
        //     )>,
        // >; // to continue transfer file after all child-dir hash and all child-file complete
        // async fn select_dir_hashing_with_all_child_dir_transfer_and_file_complete(
        //     &self,
        // ) -> NdnResult<Option<(Self::ItemId, DirObject, PathBuf, ItemStatus, PathDepth)>>; // to continue transfer dir after all children complete
        // async fn select_dir_transfer(
        //     &self,
        //     depth: Option<PathDepth>,
        //     offset: Option<u64>,
        //     limit: Option<u64>,
        // ) -> NdnResult<Vec<(Self::ItemId, DirObject, PathBuf, ItemStatus, PathDepth)>>; // to transfer dir
        // async fn select_item_transfer(
        //     &self,
        //     offset: Option<u64>,
        //     limit: Option<u64>,
        // ) -> NdnResult<Vec<(Self::ItemId, StorageItem, PathBuf, ItemStatus, PathDepth)>>; // to transfer

        async fn list_children_order_by_child_seq(
            &self,
            parent_key: &ItemSelectKey<Self::ItemId>,
            offset: Option<u64>,
            limit: Option<u64>,
        ) -> NdnResult<(Vec<StorageItemDetail<Self::ItemId>>, PathBuf)>;
        // async fn list_chunks_by_chunk_id(
        //     &self,
        //     chunk_ids: &[ChunkId],
        // ) -> NdnResult<Vec<StorageItemDetail<Self::ItemId>>>;
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
        async fn open_file(&self, file: &FileObject, parent_path: &Path) -> NdnResult<W>;
    }

    #[async_trait::async_trait]
    pub trait FileSystemFileWriter: Send + Sync + Sized {
        async fn length(&self) -> NdnResult<u64>;
        async fn write_chunk(&self, chunk_data: &[u8], offset: SeekFrom) -> NdnResult<()>;
    }

    #[async_trait::async_trait]
    pub trait NdnWriter: Send + Sync + Sized + Clone {
        // async fn push_object(&self, obj_id: &ObjId, obj_str: &str) -> NdnResult<Vec<ObjId>>; // lost child-obj-list
        async fn push_chunk(
            &self,
            chunk_id: &ChunkId,
            file_path: PathBuf,
            offset: SeekFrom,
            chunk_size: u64,
            chunk_data: Option<Vec<u8>>,
        ) -> NdnResult<()>;
        // async fn push_container(&self, container_id: &ObjId) -> NdnResult<Vec<ObjId>>; // lost child-obj-list
        async fn ignore(&self, obj_id: &ObjId) -> NdnResult<()>;
    }

    #[async_trait::async_trait]
    pub trait NdnReader: Send + Sync + Sized {
        async fn get_object(&self, obj_id: &ObjId) -> NdnResult<Value>;
        async fn get_chunk(&self, chunk_id: &ChunkId) -> NdnResult<Vec<u8>>;
        async fn get_container(&self, container_id: &ObjId) -> NdnResult<Value>;
    }

    pub async fn file_system_to_ndn<
        S: Storage + 'static,
        FDR: FileSystemDirReader + 'static,
        FFR: FileSystemFileReader + 'static,
        F: FileSystemReader<FDR, FFR> + 'static,
        NW: NdnWriter + 'static,
        PW: crate::packed_obj_pipeline::demo::Writer + Clone + 'static,
        PRL: crate::packed_obj_pipeline::demo::ReaderListener + 'static,
    >(
        path: Option<&Path>, // None for continue
        ndn_writer: NW,
        pipeline_writer: PW,
        reader: F,
        storage: S,
        chunk_size: u64,
    ) -> NdnResult<S::ItemId> {
        let insert_new_item = async |item: FileSystemItem,
                                     parent_path: &Path,
                                     storage: &S,
                                     depth: u64,
                                     parent_item_id: Option<S::ItemId>,
                                     is_last_child: bool|
               -> NdnResult<StorageItemDetail<S::ItemId>> {
            let detail = match item {
                FileSystemItem::Dir(dir_object) => {
                    let detail = storage
                        .create_new_item(
                            &StorageItem::Dir(dir_object),
                            depth,
                            parent_path,
                            parent_item_id,
                            is_last_child,
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
                            is_last_child,
                        )
                        .await?;
                    let chunk_size = match detail.item {
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
                                i == (file_size + chunk_size - 1) / chunk_size - 1,
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
                true,
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
                        match item_detail.status {
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
                                info!("scan dir: {}, item_id: {:?}", dir.name, item_detail.item_id);
                                let dir_path = item_detail.parent_path.join(dir.name);
                                let dir_reader = reader.open_dir(dir_path.as_path()).await?;

                                storage.begin_transaction().await?;
                                let mut first_child = None;
                                let mut last_child = None;
                                loop {
                                    let items = dir_reader.next(Some(64)).await?;
                                    if let Some(last) = last_child.take() {
                                        insert_new_item(
                                            last,
                                            dir_path.as_path(),
                                            &storage,
                                            item_detail.depth + 1,
                                            Some(item_detail.item_id.clone()),
                                            items.is_empty(),
                                        )
                                        .await?;
                                    }
                                    let is_finish = items.len() < 64;
                                    last_child = items.pop();
                                    for item in items {
                                        let child = insert_new_item(
                                            item,
                                            dir_path.as_path(),
                                            &storage,
                                            item_detail.depth + 1,
                                            Some(item_detail.item_id.clone()),
                                            false,
                                        )
                                        .await?;

                                        first_child.get_or_insert(child);
                                    }
                                    if is_finish {
                                        if let Some(last) = last_child.take() {
                                            let child = insert_new_item(
                                                last,
                                                dir_path.as_path(),
                                                &storage,
                                                item_detail.depth + 1,
                                                Some(item_detail.item_id.clone()),
                                                items.is_empty(),
                                            )
                                            .await?;
                                            first_child.get_or_insert(child);
                                        }
                                        break;
                                    }
                                }
                                storage
                                    .begin_hash(&ItemSelectKey::ItemId(item_detail.item_id))
                                    .await?;
                                storage.commit_transaction().await?;

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
                                            file_item.obj.name.check_name(),
                                            item_detail.item_id
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
                                                                file_path,
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
                                                                file_path,
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

                                        file_item.obj.content = file_chunk_list_id.to_string();
                                        let (file_obj_id, file_obj_str) =
                                            file_item.obj.gen_obj_id();

                                        let chunk_list_header =
                                            serde_json::to_value(file_chunk_list.body()).unwrap();
                                        let chunk_list_diff_line = storage
                                            .begin_transfer(
                                                &ItemSelectKey::ItemId(item_detail.item_id),
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

                                        let (content_index, header_index) = pipeline_writer
                                            .write_object_map(
                                                dir_map_id,
                                                content_header,
                                                content_line.check_map(),
                                                None,
                                                Some(item_detail.depth as u32),
                                            )
                                            .await?;
                                        let mut dir_obj = dir_item.clone();
                                        dir_obj.content = dir_map_id.to_string();

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

                                        let (content_index, header_index) = pipeline_writer
                                            .write_object_array(
                                                file_chunk_list_id,
                                                content_header,
                                                content_line.check_array(),
                                                None,
                                                Some(item_detail.depth as u32),
                                            )
                                            .await?;
                                        let mut file_obj = file_item.obj.clone();
                                        file_obj.content = file_chunk_list_id.to_string();

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
                                        &ItemSelectKey::ItemId(item_detail.item_id),
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

        // TODO: should wait for the root item to be created.
        let root_item = storage.get_root().await?;
        assert_eq!(root_item.depth, 0, "root item depth should be 0.");
        if let Some(path) = path {
            match root_item.item.name() {
                StorageItemNameRef::Name(name) => assert_eq!(
                    root_item
                        .parent_path
                        .unwrap_or(PathBuf::from("/"))
                        .join(name)
                        .as_path(),
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

    pub async fn ndn_to_file_system<
        S: Storage + 'static,
        FFW: FileSystemFileWriter + 'static,
        F: FileSystemWriter<FFW> + 'static,
        R: NdnReader + 'static,
    >(
        dir_path_root_obj_id: Option<(&Path, &ObjId)>, // None for continue
        writer: F,
        reader: R,
        storage: S,
    ) -> NdnResult<S::ItemId> {
        let task_handle = {
            let dir_path_root_obj_id = dir_path_root_obj_id
                .as_ref()
                .map(|(path, obj_id)| (path.to_path_buf(), (*obj_id).clone()));
            let storage = storage.clone();

            let proc = async move || -> NdnResult<()> {
                info!("start ndn to file system task...");

                let create_new_item = async |parent_path: &Path,
                                             obj_id: &ObjId,
                                             parent_item_id: Option<S::ItemId>,
                                             depth: u64|
                       -> NdnResult<()> {
                    let obj_value = reader.get_object(obj_id).await?;
                    if obj_id.obj_type.as_str() == OBJ_TYPE_DIR {
                        let dir_obj: DirObject =
                            serde_json::from_value(obj_value).map_err(|err| {
                                let msg = format!(
                                    "Failed to parse dir object from obj-id: {}, error: {}",
                                    obj_id, err
                                );
                                error!("{}", msg);
                                crate::NdnError::DecodeError(msg)
                            })?;
                        info!("create dir: {}, obj id: {}", dir_obj.name, obj_id);
                        let (item_id, _, _) = storage
                            .create_new_item(
                                &StorageItem::Dir(dir_obj),
                                depth,
                                parent_path,
                                parent_item_id,
                            )
                            .await?;
                        storage.begin_transfer(&item_id, obj_id).await?;
                    } else if obj_id.obj_type.as_str() == OBJ_TYPE_FILE {
                        let file_obj: FileObject =
                            serde_json::from_value(obj_value).map_err(|err| {
                                let msg = format!(
                                    "Failed to parse file object from obj-id: {}, error: {:?}",
                                    obj_id, err
                                );
                                error!("{}", msg);
                                crate::NdnError::DecodeError(msg)
                            })?;

                        info!("create file: {}, obj id: {}", file_obj.name, obj_id);

                        let (item_id, _, _) = storage
                            .create_new_item(
                                &StorageItem::File(FileStorageItem {
                                    obj: file_obj,
                                    chunk_size: None,
                                }),
                                depth,
                                parent_path,
                                parent_item_id,
                            )
                            .await?;
                        storage.begin_transfer(&item_id, obj_id).await?;
                    } else {
                        unreachable!(
                            "expect dir or file object, got: {}, obj id: {}",
                            obj_id.obj_type, obj_id
                        );
                    }

                    Ok(())
                };

                let transfer_item = async |item_id: &S::ItemId,
                                           item: &StorageItem,
                                           parent_path: &Path,
                                           depth: u64|
                       -> NdnResult<()> {
                    info!(
                        "transfer item: {:?}, parent path: {}",
                        item_id,
                        parent_path.display()
                    );
                    match item {
                        StorageItem::Dir(dir_obj) => {
                            info!("transfer dir: {:?}", dir_obj.name);
                            let dir_obj_map_id = ObjId::try_from(dir_obj.content.as_str())?;
                            let obj_map_json = reader.get_container(&dir_obj_map_id).await?;
                            let dir_obj_map = TrieObjectMap::open(obj_map_json).await?;

                            writer.create_dir(dir_obj, parent_path).await?;

                            let child_obj_ids = dir_obj_map
                                .iter()?
                                .map(|(_, child_obj_id)| child_obj_id)
                                .collect::<Vec<_>>();
                            for child_obj_id in child_obj_ids {
                                create_new_item(
                                    &parent_path.join(dir_obj.name.as_str()).as_path(),
                                    &child_obj_id,
                                    Some(item_id.clone()),
                                    depth + 1,
                                )
                                .await?;
                            }
                            storage.complete(item_id).await?;
                        }
                        StorageItem::File(file_storage_item) => {
                            info!("transfer file: {:?}", file_storage_item.obj.name);
                            let file_chunk_list_id =
                                ObjId::try_from(file_storage_item.obj.content.as_str())?;
                            let chunk_list = reader.get_container(&file_chunk_list_id).await?;
                            let chunk_list =
                                ChunkListBuilder::open(chunk_list).await?.build().await?;

                            let file_writer = writer
                                .open_file(&file_storage_item.obj, parent_path)
                                .await?;
                            let file_length = file_writer.length().await?;
                            let (chunk_index, mut pos) = if file_length > 0 {
                                let (chunk_index, chunk_pos) = chunk_list
                                    .get_chunk_index_by_offset(SeekFrom::Start(file_length - 1))?;
                                let chunk_id = chunk_list
                                    .get_chunk(chunk_index as usize)?
                                    .expect("chunk id should exist");
                                match chunk_pos.cmp(
                                    &chunk_id.get_length().expect("chunk id should have length"),
                                ) {
                                    std::cmp::Ordering::Less => {
                                        info!(
                                            "chunk pos: {}, file pos: {}, chunk index: {}",
                                            chunk_pos, file_length, chunk_index
                                        );
                                        (chunk_index, file_length - (chunk_pos + 1))
                                    }
                                    std::cmp::Ordering::Equal => {
                                        info!(
                                            "chunk pos: {}, file pos: {}, chunk index: {}",
                                            chunk_pos, file_length, chunk_index
                                        );
                                        (chunk_index + 1, file_length)
                                    }
                                    std::cmp::Ordering::Greater => {
                                        unreachable!(
                                            "chunk pos: {}, file pos: {}, chunk index: {}",
                                            chunk_pos, file_length, chunk_index
                                        );
                                    }
                                }
                            } else {
                                (0, 0)
                            };

                            for chunk_index in chunk_index..chunk_list.len() {
                                let chunk_index = chunk_index as usize;
                                let chunk_id = chunk_list
                                    .get_chunk(chunk_index)?
                                    .expect("chunk id should exist");
                                let chunk_data = reader.get_chunk(&chunk_id).await?;
                                assert_eq!(
                                    chunk_data.len() as u64,
                                    chunk_id.get_length().expect("chunk id should have length"),
                                    "chunk data length should match the chunk id length."
                                );

                                let hasher = ChunkHasher::new(None).expect("hash failed.");
                                let hash = hasher.calc_from_bytes(chunk_data.as_slice());
                                let calc_chunk_id = ChunkId::from_mix_hash_result_by_hash_method(
                                    chunk_data.len() as u64,
                                    &hash,
                                    HashMethod::Sha256,
                                )?;
                                if calc_chunk_id != chunk_id {
                                    error!(
                                        "chunk id mismatch, expected: {:?}, got: {:?}",
                                        calc_chunk_id, chunk_id
                                    );
                                    return Err(NdnError::InvalidData(
                                        "chunk id mismatch".to_string(),
                                    ));
                                }

                                file_writer
                                    .write_chunk(chunk_data.as_slice(), SeekFrom::Start(pos))
                                    .await?;
                                pos += chunk_data.len() as u64;
                            }

                            storage.complete(item_id).await?;
                        }
                        StorageItem::Chunk(chunk_item) => {
                            unreachable!(
                                "should not have chunk item in dir transfer, got: {:?}",
                                chunk_item.chunk_id
                            );
                        }
                    }
                    Ok(())
                };

                if let Some((path, root_obj_id)) = dir_path_root_obj_id {
                    info!(
                        "scan path: {}, root obj id: {}",
                        path.display(),
                        root_obj_id
                    );
                    writer.create_dir_all(path.as_path()).await?;
                    create_new_item(path.as_path(), &root_obj_id, None, 0).await?;
                }

                loop {
                    let items = storage.select_item_transfer(Some(0), Some(64)).await?;
                    if items.is_empty() {
                        info!("no more items to transfer.");
                        break;
                    }

                    for (item_id, item, parent_path, item_status, depth) in items {
                        info!("transfer item: {:?}, status: {:?}", item_id, item_status);
                        assert!(item_status.is_transfer());
                        assert_eq!(depth, 0, "item depth should be 0 for root item transfer.");

                        transfer_item(&item_id, &item, parent_path.as_path(), depth).await?;
                    }
                }

                Ok(())
            };

            tokio::spawn(async move { proc().await })
        };

        task_handle.await.expect("task abort")?;

        // TODO: should wait for the root item to be created.
        let (root_item_id, item, parent_path, _, depth) = storage.get_root().await?;
        assert_eq!(depth, 0, "root item depth should be 0.");
        if let Some((dir_path, _)) = dir_path_root_obj_id {
            assert_eq!(
                dir_path,
                parent_path.as_path(),
                "root item parent path should match the scan path."
            );
        }

        Ok(root_item_id)
    }
}
