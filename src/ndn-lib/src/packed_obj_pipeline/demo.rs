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

use crate::{NdnResult, ObjId};

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
    async fn write_object_array(
        &mut self,
        obj_array_id: ObjId,
        header: Value,
        content: ObjArrayLine,
        ref_line_index: Option<LineIndex>,
    ) -> crate::NdnResult<LineIndexWithRelation>;
    async fn write_object_map(
        &mut self,
        obj_map_id: ObjId,
        header: Value,
        content: ObjMapLine,
        ref_line_index: Option<LineIndex>,
    ) -> crate::NdnResult<LineIndexWithRelation>;
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

        async fn write_object_array(
            &mut self,
            obj_array_id: ObjId,
            header: Value,
            content: ObjArrayLine,
            ref_line_index: Option<LineIndex>,
        ) -> crate::NdnResult<LineIndexWithRelation> {
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
                return Ok(LineIndexWithRelation::Ref(idx));
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

            Ok(LineIndexWithRelation::Real(content_index))
        }

        async fn write_object_map(
            &mut self,
            obj_map_id: ObjId,
            header: Value,
            content: ObjMapLine,
            ref_line_index: Option<LineIndex>,
        ) -> crate::NdnResult<LineIndexWithRelation> {
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
                return Ok(LineIndexWithRelation::Ref(idx));
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

            Ok(LineIndexWithRelation::Real(content_index))
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
