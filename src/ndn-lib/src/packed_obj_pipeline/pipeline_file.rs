/// ObjectPipeline 文件结构说明：
///
/// 一个 ObjectPipeline 文件由最多四个段组成，每个段由若干行组成，每行格式为：
/// 行号（`LineIndex`） : 行内容（JSON 字符串） : 行长度。
///
/// 各段说明：
///
/// 1. **PipelineHeader 段（可选，第一行）**
///    - 仅一行，作为整个 ObjectPipeline 的头部。
///    - 记录其他段（索引、摘要、对象）的起始位置等元信息。
///
/// 2. **ObjectIndexes 段（可选）**
///    - 包含多行，每行用于索引对象的存储偏移。
///    - 每行描述某个对象在文件中的偏移位置，便于快速定位和检索。
///
/// 3. **ObjectHeader 段（可选）**
///    - 包含多行，每行为一个对象的摘要信息。
///    - 用于存储对象的元数据（如对象 ID、类型等），便于管理和查找。
///
/// 4. **Objects 段（必选）**
///    - 包含多行，每行存储一个对象的完整信息。
///    - 这是 ObjectPipeline 文件的核心内容，所有对象数据均存储于此段。
///
/// 说明：
/// - 除 Objects 段外，其他三个段均为可选，具体是否存在由实际需求决定。
/// - 每一段的每一行都遵循统一格式：行号（`LineIndex`）与内容（JSON 字符串）。
/// - 通过 PipelineHeader 可定位其他段的起始位置，实现高效的分段读取与管理。
/// - 文件中每个段的`LineIndex`和`offset`各自独立编号和记录，在读取时应该依据`PipelineHeader`的信息重新映射成整个`ObjectPipeline`中的`LineIndex`和`offset`。
use super::*;
use buckyos_kit::is_default;
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSection {
    #[serde(skip_serializing_if = "is_default")]
    pub offset: u64, // offset in the file
    #[serde(skip_serializing_if = "is_default")]
    pub first_index: LineIndex, // first line index in this section
    #[serde(skip_serializing_if = "is_default")]
    pub line_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHeader {
    index_section: Option<FileSection>,
    header_section: Option<FileSection>,
    object_section: Option<FileSection>,
}

pub struct FileObjPipelineReader {
    reader: BufReader<File>,
    index_map: HashMap<LineIndex, u64>, // line_index -> offset
    header: Option<FileHeader>,
}

pub struct FileObjPipelineWriter {
    writer: BufWriter<File>,
    next_index: LineIndex,
    index_map: HashMap<LineIndex, u64>, // line_index -> offset
    header: Option<FileHeader>,
}

impl FileObjPipelineReader {
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut index_map = HashMap::new();
        let mut header = None;

        // Scan file for header and build index map
        let mut offset = 0u64;
        let mut buf = String::new();
        loop {
            buf.clear();
            let bytes_read = reader.read_line(&mut buf)?;
            if bytes_read == 0 {
                break;
            }
            let line: (LineIndex, Line) = serde_json::from_str(&buf)?;
        }

        Ok(Self {
            reader,
            index_map,
            header,
        })
    }
}

#[async_trait::async_trait]
impl Reader for FileObjPipelineReader {
    async fn next_line(&mut self) -> NdnResult<Option<(Line, LineIndexWithRelation)>> {
        let mut buf = String::new();
        let bytes_read = self.reader.read_line(&mut buf)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (index, line): (LineIndex, Line) = serde_json::from_str(&buf)?;
        Ok(Some((line, LineIndexWithRelation::Real(index))))
    }

    async fn line_at(
        &mut self,
        index: LineIndex,
        _offset: Option<u64>,
        _waiter: Option<Receiver<NdnResult<Option<Line>>>>,
    ) -> NdnResult<()> {
        if let Some(&offset) = self.index_map.get(&index) {
            self.reader.seek(SeekFrom::Start(offset))?;
            Ok(())
        } else {
            Err("LineIndex not found".into())
        }
    }

    async fn object_by_id(
        &mut self,
        id: &ObjId,
        _for_index: LineIndex,
        _offset: Option<u64>,
        _waiter: Option<Receiver<NdnResult<Option<Line>>>>,
    ) -> NdnResult<()> {
        // Linear scan for demo
        for (&idx, &offset) in &self.index_map {
            self.reader.seek(SeekFrom::Start(offset))?;
            let mut buf = String::new();
            self.reader.read_line(&mut buf)?;
            let (_index, line): (LineIndex, Line) = serde_json::from_str(&buf)?;
            match &line {
                Line::Obj { id: obj_id, .. } if obj_id == id => return Ok(()),
                _ => continue,
            }
        }
        Err("ObjId not found".into())
    }
}

impl FileObjPipelineWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            next_index: 0,
            index_map: HashMap::new(),
            header: None,
        })
    }
}

#[async_trait::async_trait]
impl Writer for FileObjPipelineWriter {
    async fn write_line(&mut self, line: Line, line_index: LineIndexWithRelation) -> NdnResult<()> {
        let index = match line_index {
            LineIndexWithRelation::Real(idx) => idx,
            LineIndexWithRelation::Ref(idx) => idx,
        };
        let offset = self.writer.stream_position()?;
        let line_str = serde_json::to_string(&(index, &line))?;
        self.writer.write_all(line_str.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.index_map.insert(index, offset);
        self.next_index = index + 1;
        Ok(())
    }
}
