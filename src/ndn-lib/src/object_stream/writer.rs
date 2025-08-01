use crate::types::LineIndex;

#[async_trait::async_trait]
pub trait Writer {
    // Writes a line to the object stream.
    // `line` is the line to write, `line_index` is the index of the line,
    // Returns a Result indicating success or failure.
    async fn write_line(
        &mut self,
        line: crate::types::Line,
        line_index: LineIndex,
    ) -> crate::NdnResult<()>;
}
