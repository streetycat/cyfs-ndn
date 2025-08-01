use tokio::sync::mpsc::Receiver;

use crate::{
    types::{LineIndex, LineIndexWithRelation},
    NdnResult,
};

#[async_trait::async_trait]
pub trait Reader {
    async fn next_line(&mut self)
        -> NdnResult<Option<(crate::types::Line, LineIndexWithRelation)>>;
    async fn line_at(
        &mut self,
        index: LineIndex,
        waiter: Option<Receiver<NdnResult<Option<crate::types::Line>>>>,
    ) -> NdnResult<()>;
    async fn object_by_id(
        &mut self,
        id: &crate::ObjId,
        for_index: LineIndex,
        waiter: Option<Receiver<NdnResult<Option<crate::types::Line>>>>,
    ) -> NdnResult<()>;
}
