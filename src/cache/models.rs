use kaspa_rpc_core::{RpcBlock, RpcBlockVerboseData, RpcHash, RpcHeader, RpcTransaction};

pub struct CacheBlock {
    header: RpcHeader,
    verbose_data: Option<RpcBlockVerboseData>
}

impl From<RpcBlock> for CacheBlock {
    fn from(value: RpcBlock) -> Self {
        Self {
            header: value.header, 
            verbose_data: value.verbose_data
        }
    }
}