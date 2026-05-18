use super::objtype::ObjType;

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("Missing LOGG")]
    Logg,

    #[error("Missing LOBJ")]
    Lobj,

    #[error("Io ({0:?})")]
    Io(#[from] std::io::Error),

    #[error("EOF")]
    Eof,

    #[error("Unexpected object type {0:?}")]
    UnexpectedObjType(ObjType),

    #[error("Uncompressed size mismatch")]
    UncompressedSizeMismatch,

    #[error("Zlib error")]
    ZlibError,
}

pub type ParseResult<T> = Result<T, ParseError>;
