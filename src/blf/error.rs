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

    #[error("Invalid data")]
    InvalidData,

    #[error("CSV line {line}: {message}")]
    Csv { line: usize, message: String },

    #[error("Invalid MF4 magic (expected 'MDF     ')")]
    InvalidMf4Magic,

    #[error("Invalid MF4 block: expected {expected}, got {got:?}")]
    InvalidMf4Block {
        expected: &'static str,
        got: [u8; 4],
    },

    #[error("Unsupported MF4 feature: {0}")]
    UnsupportedMf4Feature(String),
}

pub type ParseResult<T> = Result<T, ParseError>;
