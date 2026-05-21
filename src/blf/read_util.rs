use super::error::ParseResult;
use std::io::Read;

pub fn read_u8<R: Read>(r: &mut R) -> ParseResult<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

pub fn read_u16_be<R: Read>(r: &mut R) -> ParseResult<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(((b[0] as u16) << 8) | b[1] as u16)
}

pub fn read_u32_be<R: Read>(r: &mut R) -> ParseResult<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(((b[0] as u32) << 24) | ((b[1] as u32) << 16) | ((b[2] as u32) << 8) | b[3] as u32)
}
