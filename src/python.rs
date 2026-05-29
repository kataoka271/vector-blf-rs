use crate::blf::{self, ContainerHeader};
use crate::mf4;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use std::fs::File;
use std::io::BufReader;

// ── message classes ──────────────────────────────────────────────────────────

#[pyclass(get_all)]
pub struct Can {
    pub channel: u16,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: u8,
    pub rtr: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

#[pymethods]
impl Can {
    fn __repr__(&self) -> String {
        format!(
            "Can(channel={}, id=0x{:X}, dlc={}, data={})",
            self.channel,
            self.id,
            self.dlc,
            hex(&self.data)
        )
    }
}

#[pyclass(get_all)]
pub struct CanFd {
    pub channel: u16,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: u8,
    pub rtr: bool,
    pub fdf: bool,
    pub brs: bool,
    pub esi: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

#[pymethods]
impl CanFd {
    fn __repr__(&self) -> String {
        format!(
            "CanFd(channel={}, id=0x{:X}, dlc={}, data={})",
            self.channel,
            self.id,
            self.dlc,
            hex(&self.data)
        )
    }
}

#[pyclass(get_all)]
pub struct CanFd64 {
    pub channel: u8,
    pub id: u32,
    pub is_ext_id: bool,
    pub dir: u8,
    pub rtr: bool,
    pub fdf: bool,
    pub brs: bool,
    pub esi: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

#[pymethods]
impl CanFd64 {
    fn __repr__(&self) -> String {
        format!(
            "CanFd64(channel={}, id=0x{:X}, dlc={}, data={})",
            self.channel,
            self.id,
            self.dlc,
            hex(&self.data)
        )
    }
}

#[pyclass(get_all)]
pub struct Ethernet {
    pub channel: u16,
    pub dir: u8,
    pub src_addr: Vec<u8>,
    pub dst_addr: Vec<u8>,
    pub ether_type: u16,
    pub data: Vec<u8>,
}

#[pymethods]
impl Ethernet {
    fn __repr__(&self) -> String {
        format!(
            "Ethernet(channel={}, ether_type=0x{:04X}, len={})",
            self.channel,
            self.ether_type,
            self.data.len()
        )
    }
}

#[pyclass(get_all)]
pub struct EthernetEx {
    pub channel: u16,
    pub dir: u8,
    pub src_addr: Vec<u8>,
    pub dst_addr: Vec<u8>,
    pub ether_type: u16,
    pub data: Vec<u8>,
}

#[pymethods]
impl EthernetEx {
    fn __repr__(&self) -> String {
        format!(
            "EthernetEx(channel={}, ether_type=0x{:04X}, len={})",
            self.channel,
            self.ether_type,
            self.data.len()
        )
    }
}

#[pyclass(get_all)]
pub struct Mf4Signal {
    pub group: String,
    pub name: String,
    pub value: f64,
    pub unit: String,
}

#[pymethods]
impl Mf4Signal {
    fn __repr__(&self) -> String {
        format!(
            "Mf4Signal(group={:?}, name={:?}, value={}, unit={:?})",
            self.group, self.name, self.value, self.unit
        )
    }
}

// ── BaseObject ───────────────────────────────────────────────────────────────

/// A single log entry: timestamp + one message variant.
/// `message` is one of: Can, CanFd, CanFd64, Ethernet, EthernetEx, or None
/// for object types that are not yet decoded.
#[pyclass(get_all)]
pub struct BaseObject {
    /// Timestamp in nanoseconds since file epoch.
    pub timestamp_ns: u64,
    pub message: PyObject,
}

#[pymethods]
impl BaseObject {
    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "BaseObject(timestamp_ns={}, message={})",
            self.timestamp_ns,
            self.message
                .bind(py)
                .repr()
                .map(|s| s.to_string())
                .unwrap_or_default()
        )
    }
}

// ── message-type filter ──────────────────────────────────────────────────────

const F_CAN: u8 = 1 << 0;
const F_CANFD: u8 = 1 << 1;
const F_CANFD64: u8 = 1 << 2;
const F_ETHERNET: u8 = 1 << 3;
const F_ETHERNETEX: u8 = 1 << 4;
const F_OTHER: u8 = 1 << 5;
const F_MF4SIGNAL: u8 = 1 << 6;

fn message_bit(msg: &blf::Message) -> u8 {
    match msg {
        blf::Message::Can(_) => F_CAN,
        blf::Message::CanFd(_) => F_CANFD,
        blf::Message::CanFd64(_) => F_CANFD64,
        blf::Message::Ethernet(_) => F_ETHERNET,
        blf::Message::EthernetEx(_) => F_ETHERNETEX,
        blf::Message::Mf4Signal(_) => F_MF4SIGNAL,
        blf::Message::Other(_, _) => F_OTHER,
    }
}

fn parse_filter(types: Option<Vec<String>>) -> PyResult<u8> {
    let Some(list) = types else { return Ok(0) };
    let mut mask = 0u8;
    for s in &list {
        mask |= match s.to_lowercase().as_str() {
            "can"         => F_CAN,
            "canfd"       => F_CANFD,
            "canfd64"     => F_CANFD64,
            "ethernet"    => F_ETHERNET,
            "ethernetex"  => F_ETHERNETEX,
            "mf4signal"   => F_MF4SIGNAL,
            "other"       => F_OTHER,
            other => return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown message type {other:?}; valid: Can, CanFd, CanFd64, Ethernet, EthernetEx, Mf4Signal, Other"
            ))),
        };
    }
    Ok(mask)
}

// ── Reader ───────────────────────────────────────────────────────────────────

enum InnerReader {
    Blf(blf::Reader<BufReader<File>>),
    Mf4(mf4::Reader<BufReader<File>>),
}

impl InnerReader {
    fn next_item(&mut self) -> Option<Result<blf::BaseObject, blf::ParseError>> {
        match self {
            InnerReader::Blf(r) => r.next(),
            InnerReader::Mf4(r) => r.next(),
        }
    }
}

/// Iterator over BaseObjects in a BLF or MF4 file.
///
/// Parameters
/// ----------
/// path : str
///     Path to the BLF or MF4 file (.blf, .mf4, .mdf).
/// types : list[str] | None
///     Optional allowlist of message types to yield. Filtering happens in
///     Rust before any Python object is allocated. Valid values (case-
///     insensitive): ``"Can"``, ``"CanFd"``, ``"CanFd64"``, ``"Ethernet"``,
///     ``"EthernetEx"``, ``"Mf4Signal"``, ``"Other"``. If omitted, all types
///     are yielded.
///
/// Example::
///
///     import vector_blf
///     for obj in vector_blf.Reader("file.blf", types=["Can", "CanFd"]):
///         print(obj.timestamp_ns, obj.message.id)
///
///     for obj in vector_blf.Reader("file.mf4", types=["Mf4Signal"]):
///         print(obj.message.name, obj.message.value)
#[pyclass]
pub struct Reader {
    inner: InnerReader,
    filter: u8,
}

#[pymethods]
impl Reader {
    #[new]
    #[pyo3(signature = (path, types=None))]
    fn new(path: &str, types: Option<Vec<String>>) -> PyResult<Self> {
        let filter = parse_filter(types)?;
        let ext = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        let f =
            File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let inner = if ext == "mf4" || ext == "mdf" {
            let r = mf4::Reader::new(BufReader::new(f))
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            InnerReader::Mf4(r)
        } else {
            let r = blf::Reader::new(BufReader::new(f))
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            InnerReader::Blf(r)
        };
        Ok(Self { inner, filter })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<BaseObject> {
        loop {
            match slf.inner.next_item() {
                None => return Err(PyStopIteration::new_err(())),
                Some(Err(e)) => return Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
                Some(Ok(obj)) => {
                    if slf.filter == 0 || slf.filter & message_bit(&obj.message) != 0 {
                        return Ok(convert_base_object(py, obj));
                    }
                }
            }
        }
    }
}

// ── signal databases ─────────────────────────────────────────────────────────

/// CAN signal database loaded from a CSV file.
///
/// CSV format (header required)::
///
///     message_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
///     0x100,EngineSpeed,0,16,Intel,false,0.25,0.0
///
/// ``message_id`` accepts hex (``0x…``) or decimal.
/// ``byte_order`` is ``Intel`` or ``Motorola`` (case-insensitive).
/// ``is_signed`` accepts ``true``/``false`` or ``1``/``0``.
#[pyclass]
pub struct CanSignalDb {
    inner: blf::CanSignalDb,
}

#[pymethods]
impl CanSignalDb {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let f =
            File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let db = blf::CanSignalDb::from_csv(f)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: db })
    }

    /// Decode all matching signals for ``message_id`` from ``data``.
    ///
    /// Returns a list of ``(signal_name, signal_value)`` tuples.
    /// Signals whose bit range extends outside ``data`` are silently skipped.
    fn decode(&self, message_id: u32, data: &[u8]) -> Vec<(String, f64)> {
        self.inner
            .extract(message_id, data)
            .into_iter()
            .map(|(name, val)| (name.to_string(), val))
            .collect()
    }

    /// Returns ``True`` if ``message_id`` is configured as a CAN-FD container frame
    /// (i.e. the signal CSV has at least one row with a ``pdu_id`` for this CAN ID).
    fn is_container(&self, message_id: u32) -> bool {
        self.inner.is_container(message_id)
    }

    /// Demultiplex a CAN-FD container frame and decode all signals from the contained I-PDUs.
    ///
    /// ``data`` is the raw CAN frame payload. ``long_header`` selects between
    /// the 4-byte-overhead short header (default) and the 8-byte-overhead long header.
    /// Returns a list of ``(signal_name, value)`` tuples for all matched I-PDUs.
    #[pyo3(signature = (message_id, data, long_header = false))]
    fn decode_container(
        &self,
        message_id: u32,
        data: &[u8],
        long_header: bool,
    ) -> Vec<(String, f64)> {
        let header = if long_header {
            ContainerHeader::Long
        } else {
            ContainerHeader::Short
        };
        self.inner
            .extract_container(message_id, data, header)
            .into_iter()
            .map(|(name, val)| (name.to_string(), val))
            .collect()
    }
}

/// SOME/IP signal database loaded from a CSV file.
///
/// CSV format (header required)::
///
///     service_id,method_id,signal_name,start_bit,bit_length,byte_order,is_signed,scale,offset
///     0x0064,0x0001,Temperature,0,16,Intel,false,0.01,0.0
///
/// ``service_id`` and ``method_id`` accept hex (``0x…``) or decimal.
/// Signals are decoded from the SOME/IP application payload (bytes after the 16-byte header).
#[pyclass]
pub struct SomeIpSignalDb {
    inner: blf::SomeIpSignalDb,
}

#[pymethods]
impl SomeIpSignalDb {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let f =
            File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let db = blf::SomeIpSignalDb::from_csv(f)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: db })
    }

    /// Decode all matching signals for ``(service_id, method_id)`` from ``payload``.
    ///
    /// Returns a list of ``(signal_name, signal_value)`` tuples.
    /// Signals whose bit range extends outside ``payload`` are silently skipped.
    fn decode(&self, service_id: u16, method_id: u16, payload: &[u8]) -> Vec<(String, f64)> {
        self.inner
            .extract(service_id, method_id, payload)
            .into_iter()
            .map(|(name, val)| (name.to_string(), val))
            .collect()
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn hex(data: &[u8]) -> String {
    data.iter()
        .map(|b| format!("{:02X}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

fn ts_ns(ts: blf::Timestamp) -> u64 {
    match ts {
        blf::Timestamp::Nanosecond(ns) => ns,
        blf::Timestamp::Microsecond(us) => us * 1_000,
    }
}

fn convert_base_object(py: Python<'_>, obj: blf::BaseObject) -> BaseObject {
    let ts = ts_ns(obj.timestamp);
    let msg: PyObject = match obj.message {
        blf::Message::Can(m) => Py::new(
            py,
            Can {
                channel: m.channel,
                id: m.id,
                is_ext_id: m.is_ext_id,
                dir: m.dir.to_u8(),
                rtr: m.rtr,
                dlc: m.dlc,
                data: m.data,
            },
        )
        .unwrap()
        .into_any(),
        blf::Message::CanFd(m) => Py::new(
            py,
            CanFd {
                channel: m.channel,
                id: m.id,
                is_ext_id: m.is_ext_id,
                dir: m.dir.to_u8(),
                rtr: m.rtr,
                fdf: m.fdf,
                brs: m.brs,
                esi: m.esi,
                dlc: m.dlc,
                data: m.data,
            },
        )
        .unwrap()
        .into_any(),
        blf::Message::CanFd64(m) => Py::new(
            py,
            CanFd64 {
                channel: m.channel,
                id: m.id,
                is_ext_id: m.is_ext_id,
                dir: m.dir.to_u8(),
                rtr: m.rtr,
                fdf: m.fdf,
                brs: m.brs,
                esi: m.esi,
                dlc: m.dlc,
                data: m.data,
            },
        )
        .unwrap()
        .into_any(),
        blf::Message::Ethernet(m) => Py::new(
            py,
            Ethernet {
                channel: m.channel,
                dir: m.dir.to_u8(),
                src_addr: m.src_addr.to_vec(),
                dst_addr: m.dst_addr.to_vec(),
                ether_type: m.ether_type,
                data: m.data,
            },
        )
        .unwrap()
        .into_any(),
        blf::Message::EthernetEx(m) => Py::new(
            py,
            EthernetEx {
                channel: m.channel,
                dir: m.dir.to_u8(),
                src_addr: m.src_addr.to_vec(),
                dst_addr: m.dst_addr.to_vec(),
                ether_type: m.ether_type,
                data: m.data,
            },
        )
        .unwrap()
        .into_any(),
        blf::Message::Mf4Signal(m) => Py::new(
            py,
            Mf4Signal {
                group: m.group.clone(),
                name: m.name.clone(),
                value: m.value,
                unit: m.unit.clone(),
            },
        )
        .unwrap()
        .into_any(),
        blf::Message::Other(_, _) => py.None(),
    };
    BaseObject {
        timestamp_ns: ts,
        message: msg,
    }
}

// ── module ───────────────────────────────────────────────────────────────────

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<BaseObject>()?;
    m.add_class::<Can>()?;
    m.add_class::<CanFd>()?;
    m.add_class::<CanFd64>()?;
    m.add_class::<Ethernet>()?;
    m.add_class::<EthernetEx>()?;
    m.add_class::<Mf4Signal>()?;
    m.add_class::<CanSignalDb>()?;
    m.add_class::<SomeIpSignalDb>()?;
    Ok(())
}
