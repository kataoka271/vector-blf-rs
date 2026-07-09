// PyO3's macro-generated error conversions trigger this lint as a false positive.
#![allow(clippy::useless_conversion)]

use crate::blf::diag::doip::DOIP_PORT;
use crate::blf::{self, ContainerHeader};
use crate::mf4;
use pyo3::exceptions::{PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::fs::File;
use std::io::{BufReader, Cursor};

// (uds_type, service_id, service_name, nrc, nrc_name, data)
type UdsTuple = (String, i32, String, Option<i32>, Option<String>, Vec<u8>);

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
            other => return Err(PyValueError::new_err(format!(
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

    fn start_time_ns(&self) -> u64 {
        match self {
            InnerReader::Blf(r) => ts_ns(r.header.start_timestamp),
            InnerReader::Mf4(r) => r.start_time_ns,
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
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            InnerReader::Mf4(r)
        } else {
            let r = blf::Reader::new(BufReader::new(f))
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            InnerReader::Blf(r)
        };
        Ok(Self { inner, filter })
    }

    /// Return the recording start time as nanoseconds since the Unix epoch.
    ///
    /// For BLF files this reads the ``start_timestamp`` field of the file header.
    /// For MF4/MDF files this reads the HD block ``start_time_ns`` field.
    /// Returns 0 when the file header contains no valid timestamp.
    fn start_time_ns(&self) -> u64 {
        self.inner.start_time_ns()
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<BaseObject> {
        loop {
            match slf.inner.next_item() {
                None => return Err(PyStopIteration::new_err(())),
                Some(Err(e)) => return Err(PyValueError::new_err(e.to_string())),
                Some(Ok(obj)) => {
                    if slf.filter == 0 || slf.filter & message_bit(&obj.message) != 0 {
                        return Ok(convert_base_object(py, obj));
                    }
                }
            }
        }
    }

    /// Read up to ``n`` records and return a column-oriented dict ready for
    /// ``pd.DataFrame(batch)``.  Returns ``None`` at EOF.
    ///
    /// Columns match the bronze output schema:
    /// ``timestamp_ns``, ``message_type``, ``channel``, ``can_id``,
    /// ``is_ext_id``, ``dir``, ``rtr``, ``dlc``, ``data``, ``fdf``,
    /// ``brs``, ``esi``, ``src_addr``, ``dst_addr``, ``ether_type``,
    /// ``mf4_group``, ``mf4_name``, ``mf4_value``, ``mf4_unit``.
    #[pyo3(signature = (n = 50_000))]
    fn read_batch<'py>(
        &mut self,
        py: Python<'py>,
        n: usize,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        // Column accumulators — all same length after the loop.
        let mut timestamp_ns: Vec<u64> = Vec::with_capacity(n);
        let mut message_type: Vec<&'static str> = Vec::with_capacity(n);
        let mut channel: Vec<PyObject> = Vec::with_capacity(n);
        let mut can_id: Vec<PyObject> = Vec::with_capacity(n);
        let mut is_ext_id: Vec<PyObject> = Vec::with_capacity(n);
        let mut dir: Vec<PyObject> = Vec::with_capacity(n);
        let mut rtr: Vec<PyObject> = Vec::with_capacity(n);
        let mut dlc: Vec<PyObject> = Vec::with_capacity(n);
        let mut data: Vec<PyObject> = Vec::with_capacity(n);
        let mut fdf: Vec<PyObject> = Vec::with_capacity(n);
        let mut brs: Vec<PyObject> = Vec::with_capacity(n);
        let mut esi: Vec<PyObject> = Vec::with_capacity(n);
        let mut src_addr: Vec<PyObject> = Vec::with_capacity(n);
        let mut dst_addr: Vec<PyObject> = Vec::with_capacity(n);
        let mut ether_type: Vec<PyObject> = Vec::with_capacity(n);
        let mut vlan_tpid: Vec<PyObject> = Vec::with_capacity(n);
        let mut vlan_cos: Vec<PyObject> = Vec::with_capacity(n);
        let mut vlan_id: Vec<PyObject> = Vec::with_capacity(n);
        let mut mf4_group: Vec<PyObject> = Vec::with_capacity(n);
        let mut mf4_name: Vec<PyObject> = Vec::with_capacity(n);
        let mut mf4_value: Vec<PyObject> = Vec::with_capacity(n);
        let mut mf4_unit: Vec<PyObject> = Vec::with_capacity(n);

        let none = py.None();

        macro_rules! push_none {
            ($($col:ident),+) => { $( $col.push(none.clone_ref(py)); )+ };
        }

        let mut count = 0usize;
        loop {
            if count >= n {
                break;
            }
            match self.inner.next_item() {
                None => break,
                Some(Err(e)) => return Err(PyValueError::new_err(e.to_string())),
                Some(Ok(obj)) => {
                    if self.filter != 0 && self.filter & message_bit(&obj.message) == 0 {
                        continue;
                    }
                    let ts = ts_ns(obj.timestamp);
                    timestamp_ns.push(ts);

                    match obj.message {
                        blf::Message::Can(m) => {
                            message_type.push("CAN");
                            channel.push((m.channel as i64).into_py(py));
                            can_id.push((m.id as i64).into_py(py));
                            is_ext_id.push(m.is_ext_id.into_py(py));
                            dir.push((m.dir.to_u8() as i64).into_py(py));
                            rtr.push(m.rtr.into_py(py));
                            dlc.push((m.dlc as i64).into_py(py));
                            data.push(PyBytes::new_bound(py, &m.data).into_any().unbind());
                            push_none!(
                                fdf, brs, esi, src_addr, dst_addr, ether_type, vlan_tpid, vlan_cos,
                                vlan_id, mf4_group, mf4_name, mf4_value, mf4_unit
                            );
                        }
                        blf::Message::CanFd(m) => {
                            message_type.push("CAN_FD");
                            channel.push((m.channel as i64).into_py(py));
                            can_id.push((m.id as i64).into_py(py));
                            is_ext_id.push(m.is_ext_id.into_py(py));
                            dir.push((m.dir.to_u8() as i64).into_py(py));
                            rtr.push(m.rtr.into_py(py));
                            dlc.push((m.dlc as i64).into_py(py));
                            data.push(PyBytes::new_bound(py, &m.data).into_any().unbind());
                            fdf.push(m.fdf.into_py(py));
                            brs.push(m.brs.into_py(py));
                            esi.push(m.esi.into_py(py));
                            push_none!(
                                src_addr, dst_addr, ether_type, vlan_tpid, vlan_cos, vlan_id,
                                mf4_group, mf4_name, mf4_value, mf4_unit
                            );
                        }
                        blf::Message::CanFd64(m) => {
                            message_type.push("CAN_FD64");
                            channel.push((m.channel as i64).into_py(py));
                            can_id.push((m.id as i64).into_py(py));
                            is_ext_id.push(m.is_ext_id.into_py(py));
                            dir.push((m.dir.to_u8() as i64).into_py(py));
                            rtr.push(m.rtr.into_py(py));
                            dlc.push((m.dlc as i64).into_py(py));
                            data.push(PyBytes::new_bound(py, &m.data).into_any().unbind());
                            fdf.push(m.fdf.into_py(py));
                            brs.push(m.brs.into_py(py));
                            esi.push(m.esi.into_py(py));
                            push_none!(
                                src_addr, dst_addr, ether_type, vlan_tpid, vlan_cos, vlan_id,
                                mf4_group, mf4_name, mf4_value, mf4_unit
                            );
                        }
                        blf::Message::Ethernet(m) => {
                            message_type.push("ETH");
                            channel.push((m.channel as i64).into_py(py));
                            push_none!(can_id, is_ext_id, rtr, dlc, fdf, brs, esi);
                            dir.push((m.dir.to_u8() as i64).into_py(py));
                            data.push(PyBytes::new_bound(py, &m.data).into_any().unbind());
                            src_addr.push(PyBytes::new_bound(py, &m.src_addr).into_any().unbind());
                            dst_addr.push(PyBytes::new_bound(py, &m.dst_addr).into_any().unbind());
                            ether_type.push((m.ether_type as i64).into_py(py));
                            match &m.vlan {
                                Some(v) => {
                                    vlan_tpid.push((v.tpid as i64).into_py(py));
                                    vlan_cos.push((v.pri as i64).into_py(py));
                                    vlan_id.push((v.vid as i64).into_py(py));
                                }
                                None => {
                                    push_none!(vlan_tpid, vlan_cos, vlan_id);
                                }
                            }
                            push_none!(mf4_group, mf4_name, mf4_value, mf4_unit);
                        }
                        blf::Message::EthernetEx(m) => {
                            message_type.push("ETH_EX");
                            channel.push((m.channel as i64).into_py(py));
                            push_none!(can_id, is_ext_id, rtr, dlc, fdf, brs, esi);
                            dir.push((m.dir.to_u8() as i64).into_py(py));
                            data.push(PyBytes::new_bound(py, &m.data).into_any().unbind());
                            src_addr.push(PyBytes::new_bound(py, &m.src_addr).into_any().unbind());
                            dst_addr.push(PyBytes::new_bound(py, &m.dst_addr).into_any().unbind());
                            ether_type.push((m.ether_type as i64).into_py(py));
                            match &m.vlan {
                                Some(v) => {
                                    vlan_tpid.push((v.tpid as i64).into_py(py));
                                    vlan_cos.push((v.pri as i64).into_py(py));
                                    vlan_id.push((v.vid as i64).into_py(py));
                                }
                                None => {
                                    push_none!(vlan_tpid, vlan_cos, vlan_id);
                                }
                            }
                            push_none!(mf4_group, mf4_name, mf4_value, mf4_unit);
                        }
                        blf::Message::Mf4Signal(m) => {
                            message_type.push("MF4_SIGNAL");
                            push_none!(
                                channel, can_id, is_ext_id, dir, rtr, dlc, data, fdf, brs, esi,
                                src_addr, dst_addr, ether_type, vlan_tpid, vlan_cos, vlan_id
                            );
                            mf4_group.push(m.group.into_py(py));
                            mf4_name.push(m.name.into_py(py));
                            mf4_value.push(m.value.into_py(py));
                            mf4_unit.push(m.unit.into_py(py));
                        }
                        blf::Message::Other(_, _) => {
                            message_type.push("OTHER");
                            push_none!(
                                channel, can_id, is_ext_id, dir, rtr, dlc, data, fdf, brs, esi,
                                src_addr, dst_addr, ether_type, vlan_tpid, vlan_cos, vlan_id,
                                mf4_group, mf4_name, mf4_value, mf4_unit
                            );
                        }
                    }
                    count += 1;
                }
            }
        }

        if count == 0 {
            return Ok(None);
        }

        let d = PyDict::new_bound(py);
        d.set_item("timestamp_ns", PyList::new_bound(py, &timestamp_ns))?;
        d.set_item("message_type", PyList::new_bound(py, &message_type))?;
        d.set_item("channel", PyList::new_bound(py, &channel))?;
        d.set_item("can_id", PyList::new_bound(py, &can_id))?;
        d.set_item("is_ext_id", PyList::new_bound(py, &is_ext_id))?;
        d.set_item("dir", PyList::new_bound(py, &dir))?;
        d.set_item("rtr", PyList::new_bound(py, &rtr))?;
        d.set_item("dlc", PyList::new_bound(py, &dlc))?;
        d.set_item("data", PyList::new_bound(py, &data))?;
        d.set_item("fdf", PyList::new_bound(py, &fdf))?;
        d.set_item("brs", PyList::new_bound(py, &brs))?;
        d.set_item("esi", PyList::new_bound(py, &esi))?;
        d.set_item("src_addr", PyList::new_bound(py, &src_addr))?;
        d.set_item("dst_addr", PyList::new_bound(py, &dst_addr))?;
        d.set_item("ether_type", PyList::new_bound(py, &ether_type))?;
        d.set_item("vlan_tpid", PyList::new_bound(py, &vlan_tpid))?;
        d.set_item("vlan_cos", PyList::new_bound(py, &vlan_cos))?;
        d.set_item("vlan_id", PyList::new_bound(py, &vlan_id))?;
        d.set_item("mf4_group", PyList::new_bound(py, &mf4_group))?;
        d.set_item("mf4_name", PyList::new_bound(py, &mf4_name))?;
        d.set_item("mf4_value", PyList::new_bound(py, &mf4_value))?;
        d.set_item("mf4_unit", PyList::new_bound(py, &mf4_unit))?;
        Ok(Some(d))
    }
}

// ── signal databases ─────────────────────────────────────────────────────────

/// CAN signal database loaded from a CSV file.
///
/// CSV format (header required)::
///
///     message_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
///     0x100,EngineSpeed,0,0,16,Intel,false,0.25,0.0
///
/// ``message_id`` accepts hex (``0x…``) or decimal.
/// ``byte_order`` is ``Intel`` or ``Motorola`` (case-insensitive).
/// ``is_signed`` accepts ``true``/``false`` or ``1``/``0``.
#[pyclass]
pub struct CanSignalDb {
    inner: blf::CanSignalDb,
    enum_map: Option<blf::EnumValueMap>,
}

#[pymethods]
impl CanSignalDb {
    #[new]
    #[pyo3(signature = (path, enum_path=None))]
    fn new(path: &str, enum_path: Option<&str>) -> PyResult<Self> {
        let f =
            File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let db = blf::CanSignalDb::from_csv(f).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let enum_map = enum_path
            .map(|p| {
                let f = File::open(p)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
                blf::enum_value_map_from_csv(f).map_err(|e| PyValueError::new_err(e.to_string()))
            })
            .transpose()?;
        Ok(Self {
            inner: db,
            enum_map,
        })
    }

    /// Decode all matching signals for ``message_id`` from ``data``.
    ///
    /// Returns a list of ``(signal_name, signal_value, category)`` tuples.
    /// ``category`` is a string when a value-to-category mapping exists, otherwise ``None``.
    /// Signals whose bit range extends outside ``data`` are silently skipped.
    fn decode(&self, message_id: u32, data: &[u8]) -> Vec<(String, f64, Option<String>)> {
        self.inner
            .extract(message_id, data, self.enum_map.as_ref())
            .into_iter()
            .map(|(name, val, cat)| (name.to_string(), val, cat))
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
    /// Returns a list of ``(signal_name, value, category)`` tuples for all matched I-PDUs.
    #[pyo3(signature = (message_id, data, long_header = false))]
    fn decode_container(
        &self,
        message_id: u32,
        data: &[u8],
        long_header: bool,
    ) -> Vec<(String, f64, Option<String>)> {
        let header = if long_header {
            ContainerHeader::Long
        } else {
            ContainerHeader::Short
        };
        self.inner
            .extract_container(message_id, data, header, self.enum_map.as_ref())
            .into_iter()
            .map(|(name, val, cat)| (name.to_string(), val, cat))
            .collect()
    }

    /// Demultiplex a CAN-FD container frame into raw ``(pdu_id, pdu_payload)`` pairs
    /// without decoding signals.
    ///
    /// Returns an empty list if ``message_id`` is not a known container frame.
    /// ``long_header`` selects the 8-byte-overhead long header (default: 4-byte short).
    #[pyo3(signature = (message_id, data, long_header = false))]
    fn extract_container_pdus(
        &self,
        message_id: u32,
        data: &[u8],
        long_header: bool,
    ) -> Vec<(u32, Vec<u8>)> {
        let header = if long_header {
            ContainerHeader::Long
        } else {
            ContainerHeader::Short
        };
        self.inner.demux_pdus(message_id, data, header)
    }

    /// Decode signals for a single I-PDU previously extracted from a container frame.
    ///
    /// ``can_id`` is the parent container CAN ID; ``pdu_id`` identifies the I-PDU.
    /// Returns ``(signal_name, signal_value, category)`` tuples.
    fn decode_pdu(
        &self,
        can_id: u32,
        pdu_id: u32,
        data: &[u8],
    ) -> Vec<(String, f64, Option<String>)> {
        self.inner
            .extract_pdu(can_id, pdu_id, data, self.enum_map.as_ref())
            .into_iter()
            .map(|(name, val, cat)| (name.to_string(), val, cat))
            .collect()
    }
}

/// SOME/IP signal database loaded from a CSV file.
///
/// CSV format (header required)::
///
///     service_id,method_id,signal_name,start_byte,start_bit,bit_length,byte_order,is_signed,scale,offset
///     0x0064,0x0001,Temperature,0,0,16,Intel,false,0.01,0.0
///
/// ``service_id`` and ``method_id`` accept hex (``0x…``) or decimal.
/// Signals are decoded from the SOME/IP application payload (bytes after the 16-byte header).
#[pyclass]
pub struct SomeIpSignalDb {
    inner: blf::SomeIpSignalDb,
    enum_map: Option<blf::EnumValueMap>,
}

#[pymethods]
impl SomeIpSignalDb {
    #[new]
    #[pyo3(signature = (path, enum_path=None))]
    fn new(path: &str, enum_path: Option<&str>) -> PyResult<Self> {
        let f =
            File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let db =
            blf::SomeIpSignalDb::from_csv(f).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let enum_map = enum_path
            .map(|p| {
                let f = File::open(p)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
                blf::enum_value_map_from_csv(f).map_err(|e| PyValueError::new_err(e.to_string()))
            })
            .transpose()?;
        Ok(Self {
            inner: db,
            enum_map,
        })
    }

    /// Decode all matching signals for ``(service_id, method_id)`` from ``payload``.
    ///
    /// Returns a list of ``(signal_name, signal_value, category)`` tuples.
    /// ``category`` is a string when a value-to-category mapping exists, otherwise ``None``.
    /// Signals whose bit range extends outside ``payload`` are silently skipped.
    fn decode(
        &self,
        service_id: u16,
        method_id: u16,
        payload: &[u8],
    ) -> Vec<(String, f64, Option<String>)> {
        self.inner
            .extract(service_id, method_id, payload, self.enum_map.as_ref())
            .into_iter()
            .map(|(name, val, cat)| (name.to_string(), val, cat))
            .collect()
    }
}

// ── IsoTpReassembler ─────────────────────────────────────────────────────────

/// Stateful ISO-TP (ISO 15765-2) reassembler for a single sender/receiver conversation.
///
/// Create one instance per ``(channel, can_id)`` pair and call ``push()`` for
/// every CAN frame in timestamp order.  Handles standard and extended
/// (CAN-FD) Single-Frame and First-Frame formats automatically.
///
/// Example::
///
///     r = vector_blf.IsoTpReassembler()
///     result = r.push(frame_data)  # returns tuple or None
#[pyclass]
pub struct IsoTpReassembler {
    inner: blf::Reassembler,
}

#[pymethods]
impl IsoTpReassembler {
    #[new]
    fn new() -> Self {
        Self {
            inner: blf::Reassembler::new(),
        }
    }

    /// Feed one CAN frame payload.
    ///
    /// Returns ``(uds_type, service_id, service_name, nrc, nrc_name, data)``
    /// when a complete UDS PDU is assembled, or ``None`` if more frames are needed.
    ///
    /// ``uds_type`` is one of ``"Request"``, ``"PositiveResponse"``, or
    /// ``"NegativeResponse"``.  ``nrc`` and ``nrc_name`` are ``None`` unless
    /// the PDU is a ``NegativeResponse``.
    fn push(&mut self, data: &[u8]) -> Option<UdsTuple> {
        let frame = blf::IsoTpFrame::parse(data).ok()?;
        let uds = self.inner.push(&frame).ok()??;
        Some(uds_to_tuple(uds))
    }
}

// ── module-level protocol parse functions ────────────────────────────────────

/// Parse SOME/IP messages from an Ethernet frame payload.
///
/// Supports IPv4 and IPv6 outer headers, UDP transport, and AUTOSAR Container
/// PDU Transport (multiple back-to-back SOME/IP PDUs per UDP datagram).
/// SOME/IP-SD (service_id=0xFFFF) frames are silently skipped.
///
/// Returns a list of dicts.  Keys match the ``_SOMEIP_ROW_SCHEMA`` used in the
/// DLT pipeline: ``src_ip``, ``dst_ip``, ``udp_src_port``, ``udp_dst_port``,
/// ``someip_service_id``, ``someip_method_id``, ``someip_length``,
/// ``someip_client_id``, ``someip_session_id``, ``someip_protocol_version``,
/// ``someip_interface_version``, ``someip_msg_type``, ``someip_return_code``,
/// ``payload``.
#[pyfunction]
fn parse_someip_udp<'py>(
    py: Python<'py>,
    ether_type: u16,
    eth_payload: &[u8],
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let ip = match blf::Ip::parse(ether_type, eth_payload) {
        Ok(ip) => ip,
        Err(_) => return Ok(vec![]),
    };
    let (src_ip, dst_ip, transport_data) = match &ip {
        blf::Ip::V4(v4) => (
            format!(
                "{}.{}.{}.{}",
                v4.src_addr[0], v4.src_addr[1], v4.src_addr[2], v4.src_addr[3]
            ),
            format!(
                "{}.{}.{}.{}",
                v4.dst_addr[0], v4.dst_addr[1], v4.dst_addr[2], v4.dst_addr[3]
            ),
            v4.parse_transport(),
        ),
        blf::Ip::V6(v6) => (
            ipv6_str(&v6.src_addr),
            ipv6_str(&v6.dst_addr),
            v6.parse_transport(),
        ),
    };
    let udp = match transport_data {
        Ok(blf::eth::transport::Transport::Udp(u)) => u,
        _ => return Ok(vec![]),
    };

    let mut results = Vec::new();
    let mut pos = 0usize;
    while pos < udp.data.len() {
        let slice = &udp.data[pos..];
        let msg = match blf::SomeIp::parse(Cursor::new(slice)) {
            Ok(m) => m,
            Err(_) => break,
        };
        // skip SOME/IP-SD and frames with invalid protocol version
        if !msg.is_sd() && msg.protocol_version == 0x01 {
            let valid_msg_types: &[u8] = &[0x00, 0x01, 0x02, 0x80, 0x81];
            if valid_msg_types.contains(&msg.message_type.to_u8()) {
                let d = PyDict::new_bound(py);
                d.set_item("src_ip", &src_ip)?;
                d.set_item("dst_ip", &dst_ip)?;
                d.set_item("udp_src_port", udp.src_port as i64)?;
                d.set_item("udp_dst_port", udp.dst_port as i64)?;
                d.set_item("someip_service_id", msg.service_id as i64)?;
                d.set_item("someip_method_id", msg.method_id as i64)?;
                // length field = 8 (fixed header after length) + payload
                let length = (8 + msg.payload.len()) as i64;
                d.set_item("someip_length", length)?;
                d.set_item("someip_client_id", msg.client_id as i64)?;
                d.set_item("someip_session_id", msg.session_id as i64)?;
                d.set_item("someip_protocol_version", msg.protocol_version as i64)?;
                d.set_item("someip_interface_version", msg.interface_version as i64)?;
                d.set_item("someip_msg_type", msg.message_type.to_u8() as i64)?;
                d.set_item("someip_return_code", msg.return_code.to_u8() as i64)?;
                d.set_item("payload", PyBytes::new_bound(py, &msg.payload))?;
                results.push(d);
            }
        }
        // advance: 8-byte fixed header (service+method+length) + length-field-value (8+payload)
        pos += 16 + msg.payload.len();
    }
    Ok(results)
}

/// Parse DoIP DiagMessages from an Ethernet frame payload.
///
/// Strips the IP and TCP headers (port 13400), then iterates over back-to-back
/// DoIP frames in the TCP segment.  Each DiagMessage yields a
/// ``(src_addr, target_addr, uds_payload)`` tuple where addresses are DoIP
/// logical addresses (integers) and ``uds_payload`` is the raw UDS bytes.
///
/// Returns an empty list if the frame is not TCP/13400 or contains no
/// DiagMessages.
#[pyfunction]
fn parse_doip_diag<'py>(
    py: Python<'py>,
    ether_type: u16,
    eth_payload: &[u8],
) -> PyResult<Vec<(i64, i64, Bound<'py, PyBytes>)>> {
    let ip = match blf::Ip::parse(ether_type, eth_payload) {
        Ok(ip) => ip,
        Err(_) => return Ok(vec![]),
    };
    let transport_data = match &ip {
        blf::Ip::V4(v4) => v4.parse_transport(),
        blf::Ip::V6(v6) => v6.parse_transport(),
    };
    let tcp = match transport_data {
        Ok(blf::eth::transport::Transport::Tcp(t)) => t,
        _ => return Ok(vec![]),
    };
    if tcp.src_port != DOIP_PORT && tcp.dst_port != DOIP_PORT {
        return Ok(vec![]);
    }

    let mut results = Vec::new();
    let mut pos = 0usize;
    while pos + 8 <= tcp.data.len() {
        let msg = match blf::DoIp::parse(Cursor::new(&tcp.data[pos..])) {
            Ok(m) => m,
            Err(_) => break,
        };
        let consumed = 8 + msg.payload.len();
        if msg.payload_type == blf::PayloadType::DiagMessage {
            if let Ok(diag) = msg.parse_diag_message() {
                let uds_bytes = PyBytes::new_bound(py, &diag.data);
                results.push((diag.src_addr as i64, diag.target_addr as i64, uds_bytes));
            }
        }
        pos += consumed;
    }
    Ok(results)
}

/// Parse a raw UDS payload (ISO 14229-1).
///
/// Returns ``(uds_type, service_id, service_name, nrc, nrc_name, data)``
/// or ``None`` if the payload is empty or malformed.
///
/// ``uds_type`` is one of ``"Request"``, ``"PositiveResponse"``, or
/// ``"NegativeResponse"``.  ``nrc`` and ``nrc_name`` are ``None`` unless
/// the PDU is a ``NegativeResponse``.
#[pyfunction]
fn parse_uds(data: &[u8]) -> Option<UdsTuple> {
    let uds = blf::Uds::parse(data).ok()?;
    Some(uds_to_tuple(uds))
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

fn ipv6_str(addr: &[u8; 16]) -> String {
    (0..8)
        .map(|i| {
            format!(
                "{:04x}",
                ((addr[i * 2] as u16) << 8) | addr[i * 2 + 1] as u16
            )
        })
        .collect::<Vec<_>>()
        .join(":")
}

fn uds_to_tuple(uds: blf::Uds) -> (String, i32, String, Option<i32>, Option<String>, Vec<u8>) {
    match uds {
        blf::Uds::Request { service, data } => (
            "Request".into(),
            service.to_u8() as i32,
            service.name(),
            None,
            None,
            data,
        ),
        blf::Uds::PositiveResponse { service, data } => (
            "PositiveResponse".into(),
            service.to_u8() as i32,
            service.name(),
            None,
            None,
            data,
        ),
        blf::Uds::NegativeResponse { service, nrc } => (
            "NegativeResponse".into(),
            service.to_u8() as i32,
            service.name(),
            Some(nrc.to_u8() as i32),
            Some(nrc.name()),
            vec![],
        ),
    }
}

/// Parse IP/TCP/UDP header fields from an Ethernet frame payload as named signals.
///
/// Returns a list of dicts matching ``_ETH_SIGNAL_RESULT_SCHEMA``:
/// ``{signal_name: str, signal_value: float | None, signal_str: str | None}``.
/// IPv4 and IPv6 are both supported.  Returns an empty list for non-IP frames or
/// frames that cannot be parsed.
///
/// Signal names emitted:
///   ``ip.protocol``, ``ip.ttl`` / ``ip.hop_limit``, ``ip.total_len`` (IPv4 only),
///   ``ip.src``, ``ip.dst``, ``tcp.src_port``, ``tcp.dst_port``, ``tcp.flags``,
///   ``udp.src_port``, ``udp.dst_port``, ``udp.payload_bytes``.
#[pyfunction]
fn parse_eth_payload_signals<'py>(
    py: Python<'py>,
    ether_type: u16,
    eth_payload: &[u8],
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let mut out: Vec<Bound<'py, PyDict>> = Vec::new();

    macro_rules! sig_num {
        ($name:expr, $value:expr) => {{
            let d = PyDict::new_bound(py);
            d.set_item("signal_name", $name)?;
            d.set_item("signal_value", $value as f64)?;
            d.set_item("signal_str", py.None())?;
            out.push(d);
        }};
    }
    macro_rules! sig_str {
        ($name:expr, $value:expr) => {{
            let d = PyDict::new_bound(py);
            d.set_item("signal_name", $name)?;
            d.set_item("signal_value", py.None())?;
            d.set_item("signal_str", $value)?;
            out.push(d);
        }};
    }

    // ARP (EtherType 0x0806) — IPv4-over-Ethernet only.
    if ether_type == 0x0806 {
        if let Ok(arp) = blf::Arp::parse(eth_payload) {
            sig_num!("arp.op", arp.operation.to_u16());
            sig_str!("arp.op_name", arp.operation.name());
            sig_str!(
                "arp.sender_ip",
                format!(
                    "{}.{}.{}.{}",
                    arp.sender_ip[0], arp.sender_ip[1], arp.sender_ip[2], arp.sender_ip[3]
                )
            );
            sig_str!(
                "arp.target_ip",
                format!(
                    "{}.{}.{}.{}",
                    arp.target_ip[0], arp.target_ip[1], arp.target_ip[2], arp.target_ip[3]
                )
            );
        }
        return Ok(out);
    }

    let ip = match blf::Ip::parse(ether_type, eth_payload) {
        Ok(ip) => ip,
        Err(_) => return Ok(out),
    };

    let transport_result = match &ip {
        blf::Ip::V4(v4) => {
            sig_num!("ip.protocol", v4.protocol.to_u8());
            sig_num!("ip.ttl", v4.ttl);
            sig_num!("ip.total_len", v4.total_length);
            sig_str!(
                "ip.src",
                format!(
                    "{}.{}.{}.{}",
                    v4.src_addr[0], v4.src_addr[1], v4.src_addr[2], v4.src_addr[3]
                )
            );
            sig_str!(
                "ip.dst",
                format!(
                    "{}.{}.{}.{}",
                    v4.dst_addr[0], v4.dst_addr[1], v4.dst_addr[2], v4.dst_addr[3]
                )
            );
            v4.parse_transport()
        }
        blf::Ip::V6(v6) => {
            sig_num!("ip.protocol", v6.next_header.to_u8());
            sig_num!("ip.hop_limit", v6.hop_limit);
            sig_str!("ip.src", ipv6_str(&v6.src_addr));
            sig_str!("ip.dst", ipv6_str(&v6.dst_addr));
            v6.parse_transport()
        }
    };

    if let Ok(transport) = transport_result {
        match transport {
            blf::eth::transport::Transport::Tcp(tcp) => {
                sig_num!("tcp.src_port", tcp.src_port);
                sig_num!("tcp.dst_port", tcp.dst_port);
                let flags_byte = (tcp.flags.fin as u8)
                    | (tcp.flags.syn as u8) << 1
                    | (tcp.flags.rst as u8) << 2
                    | (tcp.flags.psh as u8) << 3
                    | (tcp.flags.ack as u8) << 4
                    | (tcp.flags.urg as u8) << 5;
                sig_num!("tcp.flags", flags_byte);
            }
            blf::eth::transport::Transport::Udp(udp) => {
                sig_num!("udp.src_port", udp.src_port);
                sig_num!("udp.dst_port", udp.dst_port);
                sig_num!("udp.payload_bytes", udp.data.len());
            }
        }
    }

    // IGMP (IPv4 protocol 2) — parsed from the IPv4 payload.
    if let blf::Ip::V4(v4) = &ip {
        if v4.protocol == blf::IpProtocol::Igmp {
            if let Ok(igmp) = blf::Igmp::parse(v4.data.as_slice()) {
                sig_num!("igmp.type", igmp.igmp_type.to_u8());
                sig_str!("igmp.type_name", igmp.igmp_type.name());
                sig_str!(
                    "igmp.group",
                    format!(
                        "{}.{}.{}.{}",
                        igmp.group_addr[0],
                        igmp.group_addr[1],
                        igmp.group_addr[2],
                        igmp.group_addr[3]
                    )
                );
                sig_num!("igmp.max_resp_time", igmp.max_resp_time);
            }
        }
    }

    Ok(out)
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

/// Maps (type, channel_number) to a channel name.
///
/// CSV format (header required)::
///
///     type,channel,name
///     CAN,1,CAN_HS
///     Ethernet,1,ETH_BACKBONE
///
/// ``type`` is case-insensitive: ``CAN`` or ``Ethernet``.
#[pyclass]
pub struct ChannelDb {
    inner: blf::ChannelDb,
}

#[pymethods]
impl ChannelDb {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let f =
            File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let db = blf::ChannelDb::from_csv(f).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: db })
    }

    /// Look up the channel name for ``(type_str, channel)``.
    ///
    /// ``type_str`` is ``"CAN"`` or ``"Ethernet"`` (case-insensitive).
    /// Returns ``None`` when no mapping exists for the given pair.
    fn name(&self, type_str: &str, channel: u32) -> PyResult<Option<String>> {
        let ty = match type_str.to_lowercase().as_str() {
            "can" => blf::ChannelType::Can,
            "ethernet" => blf::ChannelType::Ethernet,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "invalid type: {:?}, expected CAN or Ethernet",
                    type_str
                )))
            }
        };
        Ok(self.inner.name(ty, channel).map(String::from))
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
    m.add_class::<ChannelDb>()?;
    m.add_class::<IsoTpReassembler>()?;
    m.add_function(wrap_pyfunction!(parse_someip_udp, m)?)?;
    m.add_function(wrap_pyfunction!(parse_doip_diag, m)?)?;
    m.add_function(wrap_pyfunction!(parse_uds, m)?)?;
    m.add_function(wrap_pyfunction!(parse_eth_payload_signals, m)?)?;
    Ok(())
}
