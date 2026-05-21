use crate::blf;
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
        format!("BaseObject(timestamp_ns={}, message={})", self.timestamp_ns, self.message.bind(py).repr().map(|s| s.to_string()).unwrap_or_default())
    }
}

// ── Reader ───────────────────────────────────────────────────────────────────

/// Iterator over BaseObjects in a BLF file.
///
/// Usage::
///
///     import vector_blf
///     for obj in vector_blf.Reader("path/to/file.blf"):
///         if isinstance(obj.message, vector_blf.Can):
///             print(obj.timestamp_ns, obj.message.id)
#[pyclass]
pub struct Reader {
    inner: blf::Reader<BufReader<File>>,
}

#[pymethods]
impl Reader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let f = File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let r = blf::Reader::new(BufReader::new(f))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: r })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<BaseObject> {
        match slf.inner.next() {
            None => Err(PyStopIteration::new_err(())),
            Some(Err(e)) => Err(pyo3::exceptions::PyValueError::new_err(e.to_string())),
            Some(Ok(obj)) => Ok(convert_base_object(py, obj)),
        }
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ")
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
        blf::Message::Can(m) => Py::new(py, Can {
            channel: m.channel,
            id: m.id,
            is_ext_id: m.is_ext_id,
            dir: m.dir.to_u8(),
            rtr: m.rtr,
            dlc: m.dlc,
            data: m.data,
        }).unwrap().into_any(),
        blf::Message::CanFd(m) => Py::new(py, CanFd {
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
        }).unwrap().into_any(),
        blf::Message::CanFd64(m) => Py::new(py, CanFd64 {
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
        }).unwrap().into_any(),
        blf::Message::Ethernet(m) => Py::new(py, Ethernet {
            channel: m.channel,
            dir: m.dir.to_u8(),
            src_addr: m.src_addr.to_vec(),
            dst_addr: m.dst_addr.to_vec(),
            ether_type: m.ether_type,
            data: m.data,
        }).unwrap().into_any(),
        blf::Message::EthernetEx(m) => Py::new(py, EthernetEx {
            channel: m.channel,
            dir: m.dir.to_u8(),
            src_addr: m.src_addr.to_vec(),
            dst_addr: m.dst_addr.to_vec(),
            ether_type: m.ether_type,
            data: m.data,
        }).unwrap().into_any(),
        blf::Message::Other(_, _) => py.None(),
    };
    BaseObject { timestamp_ns: ts, message: msg }
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
    Ok(())
}
