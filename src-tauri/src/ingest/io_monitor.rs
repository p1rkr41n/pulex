//! io_monitor.rs
//!
//! Tracks process-level disk I/O read/write byte counters on Windows via Win32 `GetProcessIoCounters`.
//! Requires only Standard User permissions and reads kernel memory in < 0.001ms with zero disk overhead.

#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
pub struct IoCounters {
    pub read_operation_count: u64,
    pub write_operation_count: u64,
    pub other_operation_count: u64,
    pub read_transfer_count: u64,
    pub write_transfer_count: u64,
    pub other_transfer_count: u64,
}

#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
struct MemoryStatusEx {
    dw_length: u32,
    dw_memory_load: u32,
    ull_total_phys: u64,
    ull_avail_phys: u64,
    ull_total_page_file: u64,
    ull_avail_page_file: u64,
    ull_total_virtual: u64,
    ull_avail_virtual: u64,
    ull_avail_extended_virtual: u64,
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn OpenProcess(
        dwDesiredAccess: u32,
        bInheritHandle: i32,
        dwProcessId: u32,
    ) -> *mut std::ffi::c_void;
    fn GetProcessIoCounters(
        hProcess: *mut std::ffi::c_void,
        lpIoCounters: *mut IoCounters,
    ) -> i32;
    fn CloseHandle(hObject: *mut std::ffi::c_void) -> i32;
    fn GetCurrentProcess() -> *mut std::ffi::c_void;
    fn GlobalMemoryStatusEx(lpBuffer: *mut MemoryStatusEx) -> i32;
}

/// Returns total physical system RAM in bytes via Win32 `GlobalMemoryStatusEx`.
pub fn get_total_system_ram_bytes() -> Option<u64> {
    #[cfg(windows)]
    unsafe {
        let mut status = MemoryStatusEx {
            dw_length: std::mem::size_of::<MemoryStatusEx>() as u32,
            ..Default::default()
        };
        if GlobalMemoryStatusEx(&mut status) != 0 && status.ull_total_phys > 0 {
            Some(status.ull_total_phys)
        } else {
            None
        }
    }

    #[cfg(not(windows))]
    {
        None
    }
}

/// Returns cumulative `(read_bytes, write_bytes)` for the target process ID.
/// Pass `0` or `std::process::id()` to query the current process without `OpenProcess`.
pub fn get_process_io_bytes(pid: u32) -> Option<(u64, u64)> {
    #[cfg(windows)]
    unsafe {
        let mut counters = IoCounters::default();
        let current = GetCurrentProcess();
        let handle = if pid == 0 || pid == std::process::id() {
            current
        } else {
            // PROCESS_QUERY_LIMITED_INFORMATION (0x1000)
            let mut h = OpenProcess(0x1000, 0, pid);
            if h.is_null() {
                // Fallback to PROCESS_QUERY_INFORMATION (0x0400)
                h = OpenProcess(0x0400, 0, pid);
            }
            h
        };

        if handle.is_null() {
            return None;
        }

        let is_current = handle == current;
        let ok = GetProcessIoCounters(handle, &mut counters);
        if !is_current {
            CloseHandle(handle);
        }

        if ok != 0 {
            Some((counters.read_transfer_count, counters.write_transfer_count))
        } else {
            None
        }
    }

    #[cfg(not(windows))]
    {
        let _ = pid;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_current_process_io_bytes() {
        #[cfg(windows)]
        {
            let res = get_process_io_bytes(0);
            assert!(res.is_some(), "GetProcessIoCounters should succeed for current process");
        }
    }

    #[test]
    fn test_get_total_system_ram_bytes() {
        #[cfg(windows)]
        {
            let ram = get_total_system_ram_bytes().expect("GlobalMemoryStatusEx should return RAM");
            assert!(ram >= 512 * 1024 * 1024, "System RAM should be at least 512 MB");
        }
    }
}

