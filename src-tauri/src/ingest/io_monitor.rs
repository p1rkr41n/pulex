//! io_monitor.rs
//!
//! Tracks process-level disk I/O read/write byte counters and host memory/disk telemetry
//! on Windows via Win32 APIs (`GetProcessIoCounters`, `GlobalMemoryStatusEx`, `GetDiskFreeSpaceExW`).
//! Requires only Standard User permissions with negligible overhead.

use std::path::Path;

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemMemoryStatus {
    pub total_bytes: u64,
    pub avail_bytes: u64,
    pub memory_load_pct: u32,
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
    fn GetDiskFreeSpaceExW(
        lpDirectoryName: *const u16,
        lpFreeBytesAvailableToCaller: *mut u64,
        lpTotalNumberOfBytes: *mut u64,
        lpTotalNumberOfFreeBytes: *mut u64,
    ) -> i32;
}

/// Returns live host physical memory telemetry via Win32 `GlobalMemoryStatusEx`.
pub fn get_system_memory_status() -> Option<SystemMemoryStatus> {
    #[cfg(windows)]
    unsafe {
        let mut status = MemoryStatusEx {
            dw_length: std::mem::size_of::<MemoryStatusEx>() as u32,
            ..Default::default()
        };
        if GlobalMemoryStatusEx(&mut status) != 0 && status.ull_total_phys > 0 {
            Some(SystemMemoryStatus {
                total_bytes: status.ull_total_phys,
                avail_bytes: status.ull_avail_phys,
                memory_load_pct: status.dw_memory_load,
            })
        } else {
            None
        }
    }

    #[cfg(not(windows))]
    {
        None
    }
}

/// Returns total physical system RAM in bytes via Win32 `GlobalMemoryStatusEx`.
pub fn get_total_system_ram_bytes() -> Option<u64> {
    get_system_memory_status().map(|s| s.total_bytes)
}

/// Returns available disk space in bytes for the volume backing `path` via Win32 `GetDiskFreeSpaceExW`.
/// Falls back to the nearest existing ancestor directory when `path` does not yet exist.
pub fn get_available_disk_bytes(path: &Path) -> Option<u64> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;

        let target_dir = path
            .ancestors()
            .find(|p| !p.as_os_str().is_empty() && p.exists() && p.is_dir())
            .map(Path::to_path_buf)
            .or_else(|| {
                path.ancestors()
                    .find(|p| !p.as_os_str().is_empty() && p.exists())
                    .and_then(|p| p.parent())
                    .filter(|p| !p.as_os_str().is_empty() && p.exists())
                    .map(Path::to_path_buf)
            })
            .or_else(|| std::env::current_dir().ok())?;

        let wide: Vec<u16> = target_dir
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        let mut free_bytes_available: u64 = 0;
        let mut total_number_of_bytes: u64 = 0;
        let mut total_number_of_free_bytes: u64 = 0;

        let ok = unsafe {
            GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut free_bytes_available,
                &mut total_number_of_bytes,
                &mut total_number_of_free_bytes,
            )
        };
        if ok != 0 {
            Some(free_bytes_available)
        } else {
            None
        }
    }

    #[cfg(not(windows))]
    {
        let _ = path;
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

    #[test]
    fn test_get_system_memory_status() {
        #[cfg(windows)]
        {
            let status = get_system_memory_status().expect("GlobalMemoryStatusEx should succeed");
            assert!(status.total_bytes >= 512 * 1024 * 1024);
            assert!(status.avail_bytes <= status.total_bytes);
            assert!(status.memory_load_pct <= 100);
        }
    }

    #[test]
    fn test_get_available_disk_bytes_and_ancestor_fallback() {
        #[cfg(windows)]
        {
            let temp = std::env::temp_dir();
            let direct = get_available_disk_bytes(&temp).expect("GetDiskFreeSpaceExW should succeed on temp_dir");
            assert!(direct > 0);

            let nested_nonexistent = temp
                .join("pulex_nonexistent_dir_r1_test")
                .join("nested_missing")
                .join("duckdb_spill");
            let fallback = get_available_disk_bytes(&nested_nonexistent)
                .expect("GetDiskFreeSpaceExW should fall back to nearest existing ancestor");
            assert!(fallback > 0);
        }
    }
}

