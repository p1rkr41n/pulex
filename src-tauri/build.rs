//! Enhanced build.rs for Pulex Tauri v2 backend
//!
//! Automatically ensures native libraries (comctl32.dll delay-loading, duckdb.lib search path,
//! duckdb.dll / WebView2Loader.dll runtime placement) and full Windows PE resources
//! (`RT_ICON`, `RT_GROUP_ICON`, `RT_VERSION`, `RT_MANIFEST`) are embedded across build profiles.

use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

fn main() {
    tauri_build::build();

    // Delay-load comctl32.dll (for headless tests) plus duckdb.dll and WebView2Loader.dll
    // so pulex.exe launches cleanly as a true single-file portable executable from any folder
    // and extracts its embedded runtime DLLs into pulexAppData_<id>/bin/ before first use.
    println!("cargo:rustc-link-arg=--delayload=comctl32.dll");
    println!("cargo:rustc-link-arg=--delayload=duckdb.dll");
    println!("cargo:rustc-link-arg=--delayload=WebView2Loader.dll");

    copy_runtime_dlls();
    embed_runtime_dlls();

    // Full PE Resource Embedding (RT_ICON, RT_GROUP_ICON, RT_VERSION, RT_MANIFEST)
    // with HelloKittyXamKinLung / Pulex branding.
    embed_pe_resources();
}

/// Automatically locate duckdb.dll (and WebView2Loader.dll) and copy them into the target profile
/// directory (e.g., target/debug, target/release) and its deps/ directory so that both application
/// binaries and `cargo test` runners execute cleanly without STATUS_DLL_NOT_FOUND (0xc0000135).
fn copy_runtime_dlls() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "windows" && !cfg!(target_os = "windows") {
        return;
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()));
    let out_dir = match env::var("OUT_DIR") {
        Ok(val) => PathBuf::from(val),
        Err(_) => return,
    };

    let lib_dir = manifest_dir.join("lib");
    if lib_dir.exists() {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }

    let target_profile_dir = match out_dir
        .ancestors()
        .find(|p| p.file_name().map_or(false, |name| name == "build"))
        .and_then(|build_dir| build_dir.parent())
    {
        Some(dir) => dir.to_path_buf(),
        None => return,
    };

    let target_root = match target_profile_dir.parent() {
        Some(p) => p.to_path_buf(),
        None => return,
    };

    let mut dest_dirs: Vec<PathBuf> = vec![
        target_profile_dir.clone(),
        target_profile_dir.join("deps"),
    ];
    for profile_name in &["debug", "release"] {
        let profile_dir = target_root.join(profile_name);
        if profile_dir.exists() {
            dest_dirs.push(profile_dir.clone());
            dest_dirs.push(profile_dir.join("deps"));
        }
    }
    dest_dirs.sort();
    dest_dirs.dedup();

    println!("cargo:rerun-if-env-changed=DUCKDB_LIB_DIR");

    for dll_name in &["duckdb.dll", "WebView2Loader.dll"] {
        let mut candidate_sources: Vec<PathBuf> = Vec::new();
        if *dll_name == "duckdb.dll" {
            if let Ok(env_dir) = env::var("DUCKDB_LIB_DIR") {
                candidate_sources.push(PathBuf::from(env_dir).join(dll_name));
            }
        }
        candidate_sources.push(manifest_dir.join("lib").join(dll_name));
        candidate_sources.push(manifest_dir.join(dll_name));
        if let Some(workspace_root) = manifest_dir.parent() {
            candidate_sources.push(workspace_root.join(dll_name));
        }
        candidate_sources.push(target_root.join("debug").join(dll_name));
        candidate_sources.push(target_root.join("debug").join("deps").join(dll_name));
        candidate_sources.push(target_root.join("release").join(dll_name));
        candidate_sources.push(target_root.join("release").join("deps").join(dll_name));

        if let Some(source_dll) = candidate_sources.into_iter().find(|p| p.is_file()) {
            println!("cargo:rerun-if-changed={}", source_dll.display());
            for dest_dir in &dest_dirs {
                let dest_file = dest_dir.join(dll_name);
                if let Err(e) = copy_if_different(&source_dll, &dest_file) {
                    println!(
                        "cargo:warning=Failed to copy {} to {}: {}",
                        dll_name,
                        dest_file.display(),
                        e
                    );
                }
            }
        } else if *dll_name == "duckdb.dll" {
            println!(
                "cargo:warning=duckdb.dll not found in candidate paths. Windows test executables may fail with STATUS_DLL_NOT_FOUND (0xc0000135)."
            );
        }
    }
}

fn find_runtime_dll(dll_name: &str, manifest_dir: &Path, target_root: &Path) -> Option<PathBuf> {
    let mut candidate_sources: Vec<PathBuf> = Vec::new();
    if dll_name == "duckdb.dll" {
        if let Ok(env_dir) = env::var("DUCKDB_LIB_DIR") {
            candidate_sources.push(PathBuf::from(env_dir).join(dll_name));
        }
    }
    candidate_sources.push(manifest_dir.join("lib").join(dll_name));
    candidate_sources.push(manifest_dir.join(dll_name));
    if let Some(workspace_root) = manifest_dir.parent() {
        candidate_sources.push(workspace_root.join(dll_name));
    }
    candidate_sources.push(target_root.join("debug").join(dll_name));
    candidate_sources.push(target_root.join("debug").join("deps").join(dll_name));
    candidate_sources.push(target_root.join("release").join(dll_name));
    candidate_sources.push(target_root.join("release").join("deps").join(dll_name));
    candidate_sources.into_iter().find(|p| p.is_file())
}

/// Compress duckdb.dll and WebView2Loader.dll with gzip into OUT_DIR for single-file .exe embedding.
fn embed_runtime_dlls() {
    let out_dir = match env::var("OUT_DIR") {
        Ok(val) => PathBuf::from(val),
        Err(_) => return,
    };
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()));
    let target_root = out_dir
        .ancestors()
        .find(|p| p.file_name().map_or(false, |name| name == "build"))
        .and_then(|build_dir| build_dir.parent())
        .and_then(|profile_dir| profile_dir.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| manifest_dir.join("target"));

    let mut duckdb_len: u64 = 0;
    let mut webview_len: u64 = 0;

    for dll_name in &["duckdb.dll", "WebView2Loader.dll"] {
        let gz_path = out_dir.join(format!("{}.gz", dll_name));
        let marker_path = out_dir.join(format!("{}.size", dll_name));

        if let Some(src_path) = find_runtime_dll(dll_name, &manifest_dir, &target_root) {
            println!("cargo:rerun-if-changed={}", src_path.display());
            let src_len = fs::metadata(&src_path).map(|m| m.len()).unwrap_or(0);
            if *dll_name == "duckdb.dll" {
                duckdb_len = src_len;
            } else {
                webview_len = src_len;
            }

            let cached_ok = gz_path.is_file()
                && fs::read_to_string(&marker_path)
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    == Some(src_len);

            if !cached_ok {
                let mut src_file = fs::File::open(&src_path)
                    .unwrap_or_else(|e| panic!("Failed to open {}: {}", src_path.display(), e));
                let mut raw = Vec::with_capacity(src_len as usize);
                src_file
                    .read_to_end(&mut raw)
                    .unwrap_or_else(|e| panic!("Failed to read {}: {}", src_path.display(), e));

                let out_file = fs::File::create(&gz_path)
                    .unwrap_or_else(|e| panic!("Failed to create {}: {}", gz_path.display(), e));
                let mut encoder = flate2::write::GzEncoder::new(out_file, flate2::Compression::best());
                encoder
                    .write_all(&raw)
                    .unwrap_or_else(|e| panic!("Failed to compress {}: {}", dll_name, e));
                encoder
                    .finish()
                    .unwrap_or_else(|e| panic!("Failed to finalize gzip for {}: {}", dll_name, e));
                let _ = fs::write(&marker_path, src_len.to_string());
            }
        } else {
            // Write empty placeholder so non-Windows / partial check builds still compile
            if !gz_path.exists() {
                let _ = fs::write(&gz_path, &[]);
            }
        }
    }

    let meta_rs = format!(
        "pub const DUCKDB_DLL_LEN: u64 = {};\npub const WEBVIEW2_DLL_LEN: u64 = {};\n",
        duckdb_len, webview_len
    );
    let _ = fs::write(out_dir.join("embedded_dll_meta.rs"), meta_rs);
}

fn copy_if_different(from: &Path, to: &Path) -> std::io::Result<()> {
    if from == to {
        return Ok(());
    }
    if to.exists() {
        if let (Ok(src_meta), Ok(dst_meta)) = (from.metadata(), to.metadata()) {
            if src_meta.len() == dst_meta.len() && src_meta.len() > 0 {
                return Ok(());
            }
        }
    }
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(from, to)?;
    Ok(())
}

fn embed_pe_resources() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "windows" && !cfg!(target_os = "windows") {
        return;
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()));
    let ico_path = manifest_dir.join("icons").join("icon.ico");
    if ico_path.exists() {
        println!("cargo:rerun-if-changed={}", ico_path.display());
    }

    let out_dir = match env::var("OUT_DIR") {
        Ok(val) => PathBuf::from(val),
        Err(_) => return,
    };

    let resource_o = out_dir.join("resource.o");
    let libresource_a = out_dir.join("libresource.a");

    if let Err(e) = generate_resource_coff(&resource_o, &ico_path) {
        println!("cargo:warning=Failed to generate PE resource COFF: {}", e);
        return;
    }

    let _ = fs::copy(&resource_o, &libresource_a);
}

fn pad4(vec: &mut Vec<u8>) {
    while vec.len() % 4 != 0 {
        vec.push(0);
    }
}

fn make_string_entry(key: &str, val: &str) -> Vec<u8> {
    let key_u16: Vec<u16> = key.encode_utf16().chain(std::iter::once(0)).collect();
    let val_u16: Vec<u16> = val.encode_utf16().chain(std::iter::once(0)).collect();
    let val_wlen = val_u16.len() as u16;

    let mut entry = Vec::new();
    entry.extend_from_slice(&0u16.to_le_bytes()); // wLength placeholder
    entry.extend_from_slice(&val_wlen.to_le_bytes()); // wValueLength
    entry.extend_from_slice(&1u16.to_le_bytes()); // wType = 1 (text)
    for c in key_u16 {
        entry.extend_from_slice(&c.to_le_bytes());
    }
    pad4(&mut entry);
    for c in val_u16 {
        entry.extend_from_slice(&c.to_le_bytes());
    }
    pad4(&mut entry);
    let len = entry.len() as u16;
    entry[0..2].copy_from_slice(&len.to_le_bytes());
    entry
}

fn build_version_info() -> Vec<u8> {
    let strings = [
        ("CompanyName", "HelloKittyXamKinLung"),
        ("FileDescription", "Pulex Data Analyzer"),
        ("FileVersion", "3.0.2"),
        ("InternalName", "pulex.exe"),
        ("LegalCopyright", "Copyright \u{00A9} 2026 HelloKittyXamKinLung"),
        ("OriginalFilename", "pulex.exe"),
        ("ProductName", "Pulex"),
        ("ProductVersion", "3.0.2"),
    ];

    let mut str_entries = Vec::new();
    for (k, v) in &strings {
        str_entries.extend(make_string_entry(k, v));
    }

    // StringTable "040904b0" (English US 0409, codepage 04b0 = 1200 Unicode)
    let st_key: Vec<u16> = "040904b0\0".encode_utf16().collect();
    let mut st = Vec::new();
    st.extend_from_slice(&0u16.to_le_bytes());
    st.extend_from_slice(&0u16.to_le_bytes());
    st.extend_from_slice(&1u16.to_le_bytes());
    for c in st_key {
        st.extend_from_slice(&c.to_le_bytes());
    }
    pad4(&mut st);
    st.extend(str_entries);
    pad4(&mut st);
    let st_len = st.len() as u16;
    st[0..2].copy_from_slice(&st_len.to_le_bytes());

    // StringFileInfo
    let sfi_key: Vec<u16> = "StringFileInfo\0".encode_utf16().collect();
    let mut sfi = Vec::new();
    sfi.extend_from_slice(&0u16.to_le_bytes());
    sfi.extend_from_slice(&0u16.to_le_bytes());
    sfi.extend_from_slice(&1u16.to_le_bytes());
    for c in sfi_key {
        sfi.extend_from_slice(&c.to_le_bytes());
    }
    pad4(&mut sfi);
    sfi.extend(st);
    pad4(&mut sfi);
    let sfi_len = sfi.len() as u16;
    sfi[0..2].copy_from_slice(&sfi_len.to_le_bytes());

    // VarFileInfo
    let var_key: Vec<u16> = "Translation\0".encode_utf16().collect();
    let mut var = Vec::new();
    var.extend_from_slice(&0u16.to_le_bytes());
    var.extend_from_slice(&4u16.to_le_bytes());
    var.extend_from_slice(&0u16.to_le_bytes());
    for c in var_key {
        var.extend_from_slice(&c.to_le_bytes());
    }
    pad4(&mut var);
    var.extend_from_slice(&0x0409u16.to_le_bytes()); // English US
    var.extend_from_slice(&0x04B0u16.to_le_bytes()); // 1200 Unicode
    pad4(&mut var);
    let var_len = var.len() as u16;
    var[0..2].copy_from_slice(&var_len.to_le_bytes());

    let vfi_key: Vec<u16> = "VarFileInfo\0".encode_utf16().collect();
    let mut vfi = Vec::new();
    vfi.extend_from_slice(&0u16.to_le_bytes());
    vfi.extend_from_slice(&0u16.to_le_bytes());
    vfi.extend_from_slice(&1u16.to_le_bytes());
    for c in vfi_key {
        vfi.extend_from_slice(&c.to_le_bytes());
    }
    pad4(&mut vfi);
    vfi.extend(var);
    pad4(&mut vfi);
    let vfi_len = vfi.len() as u16;
    vfi[0..2].copy_from_slice(&vfi_len.to_le_bytes());

    // VS_FIXEDFILEINFO (52 bytes): 3.0.2.0
    let file_ver_ms: u32 = 3 << 16;
    let file_ver_ls: u32 = 2 << 16;
    let prod_ver_ms: u32 = 3 << 16;
    let prod_ver_ls: u32 = 2 << 16;
    let mut ffi = Vec::new();
    ffi.extend_from_slice(&0xFEEF04BDu32.to_le_bytes());
    ffi.extend_from_slice(&0x00010000u32.to_le_bytes());
    ffi.extend_from_slice(&file_ver_ms.to_le_bytes());
    ffi.extend_from_slice(&file_ver_ls.to_le_bytes());
    ffi.extend_from_slice(&prod_ver_ms.to_le_bytes());
    ffi.extend_from_slice(&prod_ver_ls.to_le_bytes());
    ffi.extend_from_slice(&0x0000003Fu32.to_le_bytes());
    ffi.extend_from_slice(&0x00000000u32.to_le_bytes());
    ffi.extend_from_slice(&0x00040004u32.to_le_bytes());
    ffi.extend_from_slice(&0x00000001u32.to_le_bytes());
    ffi.extend_from_slice(&0x00000000u32.to_le_bytes());
    ffi.extend_from_slice(&0x00000000u32.to_le_bytes());
    ffi.extend_from_slice(&0x00000000u32.to_le_bytes());

    let root_key: Vec<u16> = "VS_VERSION_INFO\0".encode_utf16().collect();
    let mut root = Vec::new();
    root.extend_from_slice(&0u16.to_le_bytes());
    root.extend_from_slice(&(ffi.len() as u16).to_le_bytes());
    root.extend_from_slice(&0u16.to_le_bytes());
    for c in root_key {
        root.extend_from_slice(&c.to_le_bytes());
    }
    pad4(&mut root);
    root.extend(ffi);
    root.extend(sfi);
    root.extend(vfi);
    pad4(&mut root);
    let root_len = root.len() as u16;
    root[0..2].copy_from_slice(&root_len.to_le_bytes());
    root
}

fn build_manifest_xml() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0" xmlns:asmv3="urn:schemas-microsoft-com:asm.v3">
  <assemblyIdentity type="win32" name="com.pulex.analyzer" version="3.0.2.0" processorArchitecture="amd64"/>
  <dependency>
    <dependentAssembly>
      <assemblyIdentity type="win32" name="Microsoft.Windows.Common-Controls" version="6.0.0.0" processorArchitecture="*" publicKeyToken="6595b64144ccf1df" language="*"/>
    </dependentAssembly>
  </dependency>
  <trustInfo xmlns="urn:schemas-microsoft-com:asm.v3">
    <security>
      <requestedPrivileges>
        <requestedExecutionLevel level="asInvoker" uiAccess="false"/>
      </requestedPrivileges>
    </security>
  </trustInfo>
  <compatibility xmlns="urn:schemas-microsoft-com:compatibility.v1">
    <application>
      <supportedOS Id="{8e0f7a12-bfb3-4fe8-b9a5-48fd50a15a9a}"/>
      <supportedOS Id="{1f676c76-80e1-4239-95bb-83d0f6d0da78}"/>
      <supportedOS Id="{4a2f28e3-53b9-4441-ba9c-d69d4a4a6e38}"/>
      <supportedOS Id="{35138b9a-5d96-4fbd-8e2d-a2440225f93a}"/>
    </application>
  </compatibility>
  <asmv3:application>
    <asmv3:windowsSettings>
      <dpiAware xmlns="http://schemas.microsoft.com/SMI/2005/WindowsSettings">true/pm</dpiAware>
      <dpiAwareness xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">PerMonitorV2, PerMonitor</dpiAwareness>
      <longPathAware xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">true</longPathAware>
    </asmv3:windowsSettings>
  </asmv3:application>
</assembly>"#
        .to_vec()
}

/// Parses an `.ico` file into `(Vec<(icon_id, raw_icon_bytes)>, grp_icon_dir_bytes)`.
fn parse_ico_for_pe(ico_path: &Path) -> Option<(Vec<(u32, Vec<u8>)>, Vec<u8>)> {
    let bytes = fs::read(ico_path).ok()?;
    if bytes.len() < 6 {
        return None;
    }
    let reserved = u16::from_le_bytes([bytes[0], bytes[1]]);
    let ico_type = u16::from_le_bytes([bytes[2], bytes[3]]);
    let count = u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
    if reserved != 0 || ico_type != 1 || count == 0 || bytes.len() < 6 + count * 16 {
        return None;
    }

    let mut grp_dir = Vec::with_capacity(6 + count * 14);
    grp_dir.extend_from_slice(&0u16.to_le_bytes());
    grp_dir.extend_from_slice(&1u16.to_le_bytes());
    grp_dir.extend_from_slice(&(count as u16).to_le_bytes());

    let mut icons = Vec::with_capacity(count);
    for idx in 0..count {
        let entry_off = 6 + idx * 16;
        let width = bytes[entry_off];
        let height = bytes[entry_off + 1];
        let color_count = bytes[entry_off + 2];
        let entry_reserved = bytes[entry_off + 3];
        let planes = u16::from_le_bytes([bytes[entry_off + 4], bytes[entry_off + 5]]);
        let bit_count = u16::from_le_bytes([bytes[entry_off + 6], bytes[entry_off + 7]]);
        let bytes_in_res = u32::from_le_bytes([
            bytes[entry_off + 8],
            bytes[entry_off + 9],
            bytes[entry_off + 10],
            bytes[entry_off + 11],
        ]) as usize;
        let image_offset = u32::from_le_bytes([
            bytes[entry_off + 12],
            bytes[entry_off + 13],
            bytes[entry_off + 14],
            bytes[entry_off + 15],
        ]) as usize;

        if image_offset + bytes_in_res > bytes.len() {
            return None;
        }
        let img_data = bytes[image_offset..image_offset + bytes_in_res].to_vec();
        let icon_id = (idx + 1) as u16;

        // GRPICONDIRENTRY (14 bytes, packed)
        grp_dir.push(width);
        grp_dir.push(height);
        grp_dir.push(color_count);
        grp_dir.push(entry_reserved);
        grp_dir.extend_from_slice(&planes.to_le_bytes());
        grp_dir.extend_from_slice(&bit_count.to_le_bytes());
        grp_dir.extend_from_slice(&(img_data.len() as u32).to_le_bytes());
        grp_dir.extend_from_slice(&icon_id.to_le_bytes());

        icons.push((icon_id as u32, img_data));
    }

    Some((icons, grp_dir))
}

struct ResourceLeaf {
    type_id: u32,
    name_id: u32,
    lang_id: u32,
    data: Vec<u8>,
}

fn generate_resource_coff(dest_path: &Path, ico_path: &Path) -> std::io::Result<()> {
    // PE Resource Types (must be sorted ascending by type_id, then name_id):
    // RT_ICON = 3, RT_GROUP_ICON = 14, RT_VERSION = 16, RT_MANIFEST = 24
    let mut leaves: Vec<ResourceLeaf> = Vec::new();

    if let Some((icons, grp_dir)) = parse_ico_for_pe(ico_path) {
        for (icon_id, icon_bytes) in icons {
            leaves.push(ResourceLeaf {
                type_id: 3, // RT_ICON
                name_id: icon_id,
                lang_id: 0x0409,
                data: icon_bytes,
            });
        }
        leaves.push(ResourceLeaf {
            type_id: 14, // RT_GROUP_ICON
            name_id: 32512, // IDI_APPLICATION (resolved by both Windows Explorer and Tauri v2)
            lang_id: 0x0409,
            data: grp_dir,
        });
    }

    leaves.push(ResourceLeaf {
        type_id: 16, // RT_VERSION
        name_id: 1,
        lang_id: 0x0409,
        data: build_version_info(),
    });

    leaves.push(ResourceLeaf {
        type_id: 24, // RT_MANIFEST
        name_id: 1,
        lang_id: 0x0409,
        data: build_manifest_xml(),
    });

    leaves.sort_by_key(|l| (l.type_id, l.name_id, l.lang_id));

    // Group leaves by type_id preserving ascending order
    let mut by_type: Vec<(u32, Vec<usize>)> = Vec::new();
    for (idx, leaf) in leaves.iter().enumerate() {
        if let Some(last) = by_type.last_mut() {
            if last.0 == leaf.type_id {
                last.1.push(idx);
                continue;
            }
        }
        by_type.push((leaf.type_id, vec![idx]));
    }

    let num_types = by_type.len();
    let num_leaves = leaves.len();

    // Calculate deterministic layout inside .rsrc section:
    // 1. Root directory: 16 + 8 * num_types
    let root_dir_offset: usize = 0;
    let root_dir_size: usize = 16 + 8 * num_types;

    // 2. Type directories (one per distinct type_id)
    let mut type_dir_offsets = Vec::with_capacity(num_types);
    let mut cursor = root_dir_offset + root_dir_size;
    for (_, leaf_indices) in &by_type {
        type_dir_offsets.push(cursor);
        cursor += 16 + 8 * leaf_indices.len();
    }

    // 3. Language directories (one per leaf, each has 1 lang entry -> 24 bytes)
    let mut lang_dir_offsets = Vec::with_capacity(num_leaves);
    for _ in 0..num_leaves {
        lang_dir_offsets.push(cursor);
        cursor += 24;
    }

    // 4. IMAGE_RESOURCE_DATA_ENTRY array (16 bytes per leaf)
    let mut data_entry_offsets = Vec::with_capacity(num_leaves);
    for _ in 0..num_leaves {
        data_entry_offsets.push(cursor);
        cursor += 16;
    }

    // 5. Raw payload blobs (4-byte aligned)
    let mut raw_data_offsets = Vec::with_capacity(num_leaves);
    let mut raw_blobs = Vec::new();
    for leaf in &leaves {
        let blob_offset = cursor + raw_blobs.len();
        raw_data_offsets.push(blob_offset);
        raw_blobs.extend_from_slice(&leaf.data);
        pad4(&mut raw_blobs);
    }

    // Assemble .rsrc section bytes
    let mut rsrc = Vec::with_capacity(cursor + raw_blobs.len());

    // Emit Root directory
    rsrc.extend_from_slice(&[0u8; 12]);
    rsrc.extend_from_slice(&0u16.to_le_bytes());
    rsrc.extend_from_slice(&(num_types as u16).to_le_bytes());
    for (t_idx, (type_id, _)) in by_type.iter().enumerate() {
        rsrc.extend_from_slice(&type_id.to_le_bytes());
        rsrc.extend_from_slice(&(0x80000000u32 | (type_dir_offsets[t_idx] as u32)).to_le_bytes());
    }

    // Emit Type directories
    for (_, leaf_indices) in &by_type {
        rsrc.extend_from_slice(&[0u8; 12]);
        rsrc.extend_from_slice(&0u16.to_le_bytes());
        rsrc.extend_from_slice(&(leaf_indices.len() as u16).to_le_bytes());
        for &leaf_idx in leaf_indices {
            rsrc.extend_from_slice(&leaves[leaf_idx].name_id.to_le_bytes());
            rsrc.extend_from_slice(
                &(0x80000000u32 | (lang_dir_offsets[leaf_idx] as u32)).to_le_bytes(),
            );
        }
    }

    // Emit Language directories
    for leaf_idx in 0..num_leaves {
        rsrc.extend_from_slice(&[0u8; 12]);
        rsrc.extend_from_slice(&0u16.to_le_bytes());
        rsrc.extend_from_slice(&1u16.to_le_bytes());
        rsrc.extend_from_slice(&leaves[leaf_idx].lang_id.to_le_bytes());
        rsrc.extend_from_slice(&(data_entry_offsets[leaf_idx] as u32).to_le_bytes());
    }

    // Emit IMAGE_RESOURCE_DATA_ENTRY records (OffsetToData gets relocated via ADDR32NB)
    for leaf_idx in 0..num_leaves {
        rsrc.extend_from_slice(&(raw_data_offsets[leaf_idx] as u32).to_le_bytes());
        rsrc.extend_from_slice(&(leaves[leaf_idx].data.len() as u32).to_le_bytes());
        rsrc.extend_from_slice(&0u32.to_le_bytes());
        rsrc.extend_from_slice(&0u32.to_le_bytes());
    }

    // Emit Raw data blobs
    rsrc.extend_from_slice(&raw_blobs);
    pad4(&mut rsrc);

    let sec_data_offset: u32 = 60; // 20-byte COFF header + 40-byte section header
    let sec_data_len: u32 = rsrc.len() as u32;
    let reloc_offset: u32 = sec_data_offset + sec_data_len;
    let num_relocs: u16 = num_leaves as u16;
    let symtab_offset: u32 = reloc_offset + (num_relocs as u32) * 10;

    let mut coff = Vec::with_capacity(symtab_offset as usize + 22);
    // COFF Header (20 bytes)
    coff.extend_from_slice(&0x8664u16.to_le_bytes()); // Machine AMD64
    coff.extend_from_slice(&1u16.to_le_bytes()); // NumberOfSections = 1
    coff.extend_from_slice(&0u32.to_le_bytes()); // TimeDateStamp
    coff.extend_from_slice(&symtab_offset.to_le_bytes());
    coff.extend_from_slice(&1u32.to_le_bytes()); // NumberOfSymbols = 1
    coff.extend_from_slice(&0u16.to_le_bytes()); // SizeOfOptionalHeader = 0
    coff.extend_from_slice(&0u16.to_le_bytes()); // Characteristics = 0

    // Section Header (40 bytes)
    let mut sec_name = [0u8; 8];
    sec_name[..5].copy_from_slice(b".rsrc");
    coff.extend_from_slice(&sec_name);
    coff.extend_from_slice(&0u32.to_le_bytes());
    coff.extend_from_slice(&0u32.to_le_bytes());
    coff.extend_from_slice(&sec_data_len.to_le_bytes());
    coff.extend_from_slice(&sec_data_offset.to_le_bytes());
    coff.extend_from_slice(&reloc_offset.to_le_bytes());
    coff.extend_from_slice(&0u32.to_le_bytes());
    coff.extend_from_slice(&num_relocs.to_le_bytes());
    coff.extend_from_slice(&0u16.to_le_bytes());
    coff.extend_from_slice(&0x40000040u32.to_le_bytes()); // INITIALIZED_DATA | READ

    // Section Data
    coff.extend_from_slice(&rsrc);

    // Relocations (10 bytes each, IMAGE_REL_AMD64_ADDR32NB = 3)
    for &entry_off in &data_entry_offsets {
        coff.extend_from_slice(&(entry_off as u32).to_le_bytes());
        coff.extend_from_slice(&0u32.to_le_bytes()); // Symbol index 0 (.rsrc)
        coff.extend_from_slice(&3u16.to_le_bytes()); // IMAGE_REL_AMD64_ADDR32NB
    }

    // Symbol Table (18 bytes)
    coff.extend_from_slice(&sec_name);
    coff.extend_from_slice(&0u32.to_le_bytes());
    coff.extend_from_slice(&1i16.to_le_bytes());
    coff.extend_from_slice(&0u16.to_le_bytes());
    coff.push(3u8); // IMAGE_SYM_CLASS_STATIC
    coff.push(0u8);

    // String Table Header (4 bytes)
    coff.extend_from_slice(&4u32.to_le_bytes());

    fs::write(dest_path, coff)?;
    Ok(())
}
