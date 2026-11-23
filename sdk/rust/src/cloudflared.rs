//! The `cloudflared` the SDK pins (spec/tunnels.md, "Running it"): one
//! release, with its asset and the asset's SHA-256 for each system the SDK
//! supports. [`fetch_cloudflared`] is for an app's build step or first run,
//! and the tunnel runner looks for its copy before the `PATH`.

use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

pub const CLOUDFLARED_VERSION: &str = "2026.10.4";

/// A Mac's asset is a .tgz holding the binary, the others the binary.
fn asset() -> Option<(&'static str, &'static str)> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => {
            ("cloudflared-darwin-arm64.tgz", "68b6bc4080ac1330234db2a9002d07f98b000537a1bd531a687b660d4160fa7f")
        }
        ("macos", "x86_64") => {
            ("cloudflared-darwin-amd64.tgz", "f11fbef4207d715892331917dafd437f032cf4b7137ed64dea3008338c1c613d")
        }
        ("linux", "x86_64") => {
            ("cloudflared-linux-amd64", "ae5e8add7884b0531d66bbca4570d1de491ff6da51e4ebe7eb160cdf241d9e42")
        }
        ("windows", "x86_64") => {
            ("cloudflared-windows-amd64.exe", "411b16d40c8d872243efeb52c550053d00af0a8a97fbb32e730fb8aba429eaa8")
        }
        _ => return None,
    })
}

const BINARY: &str = "cloudflared";

/// Where [`fetch_cloudflared`] puts it in `directory`.
pub fn pinned_path(directory: &Path) -> PathBuf {
    directory.join(if cfg!(windows) { "cloudflared.exe" } else { BINARY })
}

/// Why the pinned `cloudflared` could not be fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchError {
    /// No pinned asset for this system.
    Unsupported,
    Download,
    Checksum,
    Unpack,
    Write,
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FetchError::Unsupported => "No cloudflared is pinned for this system.",
            FetchError::Download => "The pinned cloudflared could not be downloaded.",
            FetchError::Checksum => "The downloaded cloudflared is not the pinned one.",
            FetchError::Unpack => "The downloaded cloudflared could not be unpacked.",
            FetchError::Write => "The pinned cloudflared could not be written.",
        })
    }
}

impl std::error::Error for FetchError {}

fn field(header: &[u8], start: usize, length: usize) -> String {
    let bytes = &header[start..start + length];
    let end = bytes.iter().position(|byte| *byte == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// The file named `name` in a tar archive: each entry is a 512-byte header
/// and its body padded to 512 bytes, until two empty blocks.
fn from_tar(tar: &[u8], name: &str) -> Option<Vec<u8>> {
    let mut offset = 0;
    while offset + 512 <= tar.len() {
        let header = &tar[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            return None;
        }
        let size = usize::from_str_radix(field(header, 124, 12).trim(), 8).ok()?;
        let path = field(header, 0, 100);
        let path = path.strip_prefix("./").unwrap_or(&path);
        if path == name && matches!(header[156], b'0' | 0) {
            return tar.get(offset + 512..offset + 512 + size).map(<[u8]>::to_vec);
        }
        offset += 512 + size.div_ceil(512) * 512;
    }
    None
}

/// Downloads the pinned `cloudflared` for this system into `directory`,
/// checks its SHA-256, unpacks it and answers its path.
pub async fn fetch_cloudflared(directory: impl AsRef<Path>) -> Result<PathBuf, FetchError> {
    let directory = directory.as_ref().to_owned();
    crate::runtime::on_own_runtime(fetch(directory)).await
}

async fn fetch(directory: PathBuf) -> Result<PathBuf, FetchError> {
    let (name, sha256) = asset().ok_or(FetchError::Unsupported)?;
    let url = format!("https://github.com/cloudflare/cloudflared/releases/download/{CLOUDFLARED_VERSION}/{name}");
    let response = reqwest::get(url).await.map_err(|_| FetchError::Download)?;
    if !response.status().is_success() {
        return Err(FetchError::Download);
    }
    let download = response.bytes().await.map_err(|_| FetchError::Download)?;
    let digest: String = Sha256::digest(&download).iter().map(|byte| format!("{byte:02x}")).collect();
    if digest != sha256 {
        return Err(FetchError::Checksum);
    }
    let binary = if name.ends_with(".tgz") {
        let mut tar = Vec::new();
        flate2::read::GzDecoder::new(&download[..]).read_to_end(&mut tar).map_err(|_| FetchError::Unpack)?;
        from_tar(&tar, BINARY).ok_or(FetchError::Unpack)?
    } else {
        download.to_vec()
    };
    let path = pinned_path(&directory);
    // Whole or not at all, so the runner never finds half a binary.
    let mut next = path.clone().into_os_string();
    next.push(".next");
    let next = PathBuf::from(next);
    let written = async {
        tokio::fs::create_dir_all(&directory).await?;
        tokio::fs::write(&next, &binary).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&next, std::fs::Permissions::from_mode(0o755)).await?;
        }
        tokio::fs::rename(&next, &path).await
    };
    written.await.map_err(|_| FetchError::Write)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::from_tar;

    fn entry(name: &str, body: &[u8]) -> Vec<u8> {
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        let size = format!("{:011o}\0", body.len());
        header[124..136].copy_from_slice(size.as_bytes());
        header[156] = b'0';
        let mut out = header.to_vec();
        out.extend_from_slice(body);
        out.resize(512 + body.len().div_ceil(512) * 512, 0);
        out
    }

    #[test]
    fn finds_the_binary_in_a_tar_archive() {
        let mut tar = entry("./README", &[7; 600]);
        tar.extend(entry("./cloudflared", b"the binary"));
        tar.extend([0; 1024]);
        assert_eq!(from_tar(&tar, "cloudflared").as_deref(), Some(&b"the binary"[..]));
        assert_eq!(from_tar(&tar, "other"), None);
    }
}
