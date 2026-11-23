use colibrii::{CredentialStore, FileStore};

fn scratch() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("colibrii-store-{}", colibrii::wire::ids::new_connection_id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[cfg(unix)]
#[test]
fn keeps_the_file_readable_only_by_its_owner() {
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch();
    let path = dir.join("nested").join("device.json");
    FileStore::new(&path).write("{}").unwrap();
    let mode = |path: &std::path::Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&path), 0o600);
    assert_eq!(mode(&dir.join("nested")), 0o700);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn reads_what_it_wrote_last_and_nothing_once_removed() {
    let dir = scratch();
    let store = FileStore::new(dir.join("plain.json"));
    assert_eq!(store.read().unwrap(), None);
    store.write("first").unwrap();
    store.write("second").unwrap();
    assert_eq!(store.read().unwrap().as_deref(), Some("second"));
    store.remove().unwrap();
    assert_eq!(store.read().unwrap(), None);
    store.remove().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
