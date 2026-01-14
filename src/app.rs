use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path as AxumPath, Query, State, WebSocketUpgrade,
    },
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use minijinja::{context, value::Value, Environment};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fs,
    net::Ipv6Addr,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::SystemTime,
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, Mutex},
};
use tower_http::cors::CorsLayer;

const TEMPLATE_NAME: &str = "main.html";
static TEMPLATE_ENV: OnceLock<Environment<'static>> = OnceLock::new();
const MERMAID_JS: &str = include_str!("../static/js/mermaid.min.js");
const MERMAID_ETAG: &str = concat!("\"", env!("CARGO_PKG_VERSION"), "\"");

type SharedMarkdownState = Arc<Mutex<MarkdownState>>;

fn template_env() -> &'static Environment<'static> {
    TEMPLATE_ENV.get_or_init(|| {
        let mut env = Environment::new();
        minijinja_embed::load_templates!(&mut env);
        env
    })
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum ClientMessage {
    Ping,
    RequestRefresh,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum ServerMessage {
    Reload,
    Pong,
}

use std::collections::HashMap;

#[derive(Deserialize)]
struct FileQuery {
    file: Option<String>,
}

struct NavDir {
    name: String,
    children: Vec<NavNode>,
    is_open: bool,
}

struct NavFile {
    name: String,
    path: String,
    url: String,
    is_active: bool,
}

enum NavNode {
    Dir(NavDir),
    File(NavFile),
}

pub fn scan_markdown_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut md_files = Vec::new();

    scan_markdown_files_recursive(dir, &mut md_files)?;

    md_files.sort_by(|a, b| b.cmp(a));

    Ok(md_files)
}

fn scan_markdown_files_recursive(dir: &Path, md_files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            scan_markdown_files_recursive(&path, md_files)?;
        } else if path.is_file() && is_markdown_file(&path) {
            md_files.push(path);
        }
    }

    Ok(())
}

fn is_markdown_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("md") || ext.eq_ignore_ascii_case("markdown"))
        .unwrap_or(false)
}

fn relative_path_key(base_dir: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(base_dir).unwrap_or(path);
    relative.to_string_lossy().replace('\\', "/")
}

fn scan_directory_paths(base_dir: &Path) -> Result<Vec<String>> {
    let mut dirs = Vec::new();
    scan_directory_paths_recursive(base_dir, base_dir, &mut dirs)?;
    Ok(dirs)
}

fn scan_directory_paths_recursive(
    base_dir: &Path,
    dir: &Path,
    dirs: &mut Vec<String>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            dirs.push(relative_path_key(base_dir, &path));
            scan_directory_paths_recursive(base_dir, &path, dirs)?;
        }
    }

    Ok(())
}

fn file_href(path: &str) -> String {
    if path.contains('/') {
        format!("/?file={path}")
    } else {
        format!("/{path}")
    }
}

impl NavNode {
    fn name(&self) -> &str {
        match self {
            NavNode::Dir(dir) => &dir.name,
            NavNode::File(file) => &file.name,
        }
    }

    fn is_dir(&self) -> bool {
        matches!(self, NavNode::Dir(_))
    }

    fn to_value(&self) -> Value {
        let mut map = HashMap::new();
        match self {
            NavNode::Dir(dir) => {
                map.insert("kind".to_string(), Value::from("dir"));
                map.insert("name".to_string(), Value::from(dir.name.clone()));
                map.insert("is_open".to_string(), Value::from(dir.is_open));
                let children: Value = dir.children.iter().map(|child| child.to_value()).collect();
                map.insert("children".to_string(), children);
            }
            NavNode::File(file) => {
                map.insert("kind".to_string(), Value::from("file"));
                map.insert("name".to_string(), Value::from(file.name.clone()));
                map.insert("path".to_string(), Value::from(file.path.clone()));
                map.insert("url".to_string(), Value::from(file.url.clone()));
                map.insert("is_active".to_string(), Value::from(file.is_active));
            }
        }
        Value::from_object(map)
    }
}

fn insert_nav_path(nodes: &mut Vec<NavNode>, path: &str, current_file: &str) {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    insert_nav_parts(nodes, &parts, path, current_file, "");
}

fn insert_dir_path(nodes: &mut Vec<NavNode>, path: &str) {
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    insert_dir_parts(nodes, &parts);
}

fn insert_dir_parts(nodes: &mut Vec<NavNode>, parts: &[&str]) {
    if parts.is_empty() {
        return;
    }

    let dir_name = parts[0];
    let dir_index = nodes.iter().position(|node| match node {
        NavNode::Dir(dir) => dir.name == dir_name,
        _ => false,
    });

    let index = if let Some(index) = dir_index {
        index
    } else {
        nodes.push(NavNode::Dir(NavDir {
            name: dir_name.to_string(),
            children: Vec::new(),
            is_open: false,
        }));
        nodes.len() - 1
    };

    if let NavNode::Dir(dir) = &mut nodes[index] {
        insert_dir_parts(&mut dir.children, &parts[1..]);
    }
}

fn insert_nav_parts(
    nodes: &mut Vec<NavNode>,
    parts: &[&str],
    full_path: &str,
    current_file: &str,
    prefix: &str,
) {
    if parts.is_empty() {
        return;
    }

    if parts.len() == 1 {
        let name = parts[0].to_string();
        nodes.push(NavNode::File(NavFile {
            name,
            path: full_path.to_string(),
            url: file_href(full_path),
            is_active: full_path == current_file,
        }));
        return;
    }

    let dir_name = parts[0];
    let dir_path = if prefix.is_empty() {
        dir_name.to_string()
    } else {
        format!("{prefix}/{dir_name}")
    };

    let dir_index = nodes.iter().position(|node| match node {
        NavNode::Dir(dir) => dir.name == dir_name,
        _ => false,
    });

    let index = if let Some(index) = dir_index {
        index
    } else {
        nodes.push(NavNode::Dir(NavDir {
            name: dir_name.to_string(),
            children: Vec::new(),
            is_open: false,
        }));
        nodes.len() - 1
    };

    if let NavNode::Dir(dir) = &mut nodes[index] {
        insert_nav_parts(&mut dir.children, &parts[1..], full_path, current_file, &dir_path);
    }
}

fn mark_nav_open(node: &mut NavNode) -> bool {
    match node {
        NavNode::File(file) => file.is_active,
        NavNode::Dir(dir) => {
            let mut has_active = false;
            for child in &mut dir.children {
                if mark_nav_open(child) {
                    has_active = true;
                }
            }
            dir.is_open = has_active;
            has_active
        }
    }
}

fn sort_nav_nodes(nodes: &mut Vec<NavNode>) {
    for node in nodes.iter_mut() {
        if let NavNode::Dir(dir) = node {
            sort_nav_nodes(&mut dir.children);
        }
    }

    nodes.sort_by(|a, b| match (a.is_dir(), b.is_dir()) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => b.name().cmp(a.name()),
    });
}

struct TrackedFile {
    path: PathBuf,
    last_modified: SystemTime,
    html: String,
}

struct MarkdownState {
    base_dir: PathBuf,
    tracked_files: HashMap<String, TrackedFile>,
    is_directory_mode: bool,
    change_tx: broadcast::Sender<ServerMessage>,
}

impl MarkdownState {
    fn new(base_dir: PathBuf, file_paths: Vec<PathBuf>, is_directory_mode: bool) -> Result<Self> {
        let (change_tx, _) = broadcast::channel::<ServerMessage>(16);

        let mut tracked_files = HashMap::new();
        for file_path in file_paths {
            let metadata = fs::metadata(&file_path)?;
            let last_modified = metadata.modified()?;
            let content = fs::read_to_string(&file_path)?;
            let html = Self::markdown_to_html(&content)?;

            let filename = relative_path_key(&base_dir, &file_path);

            tracked_files.insert(
                filename,
                TrackedFile {
                    path: file_path,
                    last_modified,
                    html,
                },
            );
        }

        Ok(MarkdownState {
            base_dir,
            tracked_files,
            is_directory_mode,
            change_tx,
        })
    }

    fn show_navigation(&self) -> bool {
        self.is_directory_mode
    }

    fn get_sorted_filenames(&self) -> Vec<String> {
        let mut filenames: Vec<_> = self.tracked_files.keys().cloned().collect();
        filenames.sort_by(|a, b| b.cmp(a));
        filenames
    }

    fn build_navigation_tree(&self, current_file: &str) -> Vec<Value> {
        let mut nodes = Vec::new();

        if let Ok(dirs) = scan_directory_paths(&self.base_dir) {
            for dir in dirs {
                insert_dir_path(&mut nodes, &dir);
            }
        }

        for path in self.tracked_files.keys() {
            insert_nav_path(&mut nodes, path, current_file);
        }

        for node in nodes.iter_mut() {
            mark_nav_open(node);
        }

        sort_nav_nodes(&mut nodes);

        nodes.into_iter().map(|node| node.to_value()).collect()
    }

    fn rescan_tracked_files(&mut self) -> Result<()> {
        let file_paths = scan_markdown_files(&self.base_dir)?;
        let mut tracked_files = HashMap::new();

        for file_path in file_paths {
            let metadata = fs::metadata(&file_path)?;
            let last_modified = metadata.modified()?;
            let content = fs::read_to_string(&file_path)?;
            let html = Self::markdown_to_html(&content)?;
            let filename = relative_path_key(&self.base_dir, &file_path);

            tracked_files.insert(
                filename,
                TrackedFile {
                    path: file_path,
                    last_modified,
                    html,
                },
            );
        }

        self.tracked_files = tracked_files;
        Ok(())
    }

    fn refresh_file(&mut self, filename: &str) -> Result<()> {
        if let Some(tracked) = self.tracked_files.get_mut(filename) {
            let metadata = fs::metadata(&tracked.path)?;
            let current_modified = metadata.modified()?;

            if current_modified > tracked.last_modified {
                let content = fs::read_to_string(&tracked.path)?;
                tracked.html = Self::markdown_to_html(&content)?;
                tracked.last_modified = current_modified;
            }
        }

        Ok(())
    }

    fn add_tracked_file(&mut self, file_path: PathBuf) -> Result<()> {
        let filename = relative_path_key(&self.base_dir, &file_path);

        if self.tracked_files.contains_key(&filename) {
            return Ok(());
        }

        let metadata = fs::metadata(&file_path)?;
        let content = fs::read_to_string(&file_path)?;

        self.tracked_files.insert(
            filename,
            TrackedFile {
                path: file_path,
                last_modified: metadata.modified()?,
                html: Self::markdown_to_html(&content)?,
            },
        );

        Ok(())
    }

    fn markdown_to_html(content: &str) -> Result<String> {
        let mut options = markdown::Options::gfm();
        options.compile.allow_dangerous_html = true;
        options.parse.constructs.frontmatter = true;

        let html_body = markdown::to_html_with_options(content, &options)
            .unwrap_or_else(|_| "Error parsing markdown".to_string());

        Ok(html_body)
    }
}

/// Handles a markdown file that may have been created or modified.
/// Refreshes tracked files or adds new files in directory mode, sending reload notifications.
async fn handle_markdown_file_change(path: &Path, state: &SharedMarkdownState) {
    if !is_markdown_file(path) {
        return;
    }

    let mut state_guard = state.lock().await;
    let filename = relative_path_key(&state_guard.base_dir, path);

    // If file is already tracked, refresh its content
    if state_guard.tracked_files.contains_key(&filename) {
        if state_guard.refresh_file(&filename).is_ok() {
            let _ = state_guard.change_tx.send(ServerMessage::Reload);
        }
    } else if state_guard.is_directory_mode {
        // New file in directory mode - add and reload
        if state_guard.add_tracked_file(path.to_path_buf()).is_ok() {
            let _ = state_guard.change_tx.send(ServerMessage::Reload);
        }
    }
}

async fn handle_file_event(event: Event, state: &SharedMarkdownState) {
    let has_dir_path = event.paths.iter().any(|path| path.is_dir());
    let is_dir_event = matches!(
        event.kind,
        notify::EventKind::Create(notify::event::CreateKind::Folder)
            | notify::EventKind::Remove(notify::event::RemoveKind::Folder)
    ) || (matches!(
        event.kind,
        notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
    ) && has_dir_path);

    let is_create_or_remove = matches!(
        event.kind,
        notify::EventKind::Create(_) | notify::EventKind::Remove(_)
    );

    match event.kind {
        notify::EventKind::Modify(notify::event::ModifyKind::Name(rename_mode)) => {
            use notify::event::RenameMode;
            match rename_mode {
                RenameMode::Both => {
                    // Linux/Windows: Both old and new paths provided in single event
                    if event.paths.len() == 2 {
                        let new_path = &event.paths[1];
                        handle_markdown_file_change(new_path, state).await;
                    }
                }
                RenameMode::From => {
                    // File being renamed away - ignore
                }
                RenameMode::To => {
                    // File renamed to this location
                    if let Some(path) = event.paths.first() {
                        handle_markdown_file_change(path, state).await;
                    }
                }
                RenameMode::Any => {
                    // macOS: Sends separate events for old and new paths
                    // Use file existence to distinguish old (doesn't exist) from new (exists)
                    if let Some(path) = event.paths.first() {
                        if path.exists() {
                            handle_markdown_file_change(path, state).await;
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {
            for path in &event.paths {
                if is_markdown_file(path) {
                    match event.kind {
                        notify::EventKind::Create(_)
                        | notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) => {
                            handle_markdown_file_change(path, state).await;
                        }
                        notify::EventKind::Remove(_) => {
                            // Don't remove files from tracking. Editors like neovim save by
                            // renaming the file to a backup, then creating a new one. If we
                            // removed the file here, HTTP requests during that window would
                            // see empty tracked_files and return 404.
                        }
                        _ => {}
                    }
                } else if path.is_file() && is_image_file(path.to_str().unwrap_or("")) {
                    match event.kind {
                        notify::EventKind::Modify(_)
                        | notify::EventKind::Create(_)
                        | notify::EventKind::Remove(_) => {
                            let state_guard = state.lock().await;
                            let _ = state_guard.change_tx.send(ServerMessage::Reload);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    if is_dir_event {
        let mut state_guard = state.lock().await;
        if state_guard.is_directory_mode && state_guard.rescan_tracked_files().is_ok() {
            let _ = state_guard.change_tx.send(ServerMessage::Reload);
        }
        return;
    }

    if is_create_or_remove {
        let state_guard = state.lock().await;
        let _ = state_guard.change_tx.send(ServerMessage::Reload);
    }
}

/// Creates a new Router for serving markdown files.
///
/// # Errors
///
/// Returns an error if:
/// - Files cannot be read or don't exist
/// - File metadata cannot be accessed
/// - File watcher cannot be created
/// - File watcher cannot watch the base directory
pub fn new_router(
    base_dir: PathBuf,
    tracked_files: Vec<PathBuf>,
    is_directory_mode: bool,
) -> Result<Router> {
    let base_dir = base_dir.canonicalize()?;

    let state = Arc::new(Mutex::new(MarkdownState::new(
        base_dir.clone(),
        tracked_files,
        is_directory_mode,
    )?));

    let watcher_state = state.clone();
    let (tx, mut rx) = mpsc::channel(100);

    let mut watcher = RecommendedWatcher::new(
        move |res: std::result::Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        },
        Config::default(),
    )?;

    watcher.watch(&base_dir, RecursiveMode::NonRecursive)?;

    tokio::spawn(async move {
        let _watcher = watcher;
        while let Some(event) = rx.recv().await {
            handle_file_event(event, &watcher_state).await;
        }
    });

    let router = Router::new()
        .route("/", get(serve_html_root))
        .route("/ws", get(websocket_handler))
        .route("/mermaid.min.js", get(serve_mermaid_js))
        .route("/:filename", get(serve_file))
        .layer(CorsLayer::permissive())
        .with_state(state);

    Ok(router)
}

/// Serves markdown files with live reload support.
///
/// # Errors
///
/// Returns an error if:
/// - Files cannot be read or don't exist
/// - Cannot bind to the specified host address
/// - Server fails to start
/// - Axum serve encounters an error
pub async fn serve_markdown(
    base_dir: PathBuf,
    tracked_files: Vec<PathBuf>,
    is_directory_mode: bool,
    hostname: impl AsRef<str>,
    port: u16,
) -> Result<()> {
    let hostname = hostname.as_ref();

    let first_file = tracked_files.first().cloned();
    let router = new_router(base_dir.clone(), tracked_files, is_directory_mode)?;

    let listener = TcpListener::bind((hostname, port)).await?;

    let listen_addr = format_host(hostname, port);

    if is_directory_mode {
        println!("📁 Serving markdown files from: {}", base_dir.display());
    } else if let Some(file_path) = first_file {
        println!("📄 Serving markdown file: {}", file_path.display());
    }

    println!("🌐 Server running at: http://{listen_addr}");
    println!("⚡ Live reload enabled");
    println!("\nPress Ctrl+C to stop the server");

    axum::serve(listener, router).await?;

    Ok(())
}

/// Format the host address (hostname + port) for printing.
fn format_host(hostname: &str, port: u16) -> String {
    if hostname.parse::<Ipv6Addr>().is_ok() {
        format!("[{hostname}]:{port}")
    } else {
        format!("{hostname}:{port}")
    }
}

async fn serve_html_root(
    State(state): State<SharedMarkdownState>,
    Query(query): Query<FileQuery>,
) -> impl IntoResponse {
    let mut state = state.lock().await;

    let filename = if let Some(requested) = query.file {
        if state.tracked_files.contains_key(&requested) {
            requested
        } else {
            return (
                StatusCode::NOT_FOUND,
                Html("File not found".to_string()),
            );
        }
    } else {
        match state.get_sorted_filenames().into_iter().next() {
            Some(name) => name,
            None => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html("No files available to serve".to_string()),
                );
            }
        }
    };

    let _ = state.refresh_file(&filename);

    render_markdown(&state, &filename).await
}

async fn serve_file(
    AxumPath(filename): AxumPath<String>,
    State(state): State<SharedMarkdownState>,
) -> axum::response::Response {
    if filename.ends_with(".md") || filename.ends_with(".markdown") {
        let mut state = state.lock().await;

        if !state.tracked_files.contains_key(&filename) {
            return (StatusCode::NOT_FOUND, Html("File not found".to_string())).into_response();
        }

        let _ = state.refresh_file(&filename);

        let (status, html) = render_markdown(&state, &filename).await;
        (status, html).into_response()
    } else if is_image_file(&filename) {
        serve_static_file_inner(filename, state).await
    } else {
        (StatusCode::NOT_FOUND, Html("File not found".to_string())).into_response()
    }
}

async fn render_markdown(state: &MarkdownState, current_file: &str) -> (StatusCode, Html<String>) {
    let env = template_env();
    let template = match env.get_template(TEMPLATE_NAME) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(format!("Template error: {e}")),
            );
        }
    };

    let (content, has_mermaid) = if let Some(tracked) = state.tracked_files.get(current_file) {
        let html = &tracked.html;
        let mermaid = html.contains(r#"class="language-mermaid""#);
        (Value::from_safe_string(html.clone()), mermaid)
    } else {
        return (StatusCode::NOT_FOUND, Html("File not found".to_string()));
    };

    let rendered = if state.show_navigation() {
        let files = state.build_navigation_tree(current_file);

        match template.render(context! {
            content => content,
            mermaid_enabled => has_mermaid,
            show_navigation => true,
            files => files,
            current_file => current_file,
        }) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html(format!("Rendering error: {e}")),
                );
            }
        }
    } else {
        match template.render(context! {
            content => content,
            mermaid_enabled => has_mermaid,
            show_navigation => false,
        }) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html(format!("Rendering error: {e}")),
                );
            }
        }
    };

    (StatusCode::OK, Html(rendered))
}

async fn serve_mermaid_js(headers: HeaderMap) -> impl IntoResponse {
    if is_etag_match(&headers) {
        return mermaid_response(StatusCode::NOT_MODIFIED, None);
    }

    mermaid_response(StatusCode::OK, Some(MERMAID_JS))
}

fn is_etag_match(headers: &HeaderMap) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|etags| etags.split(',').any(|tag| tag.trim() == MERMAID_ETAG))
}

fn mermaid_response(status: StatusCode, body: Option<&'static str>) -> impl IntoResponse {
    // Use no-cache to force revalidation on each request. This ensures clients
    // get updated content when mdserve is rebuilt with a new Mermaid version,
    // while still benefiting from 304 responses via ETag matching.
    let headers = [
        (header::CONTENT_TYPE, "application/javascript"),
        (header::ETAG, MERMAID_ETAG),
        (header::CACHE_CONTROL, "public, no-cache"),
    ];

    match body {
        Some(content) => (status, headers, content).into_response(),
        None => (status, headers).into_response(),
    }
}

async fn serve_static_file_inner(
    filename: String,
    state: SharedMarkdownState,
) -> axum::response::Response {
    let state = state.lock().await;

    let full_path = state.base_dir.join(&filename);

    match full_path.canonicalize() {
        Ok(canonical_path) => {
            if !canonical_path.starts_with(&state.base_dir) {
                return (
                    StatusCode::FORBIDDEN,
                    [(header::CONTENT_TYPE, "text/plain")],
                    "Access denied".to_string(),
                )
                    .into_response();
            }

            match fs::read(&canonical_path) {
                Ok(contents) => {
                    let content_type = guess_image_content_type(&filename);
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, content_type.as_str())],
                        contents,
                    )
                        .into_response()
                }
                Err(_) => (
                    StatusCode::NOT_FOUND,
                    [(header::CONTENT_TYPE, "text/plain")],
                    "File not found".to_string(),
                )
                    .into_response(),
            }
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain")],
            "File not found".to_string(),
        )
            .into_response(),
    }
}

fn is_image_file(file_path: &str) -> bool {
    let extension = std::path::Path::new(file_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("");

    matches!(
        extension.to_lowercase().as_str(),
        "png" | "jpg" | "jpeg" | "gif" | "svg" | "webp" | "bmp" | "ico"
    )
}

fn guess_image_content_type(file_path: &str) -> String {
    let extension = std::path::Path::new(file_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("");

    match extension.to_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "ico" => "image/x-icon",
        _ => "application/octet-stream",
    }
    .to_string()
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<SharedMarkdownState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

async fn handle_websocket(socket: WebSocket, state: SharedMarkdownState) {
    let (mut sender, mut receiver) = socket.split();

    let mut change_rx = {
        let state = state.lock().await;
        state.change_tx.subscribe()
    };

    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                        match client_msg {
                            ClientMessage::Ping | ClientMessage::RequestRefresh => {}
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                _ => {}
            }
        }
    });

    let send_task = tokio::spawn(async move {
        while let Ok(reload_msg) = change_rx.recv().await {
            if let Ok(json) = serde_json::to_string(&reload_msg) {
                if sender.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    });

    tokio::select! {
        _ = recv_task => {},
        _ = send_task => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_is_markdown_file() {
        assert!(is_markdown_file(Path::new("test.md")));
        assert!(is_markdown_file(Path::new("/path/to/file.md")));

        assert!(is_markdown_file(Path::new("test.markdown")));
        assert!(is_markdown_file(Path::new("/path/to/file.markdown")));

        assert!(is_markdown_file(Path::new("test.MD")));
        assert!(is_markdown_file(Path::new("test.Md")));
        assert!(is_markdown_file(Path::new("test.MARKDOWN")));
        assert!(is_markdown_file(Path::new("test.MarkDown")));

        assert!(!is_markdown_file(Path::new("test.txt")));
        assert!(!is_markdown_file(Path::new("test.rs")));
        assert!(!is_markdown_file(Path::new("test.html")));
        assert!(!is_markdown_file(Path::new("test")));
        assert!(!is_markdown_file(Path::new("README")));
    }

    #[test]
    fn test_is_image_file() {
        assert!(is_image_file("test.png"));
        assert!(is_image_file("test.jpg"));
        assert!(is_image_file("test.jpeg"));
        assert!(is_image_file("test.gif"));
        assert!(is_image_file("test.svg"));
        assert!(is_image_file("test.webp"));
        assert!(is_image_file("test.bmp"));
        assert!(is_image_file("test.ico"));

        assert!(is_image_file("test.PNG"));
        assert!(is_image_file("test.JPG"));
        assert!(is_image_file("test.JPEG"));

        assert!(is_image_file("/path/to/image.png"));
        assert!(is_image_file("./images/photo.jpg"));

        assert!(!is_image_file("test.txt"));
        assert!(!is_image_file("test.md"));
        assert!(!is_image_file("test.rs"));
        assert!(!is_image_file("test"));
    }

    #[test]
    fn test_guess_image_content_type() {
        assert_eq!(guess_image_content_type("test.png"), "image/png");
        assert_eq!(guess_image_content_type("test.jpg"), "image/jpeg");
        assert_eq!(guess_image_content_type("test.jpeg"), "image/jpeg");
        assert_eq!(guess_image_content_type("test.gif"), "image/gif");
        assert_eq!(guess_image_content_type("test.svg"), "image/svg+xml");
        assert_eq!(guess_image_content_type("test.webp"), "image/webp");
        assert_eq!(guess_image_content_type("test.bmp"), "image/bmp");
        assert_eq!(guess_image_content_type("test.ico"), "image/x-icon");

        assert_eq!(guess_image_content_type("test.PNG"), "image/png");
        assert_eq!(guess_image_content_type("test.JPG"), "image/jpeg");

        assert_eq!(
            guess_image_content_type("test.xyz"),
            "application/octet-stream"
        );
        assert_eq!(guess_image_content_type("test"), "application/octet-stream");
    }

    #[test]
    fn test_scan_markdown_files_empty_directory() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        let result = scan_markdown_files(temp_dir.path()).expect("Failed to scan");
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scan_markdown_files_with_markdown_files() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("test1.md"), "# Test 1").expect("Failed to write");
        fs::write(temp_dir.path().join("test2.markdown"), "# Test 2").expect("Failed to write");
        fs::write(temp_dir.path().join("test3.md"), "# Test 3").expect("Failed to write");

        fs::write(temp_dir.path().join("test.txt"), "text").expect("Failed to write");
        fs::write(temp_dir.path().join("README"), "readme").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path()).expect("Failed to scan");

        assert_eq!(result.len(), 3);

        let filenames: Vec<_> = result
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(filenames, vec!["test3.md", "test2.markdown", "test1.md"]);
    }

    #[test]
    fn test_scan_markdown_files_includes_subdirectories() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("root.md"), "# Root").expect("Failed to write");

        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir).expect("Failed to create subdir");
        fs::write(sub_dir.join("nested.md"), "# Nested").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path()).expect("Failed to scan");

        assert_eq!(result.len(), 2);

        let relative_paths: Vec<_> = result
            .iter()
            .map(|p| {
                p.strip_prefix(temp_dir.path())
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();

        assert_eq!(relative_paths, vec!["subdir/nested.md", "root.md"]);
    }

    #[test]
    fn test_scan_markdown_files_case_insensitive() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("test1.md"), "# Test 1").expect("Failed to write");
        fs::write(temp_dir.path().join("test2.MD"), "# Test 2").expect("Failed to write");
        fs::write(temp_dir.path().join("test3.Md"), "# Test 3").expect("Failed to write");
        fs::write(temp_dir.path().join("test4.MARKDOWN"), "# Test 4").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path()).expect("Failed to scan");

        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_format_host() {
        assert_eq!(format_host("127.0.0.1", 3000), "127.0.0.1:3000");
        assert_eq!(format_host("192.168.1.1", 8080), "192.168.1.1:8080");

        assert_eq!(format_host("localhost", 3000), "localhost:3000");
        assert_eq!(format_host("example.com", 80), "example.com:80");

        assert_eq!(format_host("::1", 3000), "[::1]:3000");
        assert_eq!(format_host("2001:db8::1", 8080), "[2001:db8::1]:8080");
    }
}
