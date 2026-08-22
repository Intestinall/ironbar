#[cfg(feature = "bindmode+hyprland")]
use super::{BindModeClient, BindModeUpdate};
#[cfg(feature = "keyboard+hyprland")]
use super::{KeyboardLayoutClient, KeyboardLayoutUpdate};
use super::{Visibility, Workspace};
use crate::channels::SyncSenderExt;
use crate::{arc_mut, lock, spawn_blocking};
use hyprland::Result;
use hyprland::ctl::switch_xkb_layout;
use hyprland::data::{Devices, Workspace as HWorkspace, Workspaces};
use hyprland::dispatch::{Dispatch, DispatchType, WorkspaceIdentifierWithSpecial};
use hyprland::event_listener::EventListener;
use hyprland::prelude::*;
#[cfg(feature = "workspaces+hyprland")]
use serde::Deserialize;
#[cfg(feature = "workspaces+hyprland")]
use std::io::{Read, Write};
#[cfg(feature = "workspaces+hyprland")]
use std::os::unix::net::UnixStream;
use hyprland::shared::{Address, HyprDataVec, WorkspaceType};
use std::collections::HashMap;
use tokio::sync::broadcast::{Receiver, Sender, channel};
use tracing::{debug, error, info, warn};
use std::sync::{Arc, Mutex};

#[cfg(feature = "workspaces")]
use super::WorkspaceUpdate;

#[derive(Debug)]
struct TxRx<T> {
    tx: Sender<T>,
    _rx: Receiver<T>,
}
impl<T: Clone> TxRx<T> {
    fn new() -> Self {
        let (tx, rx) = channel(16);
        Self { tx, _rx: rx }
    }
}

#[derive(Debug)]
pub struct Client {
    #[cfg(feature = "workspaces+hyprland")]
    workspace: TxRx<WorkspaceUpdate>,

    #[cfg(feature = "workspaces+hyprland")]
    window_cache: Arc<Mutex<WindowCache>>,

    #[cfg(feature = "workspaces+hyprland")]
    use_lua_dispatch: bool,

    #[cfg(feature = "keyboard+hyprland")]
    keyboard_layout: TxRx<KeyboardLayoutUpdate>,

    #[cfg(feature = "bindmode+hyprland")]
    bindmode: TxRx<BindModeUpdate>,
}

#[derive(Debug)]
struct WindowCacheRecord {
    workspace_id: i64,
    workspace_name: String,
    class: String
}

#[derive(Debug)]
struct WindowCache {
    inner: HashMap<Address, WindowCacheRecord>,
}


impl WindowCache {
    pub fn new() -> Self {
        let mut inner = HashMap::new();
        if let Ok(clients) = hyprland::data::Clients::get() {
            for client in clients {
                inner.insert(
                    client.address,
                    WindowCacheRecord {
                        workspace_id: client.workspace.id as i64,
                        workspace_name: client.workspace.name,
                        class: client.class
                    },
                );
            }
        }
        Self { inner }
    }

    pub fn insert(&mut self, address: Address, workspace_id: i64, workspace_name: String, class: String) {
        self.inner.insert(
            address,
            WindowCacheRecord { workspace_id, workspace_name, class },
        );
    }

    pub fn remove_entry(&mut self, address: &Address) -> Option<(Address, WindowCacheRecord)> {
        self.inner.remove_entry(address)
    }

    pub fn get_classes_for_workspace(&self, workspace_id: i64) -> Vec<String> {
        self.inner
            .values()
            .filter(|r| r.workspace_id == workspace_id)
            .map(|r| r.class.clone())
            .collect()
    }

    pub fn update_class(
        &mut self,
        address: &Address,
        new_class: String,
    ) -> Option<(i64, String, Vec<String>)> {
        let (ws_id, ws_name) = {
            let record = self.inner.get_mut(address)?;
            if record.class == new_class {
                return None;
            }
            record.class = new_class;
            (record.workspace_id, record.workspace_name.clone())
        };
        Some((ws_id, ws_name, self.get_classes_for_workspace(ws_id)))
    }
}


impl Client {
    pub(crate) fn new() -> Self {
        let instance = Self {
            #[cfg(feature = "workspaces+hyprland")]
            workspace: TxRx::new(),
            #[cfg(feature = "workspaces+hyprland")]
            window_cache: arc_mut!(WindowCache::new()),
            #[cfg(feature = "workspaces+hyprland")]
            use_lua_dispatch: detect_lua_config(),
            #[cfg(feature = "keyboard+hyprland")]
            keyboard_layout: TxRx::new(),
            #[cfg(feature = "bindmode+hyprland")]
            bindmode: TxRx::new(),
        };

        instance.listen_events();
        instance
    }

    fn listen_events(&self) {
        info!("Starting Hyprland event listener");

        #[cfg(feature = "workspaces+hyprland")]
        let workspace_tx = self.workspace.tx.clone();

        #[cfg(feature = "workspaces+hyprland")]
        let window_cache = self.window_cache.clone();

        #[cfg(feature = "keyboard+hyprland")]
        let keyboard_layout_tx = self.keyboard_layout.tx.clone();

        #[cfg(feature = "bindmode+hyprland")]
        let bindmode_tx = self.bindmode.tx.clone();

        spawn_blocking(move || {
            let mut event_listener = EventListener::new();

            // we need a lock to ensure events don't run at the same time
            let lock = arc_mut!(());

            // cache the active workspace since Hyprland doesn't give us the prev active
            #[cfg(feature = "workspaces+hyprland")]
            Self::listen_workspace_events(&workspace_tx, &mut event_listener, &window_cache, &lock);

            #[cfg(feature = "keyboard+hyprland")]
            Self::listen_keyboard_events(&keyboard_layout_tx, &mut event_listener, &lock);

            #[cfg(feature = "bindmode+hyprland")]
            Self::listen_bindmode_events(&bindmode_tx, &mut event_listener, &lock);

            if let Err(err) = event_listener.start_listener() {
                error!("Failed to start listener: {err:#}");
            }
        });
    }

    #[cfg(feature = "workspaces+hyprland")]
    fn listen_workspace_events(
        tx: &Sender<WorkspaceUpdate>,
        event_listener: &mut EventListener,
        window_cache: &Arc<Mutex<WindowCache>>,
        lock: &std::sync::Arc<std::sync::Mutex<()>>,
    ) {
        let active = Self::get_active_workspace().map_or_else(
            |err| {
                error!("Failed to get active workspace: {err:#?}");
                None
            },
            Some,
        );
        let active = arc_mut!(active);

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let active = active.clone();
            let window_cache = window_cache.clone();

            event_listener.add_workspace_added_handler(move |event| {
                let _lock = lock!(lock);
                debug!("Added workspace: {event:?}");
                let cache = lock!(window_cache);

                let workspace_name = get_workspace_name(event.name);
                let prev_workspace = lock!(active);

                let workspace = Self::get_workspace(&workspace_name, prev_workspace.as_ref(), &cache);

                match workspace {
                    Ok(Some(workspace)) => {
                        tx.send_expect(WorkspaceUpdate::Add(workspace));
                    }
                    Err(e) => error!("Failed to get workspace: {e:#}"),
                    _ => {}
                }
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let active = active.clone();
            let window_cache = window_cache.clone();

            event_listener.add_workspace_changed_handler(move |event| {
                let _lock = lock!(lock);
                let cache = lock!(window_cache);

                let mut prev_workspace = lock!(active);

                debug!(
                    "Received workspace change: {:?} -> {event:?}",
                    prev_workspace.as_ref().map(|w| &w.id)
                );

                let workspace_name = get_workspace_name(event.name);
                let workspace = Self::get_workspace(&workspace_name, prev_workspace.as_ref(), &cache);

                match workspace {
                    Ok(Some(workspace)) if !workspace.visibility.is_focused() => {
                        Self::send_focus_change(&mut prev_workspace, workspace, &tx);
                    }
                    Ok(None) => {
                        error!("Unable to locate workspace");
                    }
                    Err(e) => error!("Failed to get workspace: {e:#}"),
                    _ => {}
                }
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let active = active.clone();
            let window_cache = window_cache.clone();

            event_listener.add_active_monitor_changed_handler(move |event_data| {
                let _lock = lock!(lock);
                let Some(workspace_type) = event_data.workspace_name else {
                    warn!("Received active monitor change with no workspace name");
                    return;
                };
                let cache = lock!(window_cache);

                let mut prev_workspace = lock!(active);

                debug!(
                    "Received active monitor change: {:?} -> {workspace_type:?}",
                    prev_workspace.as_ref().map(|w| &w.name)
                );

                let workspace_name = get_workspace_name(workspace_type);
                let workspace = Self::get_workspace(&workspace_name, prev_workspace.as_ref(), &cache);

                match workspace {
                    Ok(Some(workspace)) if !workspace.visibility.is_focused() => {
                        Self::send_focus_change(&mut prev_workspace, workspace, &tx);
                    }
                    Ok(None) => {
                        error!("Unable to locate workspace");
                    }
                    Err(e) => error!("Failed to get workspace: {e:#}"),
                    _ => {}
                }
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let active = active.clone();
            let window_cache = window_cache.clone();

            event_listener.add_workspace_moved_handler(move |event_data| {
                let _lock = lock!(lock);
                let workspace_type = event_data.name;
                let cache = lock!(window_cache);

                let mut prev_workspace = lock!(active);
                debug!(
                    "Received workspace move: {:?} -> {workspace_type:?}",
                    prev_workspace.as_ref().map(|w| &w.name)
                );

                let workspace_name = get_workspace_name(workspace_type);
                let workspace = Self::get_workspace(&workspace_name, prev_workspace.as_ref(), &cache);

                match workspace {
                    Ok(Some(workspace)) => {
                        tx.send_expect(WorkspaceUpdate::Move(workspace.clone()));
                        if !workspace.visibility.is_focused() {
                            Self::send_focus_change(&mut prev_workspace, workspace, &tx);
                        }
                    }
                    Ok(None) => {
                        error!("Unable to locate workspace");
                    }
                    Err(e) => error!("Failed to get workspace: {e:#}"),
                }
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let window_cache = window_cache.clone();

            event_listener.add_workspace_renamed_handler(move |data| {
                let _lock = lock!(lock);
                debug!("Received workspace rename: {data:?}");
                let cache = lock!(window_cache);

                tx.send_expect(WorkspaceUpdate::Rename {
                    id: data.id as i64,
                    name: data.name,
                    classes: Some(cache.get_classes_for_workspace(data.id as i64))
                });
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();

            event_listener.add_workspace_deleted_handler(move |data| {
                let _lock = lock!(lock);
                debug!("Received workspace destroy: {data:?}");
                tx.send_expect(WorkspaceUpdate::Remove(data.id as i64));
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();

            event_listener.add_urgent_state_changed_handler(move |address| {
                let _lock = lock!(lock);
                debug!("Received urgent state: {address:?}");

                let clients = match hyprland::data::Clients::get() {
                    Ok(clients) => clients,
                    Err(err) => {
                        error!("Failed to get clients: {err}");
                        return;
                    }
                };
                clients.iter().find(|c| c.address == address).map_or_else(
                    || {
                        error!("Unable to locate client");
                    },
                    |c| {
                        tx.send_expect(WorkspaceUpdate::Urgent {
                            id: c.workspace.id as i64,
                            urgent: true,
                        });
                    },
                );
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let window_cache = window_cache.clone();

            event_listener.add_window_opened_handler(move |window_opened_event| {
                let _lock = lock!(lock);
                let mut cache = lock!(window_cache);

                // TODO: Debug is bad
                debug!("Received window opened: {window_opened_event:?}");
                let workspace = Self::get_workspace(&window_opened_event.workspace_name, None, &cache);

                match workspace {
                    Ok(Some(workspace)) => {
                        cache.insert(
                            window_opened_event.window_address,
                            workspace.id, window_opened_event.workspace_name, window_opened_event.window_class,
                        );
                        tx.send_expect(WorkspaceUpdate::AddWindow {
                            id: workspace.id,
                            name: workspace.name,
                            classes: Some(cache.get_classes_for_workspace(workspace.id))
                        });
                    }
                    Ok(None) => {
                        error!("Unable to locate workspace");
                    }
                    Err(e) => error!("Failed to get workspace: {e:#}"),
                }
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let window_cache = window_cache.clone();

            event_listener.add_window_closed_handler(move |window_closed_address| {
                let _lock = lock!(lock);
                let mut cache = lock!(window_cache);

                match cache.remove_entry(&window_closed_address) {
                    Some((_window_address, record)) => {
                        debug!("Window closed with address {window_closed_address}");
                        tx.send_expect(WorkspaceUpdate::RemoveWindow {
                            id: record.workspace_id,
                            name: record.workspace_name,
                            classes: Some(cache.get_classes_for_workspace(record.workspace_id))
                        });
                    }
                    None => {
                        error!("Window closed with address {window_closed_address} but not found in the cache");
                    }
                };
            });
        }

        {
            let tx = tx.clone();
            let lock = lock.clone();
            let window_cache = window_cache.clone();

            event_listener.add_window_moved_handler(move |window_moved_event| {
                let _lock = lock!(lock);
                let workspace_type = window_moved_event.workspace_name;
                let mut cache = lock!(window_cache);

                let prev_workspace = lock!(active);

                let workspace_name = get_workspace_name(workspace_type);
                let new_workspace = Self::get_workspace(&workspace_name, prev_workspace.as_ref(), &cache);

                match new_workspace {
                    Ok(Some(new_workspace)) => {
                        match cache.remove_entry(&window_moved_event.window_address) {
                            Some((window_address, old_record)) => {
                                debug!(
                                    "Window moved from {} to {}",
                                    &old_record.workspace_name, &new_workspace.name
                                );
                                cache.insert(window_address, new_workspace.id, new_workspace.name.clone(), old_record.class);
                                tx.send_expect(WorkspaceUpdate::RemoveWindow {
                                    id: old_record.workspace_id,
                                    name: old_record.workspace_name,
                                    classes: Some(cache.get_classes_for_workspace(old_record.workspace_id))
                                });
                                tx.send_expect(WorkspaceUpdate::AddWindow {
                                    id: new_workspace.id,
                                    name: new_workspace.name,
                                    classes: Some(cache.get_classes_for_workspace(new_workspace.id))
                                });

                            }
                            None => {
                                error!(
                                    "Window moved to {}, but old workspace was unknown",
                                    new_workspace.name
                                );
                            }
                        };
                    }
                    Ok(None) => {
                        error!("Unable to locate workspace");
                    }
                    Err(e) => error!("Failed to get workspace: {e:#}"),
                }
            });
        }
        // Handles dynamic class changes
        {
            let tx = tx.clone();
            let lock = lock.clone();
            let window_cache = window_cache.clone();

            event_listener.add_active_window_changed_handler(move |event_data| {
                let _lock = lock!(lock);

                let Some(event_data) = event_data else {
                    return;
                };

                // Skip if class is still empty
                if event_data.class.is_empty() {
                    return;
                }

                let mut cache = lock!(window_cache);
                if let Some((ws_id, ws_name, classes)) = cache.update_class(&event_data.address, event_data.class) {
                    tx.send_expect(WorkspaceUpdate::AddWindow {
                        id: ws_id,
                        name: ws_name,
                        classes: Some(classes),
                    });
                }
            });
        }
    }

    #[cfg(feature = "keyboard+hyprland")]
    fn listen_keyboard_events(
        keyboard_layout_tx: &Sender<KeyboardLayoutUpdate>,
        event_listener: &mut EventListener,
        lock: &std::sync::Arc<std::sync::Mutex<()>>,
    ) {
        let tx = keyboard_layout_tx.clone();
        let lock = lock.clone();

        event_listener.add_layout_changed_handler(move |layout_event| {
            let _lock = lock!(lock);

            let layout = if layout_event.layout_name.is_empty() {
                // FIXME: This field is empty due to bug in `hyprland-rs_0.4.0-alpha.3`. Which is already fixed in last betas

                // The layout may be empty due to a bug in `hyprland-rs`, because of which the `layout_event` is incorrect.
                //
                // Instead of:
                // ```
                // LayoutEvent {
                //     keyboard_name: "keychron-keychron-c2",
                //     layout_name: "English (US)",
                // }
                // ```
                //
                // We get:
                // ```
                // LayoutEvent {
                //     keyboard_name: "keychron-keychron-c2,English (US)",
                //     layout_name: "",
                // }
                // ```
                // 
                // Here we are trying to recover `layout_name` from `keyboard_name`

                let layout = layout_event.keyboard_name.as_str().split(',').nth(1);
                let Some(layout) = layout else {
                    error!(
                        "Failed to get layout from string: {}. The failed logic is a workaround for a bug in `hyprland 0.4.0-alpha.3`", layout_event.keyboard_name);
                    return;
                };

                layout.into()
            }
            else {
                layout_event.layout_name
            };

            debug!("Received layout: {layout:?}");
            tx.send_expect(KeyboardLayoutUpdate(layout));
        });
    }

    #[cfg(feature = "bindmode+hyprland")]
    fn listen_bindmode_events(
        bindmode_tx: &Sender<BindModeUpdate>,
        event_listener: &mut EventListener,
        lock: &std::sync::Arc<std::sync::Mutex<()>>,
    ) {
        let tx = bindmode_tx.clone();
        let lock = lock.clone();

        event_listener.add_sub_map_changed_handler(move |bind_mode| {
            let _lock = lock!(lock);
            debug!("Received bind mode: {bind_mode:?}");

            tx.send_expect(BindModeUpdate {
                name: bind_mode,
                pango_markup: false,
            });
        });
    }

    /// Sends a `WorkspaceUpdate::Focus` event
    /// and updates the active workspace cache.
    #[cfg(feature = "workspaces+hyprland")]
    fn send_focus_change(
        prev_workspace: &mut Option<Workspace>,
        workspace: Workspace,
        tx: &Sender<WorkspaceUpdate>,
    ) {
        tx.send_expect(WorkspaceUpdate::Focus {
            old: prev_workspace.take(),
            new: workspace.clone(),
        });

        tx.send_expect(WorkspaceUpdate::Urgent {
            id: workspace.id,
            urgent: false,
        });

        prev_workspace.replace(workspace);
    }

    /// Gets a workspace by name from the server, given the active workspace if known.
    #[cfg(feature = "workspaces+hyprland")]
    fn get_workspace(name: &str, active: Option<&Workspace>, window_cache: &WindowCache) -> Result<Option<Workspace>> {
        let workspace = Workspaces::get()?.into_iter().find_map(|w| {
            if w.name == name {
                let vis = Visibility::from((&w, active.map(|w| w.name.as_ref()), &|w| {
                    create_is_visible()(w)
                }));
                let mut ws = Workspace::from((vis, w));
                ws.classes = window_cache.get_classes_for_workspace(ws.id);
                Some(ws)
            } else {
                None
            }
        });

        Ok(workspace)
    }

    /// Gets the active workspace from the server.
    fn get_active_workspace() -> Result<Workspace> {
        let w = HWorkspace::get_active().map(|w| Workspace::from((Visibility::focused(), w)))?;
        Ok(w)
    }
}

#[cfg(feature = "workspaces+hyprland")]
impl super::WorkspaceClient for Client {
    fn focus(&self, id: i64) {
        let res = if self.use_lua_dispatch {
            let arg = format!("{{workspace=\"{id}\"}}");
            Dispatch::call(DispatchType::Custom("hl.dsp.focus", &arg))
        } else {
            let identifier = WorkspaceIdentifierWithSpecial::Id(id as i32);
            Dispatch::call(DispatchType::Workspace(identifier))
        };

        if let Err(e) = res {
            error!("Couldn't focus workspace '{id}': {e:#}");
        }
    }

    fn subscribe(&self) -> Receiver<WorkspaceUpdate> {
        let rx = self.workspace.tx.subscribe();

        let active_id = HWorkspace::get_active().ok().map(|active| active.name);
        let is_visible = create_is_visible();

        match Workspaces::get() {
            Ok(workspaces) => {
                let cache = lock!(self.window_cache);

                let workspaces = workspaces
                    .into_iter()
                    .map(|w| {
                        let vis = Visibility::from((&w, active_id.as_deref(), &is_visible));
                        let mut ws = Workspace::from((vis, w));
                        ws.classes = cache.get_classes_for_workspace(ws.id);
                        ws
                    })
                    .collect();

                self.workspace
                    .tx
                    .send_expect(WorkspaceUpdate::Init(workspaces));
            }
            Err(e) => {
                error!("Failed to get workspaces: {e:#}");
            }
        }

        rx
    }
}

#[cfg(feature = "keyboard+hyprland")]
impl KeyboardLayoutClient for Client {
    fn set_next_active(&self) {
        let Ok(devices) = Devices::get() else {
            error!("Failed to get devices");
            return;
        };

        let device = devices
            .keyboards
            .iter()
            .find(|k| k.main)
            .map(|k| k.name.clone());

        if let Some(device) = device {
            if let Err(e) =
                switch_xkb_layout::call(device, switch_xkb_layout::SwitchXKBLayoutCmdTypes::Next)
            {
                error!("Failed to switch keyboard layout due to Hyprland error: {e}");
            }
        } else {
            error!("Failed to get keyboard device from hyprland");
        }
    }

    fn subscribe(&self) -> Receiver<KeyboardLayoutUpdate> {
        let rx = self.keyboard_layout.tx.subscribe();

        match Devices::get().map(|devices| {
            devices
                .keyboards
                .iter()
                .find(|k| k.main)
                .map(|k| k.active_keymap.clone())
        }) {
            Ok(Some(layout)) => {
                self.keyboard_layout
                    .tx
                    .send_expect(KeyboardLayoutUpdate(layout));
            }
            Ok(None) => error!("Failed to get current keyboard layout hyprland"),
            Err(err) => error!("Failed to get devices: {err:#?}"),
        }

        rx
    }
}

#[cfg(feature = "bindmode+hyprland")]
impl BindModeClient for Client {
    fn subscribe(&self) -> super::Result<Receiver<BindModeUpdate>> {
        Ok(self.bindmode.tx.subscribe())
    }
}

#[cfg(feature = "workspaces+hyprland")]
fn detect_lua_config() -> bool {
    match get_hyprland_config_provider() {
        Ok(provider) => provider == "lua",
        Err(err) => {
            warn!("Failed to detect Hyprland config provider, assuming legacy: {err}");
            false
        }
    }
}

#[cfg(feature = "workspaces+hyprland")]
#[derive(Deserialize)]
struct HyprlandStatus {
    #[serde(rename = "configProvider")]
    config_provider: String,
}

#[cfg(feature = "workspaces+hyprland")]
fn get_hyprland_config_provider() -> std::result::Result<String, Box<dyn std::error::Error>> {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .or_else(|_| std::env::var("UID").map(|uid| format!("/run/user/{uid}")))?;
    let instance = std::env::var("HYPRLAND_INSTANCE_SIGNATURE")?;
    let socket_path = format!("{runtime_dir}/hypr/{instance}/.socket.sock");

    let mut stream = UnixStream::connect(socket_path)?;
    stream.write_all(b"j/status")?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    Ok(serde_json::from_str::<HyprlandStatus>(&response)?.config_provider)
}

fn get_workspace_name(name: WorkspaceType) -> String {
    match name {
        WorkspaceType::Regular(name) => name,
        WorkspaceType::Special(name) => name.unwrap_or_default(),
    }
}

/// Creates a function which determines if a workspace is visible.
///
/// This function makes a Hyprland call that allocates so it should be cached when possible,
/// but it is only valid so long as workspaces do not change so it should not be stored long term
fn create_is_visible() -> impl Fn(&HWorkspace) -> bool {
    let monitors = hyprland::data::Monitors::get().map_or(Vec::new(), HyprDataVec::to_vec);

    move |w| monitors.iter().any(|m| m.active_workspace.id == w.id)
}

impl From<(Visibility, HWorkspace)> for Workspace {
    fn from((visibility, workspace): (Visibility, HWorkspace)) -> Self {
        Self {
            id: workspace.id as i64,
            index: workspace.id as i64,
            name: workspace.name,
            monitor: workspace.monitor,
            visibility,
            classes: vec![],
        }
    }
}

impl<'a, 'f, F> From<(&'a HWorkspace, Option<&str>, F)> for Visibility
where
    F: FnOnce(&'f HWorkspace) -> bool,
    'a: 'f,
{
    fn from((workspace, active_name, is_visible): (&'a HWorkspace, Option<&str>, F)) -> Self {
        if Some(workspace.name.as_str()) == active_name {
            Self::focused()
        } else if is_visible(workspace) {
            Self::visible()
        } else {
            Self::Hidden
        }
    }
}
