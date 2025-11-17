use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use auth::{
    authenticator::{Authenticator, MsaAuthorizationError, XboxAuthenticateError},
    credentials::{AUTH_STAGE_COUNT, AccountCredentials},
    models::{MinecraftAccessToken, MinecraftProfileResponse, SkinState},
    secret::PlatformSecretStorage,
    serve_redirect::{self, ProcessAuthorizationError},
};
use bridge::{
    handle::{BackendHandle, BackendReceiver, FrontendHandle},
    instance::InstanceID,
    message::{MessageToBackend, MessageToFrontend},
    modal_action::{ModalAction, ModalActionVisitUrl, ProgressTracker},
};
use image::imageops::FilterType;
use reqwest::{StatusCode, redirect::Policy};
use slab::Slab;
use tokio::sync::{OnceCell, mpsc::Receiver};

use crate::{
    account::BackendAccountInfo,
    directories::LauncherDirectories,
    instance::Instance,
    launch::Launcher,
    metadata::{items::MinecraftVersionManifestMetadataItem, manager::MetadataManager},
    mod_metadata::ModMetadataManager,
};

pub fn start(send: FrontendHandle, self_handle: BackendHandle, recv: BackendReceiver) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to initialize Tokio runtime");

    let http_client = reqwest::ClientBuilder::new()
        // .connect_timeout(Duration::from_secs(5))
        .redirect(Policy::none())
        .use_rustls_tls()
        .user_agent("PandoraLauncher/0.1.0 (https://github.com/Moulberry/PandoraLauncher)")
        .build()
        .unwrap();

    let base_dirs = directories::BaseDirs::new().unwrap();
    let data_dir = base_dirs.data_dir();
    let launcher_dir = data_dir.join("PandoraLauncher");
    let directories = Arc::new(LauncherDirectories::new(launcher_dir));

    let meta = Arc::new(MetadataManager::new(
        http_client.clone(),
        directories.metadata_dir.clone(),
    ));

    let (watcher_tx, watcher_rx) = tokio::sync::mpsc::channel::<notify_debouncer_full::DebounceEventResult>(64);
    let watcher = notify_debouncer_full::new_debouncer(Duration::from_millis(100), None, move |event| {
        let _ = watcher_tx.blocking_send(event);
    }).unwrap();

    let mut account_info = load_accounts_json(&directories);
    for account in account_info.accounts.values_mut() {
        account.try_load_head_32x_from_head();
    }

    let mod_metadata_manager = ModMetadataManager::load(directories.content_meta_dir.clone());

    let state = BackendState {
        self_handle,
        send: send.clone(),
        watcher,
        watcher_rx,
        http_client,
        meta: Arc::clone(&meta),
        watching: HashMap::new(),
        instances: Slab::new(),
        instance_by_path: HashMap::new(),
        instances_generation: 0,
        directories: Arc::clone(&directories),
        launcher: Launcher::new(meta, directories, send),
        mod_metadata_manager: Arc::new(mod_metadata_manager),
        reload_mods_immediately: HashSet::new(),
        account_info,
        secret_storage: OnceCell::new(),
    };

    runtime.spawn(state.start(recv));

    std::mem::forget(runtime);
}

#[derive(Clone, Copy)]
pub enum WatchTarget {
    InstancesDir,
    InvalidInstanceDir,
    InstanceDir { id: InstanceID },
    InstanceDotMinecraftDir { id: InstanceID },
    InstanceWorldDir { id: InstanceID },
    InstanceSavesDir { id: InstanceID },
    ServersDat { id: InstanceID },
    InstanceModsDir { id: InstanceID },
}

pub struct BackendState {
    pub self_handle: BackendHandle,
    pub send: FrontendHandle,
    pub watcher: notify_debouncer_full::Debouncer<notify::RecommendedWatcher, notify_debouncer_full::RecommendedCache>,
    pub watcher_rx: Receiver<notify_debouncer_full::DebounceEventResult>,
    pub http_client: reqwest::Client,
    pub meta: Arc<MetadataManager>,
    pub watching: HashMap<Arc<Path>, WatchTarget>,
    pub instances: Slab<Instance>,
    pub instance_by_path: HashMap<PathBuf, InstanceID>,
    pub instances_generation: usize,
    pub directories: Arc<LauncherDirectories>,
    pub launcher: Launcher,
    pub mod_metadata_manager: Arc<ModMetadataManager>,
    pub reload_mods_immediately: HashSet<InstanceID>,
    pub account_info: BackendAccountInfo,
    pub secret_storage: OnceCell<PlatformSecretStorage>,
}

impl BackendState {
    async fn start(mut self, recv: BackendReceiver) {
        // Pre-fetch version manifest
        self.meta.load(&MinecraftVersionManifestMetadataItem).await;

        self.send.send(self.account_info.create_update_message());

        let _ = std::fs::create_dir_all(&self.directories.instances_dir);

        self.watch_filesystem(&self.directories.instances_dir.clone(), WatchTarget::InstancesDir).await;

        let mut paths_with_time = Vec::new();

        for entry in std::fs::read_dir(&self.directories.instances_dir).unwrap() {
            let Ok(entry) = entry else {
                eprintln!("Error reading directory in instances folder: {:?}", entry.unwrap_err());
                continue;
            };

            let path = entry.path();

            let mut time = SystemTime::UNIX_EPOCH;
            if let Ok(metadata) = path.metadata() {
                if let Ok(created) = metadata.created() {
                    time = time.max(created);
                }
                if let Ok(created) = metadata.modified() {
                    time = time.max(created);
                }
            }

            // options.txt exists in every minecraft version, so we use its
            // modified time to determine the latest instance as well
            let mut options_txt = path.join(".minecraft");
            options_txt.push("options.txt");
            if let Ok(metadata) = options_txt.metadata() {
                if let Ok(created) = metadata.created() {
                    time = time.max(created);
                }
                if let Ok(created) = metadata.modified() {
                    time = time.max(created);
                }
            }

            paths_with_time.push((path, time));
        }

        paths_with_time.sort_by_key(|(_, time)| *time);
        for (path, _) in paths_with_time {
            let success = self.load_instance_from_path(&path, true, false).await;
            if !success {
                self.watch_filesystem(&path, WatchTarget::InvalidInstanceDir).await;
            }
        }

        self.handle(recv).await;
    }

    pub async fn watch_filesystem(&mut self, path: &Path, target: WatchTarget) {
        if self.watcher.watch(path, notify::RecursiveMode::NonRecursive).is_err() {
            self.send.send_error(format!("Unable to watch directory {:?}, launcher may be out of sync with files!", path));
            return;
        }
        self.watching.insert(path.into(), target);
    }

    pub async fn remove_instance(&mut self, id: InstanceID) {
        if let Some(instance) = self.instances.get(id.index)
            && instance.id == id
        {
            let instance = self.instances.remove(id.index);
            self.send.send(MessageToFrontend::InstanceRemoved { id });
            self.send.send_info(format!("Instance '{}' removed", instance.name));
        }
    }

    pub async fn load_instance_from_path(&mut self, path: &Path, mut show_errors: bool, show_success: bool) -> bool {
        let instance = Instance::load_from_folder(&path).await;
        let Ok(mut instance) = instance else {
            if let Some(existing) = self.instance_by_path.get(path)
                && let Some(existing_instance) = self.instances.get(existing.index)
                && existing_instance.id == *existing
            {
                let instance = self.instances.remove(existing.index);
                self.send.send(MessageToFrontend::InstanceRemoved { id: instance.id});
                show_errors = true;
            }

            if show_errors {
                let error = instance.unwrap_err();
                self.send.send_error(format!("Unable to load instance from {:?}:\n{}", &path, &error));
                eprintln!("Error loading instance: {:?}", &error);
            }

            return false;
        };

        if let Some(existing) = self.instance_by_path.get(path)
            && let Some(existing_instance) = self.instances.get_mut(existing.index)
            && existing_instance.id == *existing
        {
            existing_instance.copy_basic_attributes_from(instance);

            let _ = self.send.send(existing_instance.create_modify_message());

            if show_success {
                self.send.send_info(format!("Instance '{}' updated", existing_instance.name));
            }

            return true;
        }

        let vacant = self.instances.vacant_entry();
        let instance_id = InstanceID {
            index: vacant.key(),
            generation: self.instances_generation,
        };
        self.instances_generation = self.instances_generation.wrapping_add(1);

        if show_success {
            self.send.send_success(format!("Instance '{}' created", instance.name));
        }
        let message = MessageToFrontend::InstanceAdded {
            id: instance_id,
            name: instance.name,
            version: instance.version,
            loader: instance.loader,
            worlds_state: Arc::clone(&instance.worlds_state),
            servers_state: Arc::clone(&instance.servers_state),
            mods_state: Arc::clone(&instance.mods_state),
        };
        instance.id = instance_id;
        vacant.insert(instance);
        self.send.send(message);

        self.instance_by_path.insert(path.to_owned(), instance_id);

        self.watch_filesystem(path, WatchTarget::InstanceDir { id: instance_id }).await;
        true
    }

    async fn handle(mut self, mut backend_recv: BackendReceiver) {
        let mut interval = tokio::time::interval(Duration::from_millis(1000));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tokio::pin!(interval);

        loop {
            tokio::select! {
                message = backend_recv.recv() => {
                    if let Some(message) = message {
                        self.handle_message(message).await;
                    } else {
                        eprintln!("Backend receiver has shut down");
                        break;
                    }
                },
                instance_change = self.watcher_rx.recv() => {
                    if let Some(instance_change) = instance_change {
                        self.handle_filesystem(instance_change).await;
                    } else {
                        eprintln!("Backend filesystem has shut down");
                        break;
                    }
                },
                _ = interval.tick() => {
                    self.handle_tick().await;
                }
            }
        }
    }

    async fn handle_tick(&mut self) {
        self.meta.expire().await;

        for (_, instance) in &mut self.instances {
            if let Some(child) = &mut instance.child
                && !matches!(child.try_wait(), Ok(None))
            {
                instance.child = None;
                self.send.send(instance.create_modify_message());
            }
        }
    }

    pub async fn login(
        &mut self,
        credentials: &mut AccountCredentials,
        login_tracker: &ProgressTracker,
        modal_action: &ModalAction,
    ) -> Result<(MinecraftProfileResponse, MinecraftAccessToken), LoginError> {
        let mut authenticator = Authenticator::new(self.http_client.clone());

        login_tracker.set_total(AUTH_STAGE_COUNT as usize + 1);
        login_tracker.notify();

        let mut last_auth_stage = None;
        let mut allow_backwards = true;
        loop {
            if modal_action.has_requested_cancel() {
                return Err(LoginError::CancelledByUser);
            }

            let stage_with_data = credentials.stage();
            let stage = stage_with_data.stage();

            login_tracker.set_count(stage as usize + 1);
            login_tracker.notify();

            if let Some(last_stage) = last_auth_stage {
                if stage > last_stage {
                    allow_backwards = false;
                } else if stage < last_stage && !allow_backwards {
                    eprintln!(
                        "Stage {:?} went backwards from {:?} when going backwards isn't allowed. This is most likely a bug with the auth flow!",
                        stage, last_stage
                    );
                    return Err(LoginError::LoginStageErrorBackwards);
                } else if stage == last_stage {
                    eprintln!("Stage {:?} didn't change. This is most likely a bug with the auth flow!", stage);
                    return Err(LoginError::LoginStageErrorDidntChange);
                }
            }
            last_auth_stage = Some(stage);

            match credentials.stage() {
                auth::credentials::AuthStageWithData::Initial => {
                    let pending = authenticator.create_authorization();
                    modal_action.set_visit_url(ModalActionVisitUrl {
                        message: "Login with Microsoft".into(),
                        url: pending.url.as_str().into(),
                        prevent_auto_finish: false,
                    });
                    self.send.send(MessageToFrontend::Refresh);

                    let cancel = modal_action.request_cancel.clone();
                    let finished = tokio::task::spawn_blocking(move || {
                        serve_redirect::start_server(pending, cancel)
                    }).await.unwrap()?;

                    modal_action.unset_visit_url();
                    self.send.send(MessageToFrontend::Refresh);

                    let msa_tokens = authenticator.finish_authorization(finished).await?;

                    credentials.msa_access = Some(msa_tokens.access);
                    credentials.msa_refresh = msa_tokens.refresh;
                },
                auth::credentials::AuthStageWithData::MsaRefresh(refresh) => {
                    match authenticator.refresh_msa(&refresh).await {
                        Ok(Some(msa_tokens)) => {
                            credentials.msa_access = Some(msa_tokens.access);
                            credentials.msa_refresh = msa_tokens.refresh;
                        },
                        Ok(None) => {
                            if !allow_backwards {
                                return Err(MsaAuthorizationError::InvalidGrant.into());
                            }
                            credentials.msa_refresh = None;
                        },
                        Err(error) => {
                            if !allow_backwards || error.is_connection_error() {
                                return Err(error.into());
                            }
                            if !matches!(error, MsaAuthorizationError::InvalidGrant) {
                                eprintln!("Error using msa refresh to get msa access: {:?}", error);
                            }
                            credentials.msa_refresh = None;
                        },
                    }
                },
                auth::credentials::AuthStageWithData::MsaAccess(access) => {
                    match authenticator.authenticate_xbox(&access).await {
                        Ok(xbl) => {
                            credentials.xbl = Some(xbl);
                        },
                        Err(error) => {
                            if !allow_backwards || error.is_connection_error() {
                                return Err(error.into());
                            }
                            if !matches!(error, XboxAuthenticateError::NonOkHttpStatus(StatusCode::UNAUTHORIZED)) {
                                eprintln!("Error using msa access to get xbl token: {:?}", error);
                            }
                            credentials.msa_access = None;
                        },
                    }
                },
                auth::credentials::AuthStageWithData::XboxLive(xbl) => {
                    match authenticator.obtain_xsts(&xbl).await {
                        Ok(xsts) => {
                            credentials.xsts = Some(xsts);
                        },
                        Err(error) => {
                            if !allow_backwards || error.is_connection_error() {
                                return Err(error.into());
                            }
                            if !matches!(error, XboxAuthenticateError::NonOkHttpStatus(StatusCode::UNAUTHORIZED)) {
                                eprintln!("Error using xbl to get xsts: {:?}", error);
                            }
                            credentials.xbl = None;
                        },
                    }
                },
                auth::credentials::AuthStageWithData::XboxSecure { xsts, userhash } => {
                    match authenticator.authenticate_minecraft(&xsts, &userhash).await {
                        Ok(token) => {
                            credentials.access_token = Some(token);
                        },
                        Err(error) => {
                            if !allow_backwards || error.is_connection_error() {
                                return Err(error.into());
                            }
                            if !matches!(error, XboxAuthenticateError::NonOkHttpStatus(StatusCode::UNAUTHORIZED)) {
                                eprintln!("Error using xsts to get minecraft access token: {:?}", error);
                            }
                            credentials.xsts = None;
                        },
                    }
                },
                auth::credentials::AuthStageWithData::AccessToken(access_token) => {
                    match authenticator.get_minecraft_profile(&access_token).await {
                        Ok(profile) => {
                            login_tracker.set_count(AUTH_STAGE_COUNT as usize + 1);
                            login_tracker.notify();

                            return Ok((profile, access_token));
                        },
                        Err(error) => {
                            if !allow_backwards || error.is_connection_error() {
                                return Err(error.into());
                            }
                            if !matches!(error, XboxAuthenticateError::NonOkHttpStatus(StatusCode::UNAUTHORIZED)) {
                                eprintln!("Error using access token to get profile: {:?}", error);
                            }
                            credentials.access_token = None;
                        },
                    }
                },
            }
        }
    }

    pub async fn write_account_info(&mut self) {
        let Ok(data) = serde_json::to_vec(&self.account_info) else {
            self.send.send_error("Failed to serialize account info");
            return;
        };
        if crate::write_safe(self.directories.accounts_json.clone(), data).is_err() {
            self.send.send_error("Failed to write to accounts.json");
        }
    }

    pub fn update_profile_head(&self, profile: &MinecraftProfileResponse) {
        let Some(skin) = profile.skins.iter().find(|skin| skin.state == SkinState::Active).cloned() else {
            return;
        };

        let handle = self.self_handle.clone();
        let http_client = self.http_client.clone();
        let uuid = profile.id;
        tokio::task::spawn(async move {
            let Ok(response) = http_client.get(&*skin.url).send().await else {
                return;
            };
            let Ok(bytes) = response.bytes().await else {
                return;
            };
            let Ok(mut image) = image::load_from_memory(&bytes) else {
                return;
            };

            let head = image.crop(8, 8, 8, 8);

            let mut head_bytes = Vec::new();
            let mut cursor = Cursor::new(&mut head_bytes);
            if head.write_to(&mut cursor, image::ImageFormat::Png).is_err() {
                return;
            }

            let head_png: Arc<[u8]> = Arc::from(head_bytes);

            let head_png_32x = if head.width() != 32 || head.height() != 32 {
                let resized = head.resize_exact(32, 32, FilterType::Nearest);

                let mut head_png_32x = Vec::new();
                let mut cursor = Cursor::new(&mut head_png_32x);
                if resized.write_to(&mut cursor, image::ImageFormat::Png).is_ok() {
                    head_png_32x.into()
                } else {
                    head_png.clone()
                }
            } else {
                head_png.clone()
            };

            handle.send(MessageToBackend::UpdateAccountHeadPng {
                uuid,
                head_png,
                head_png_32x,
            });
        });
    }

    pub async fn finished_loading_worlds(&mut self, id: InstanceID, serial: bridge::serial::Serial) {
        if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
            if let Some(summaries) = instance.finish_loading_worlds(serial).await {
                self.send.send(MessageToFrontend::InstanceWorldsUpdated {
                    id: instance.id,
                    worlds: Arc::clone(&summaries)
                });

                for summary in summaries.iter() {
                    if self.watcher.watch(&summary.level_path, notify::RecursiveMode::NonRecursive).is_ok() {
                        self.watching.insert(summary.level_path.clone(), WatchTarget::InstanceWorldDir {
                            id: instance.id,
                        });
                    }
                }
            }
        }
    }

    pub async fn finished_loading_servers(&mut self, id: InstanceID, serial: bridge::serial::Serial) {
        if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
            if let Some(summaries) = instance.finish_loading_servers(serial).await {
                self.send.send(MessageToFrontend::InstanceServersUpdated {
                    id: instance.id,
                    servers: Arc::clone(&summaries)
                });
            }
        }
    }

    pub async fn finished_loading_mods(&mut self, id: InstanceID, serial: bridge::serial::Serial) {
        if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
            Self::instance_finished_loading_mods(instance, &self.send, id, serial).await
        }
    }

    pub async fn instance_finished_loading_mods(instance: &mut Instance, handle: &FrontendHandle, id: InstanceID, serial: bridge::serial::Serial) {
        if let Some(summaries) = instance.finish_loading_mods(serial).await {
            handle.send(MessageToFrontend::InstanceModsUpdated {
                id: instance.id,
                mods: Arc::clone(&summaries)
            });
        }
    }
}

fn load_accounts_json(directories: &LauncherDirectories) -> BackendAccountInfo {
    if let Ok(data) = std::fs::read(&directories.accounts_json)
        && let Ok(account_info) = serde_json::from_slice(&data)
    {
        return account_info;
    }

    BackendAccountInfo::default()
}

#[derive(thiserror::Error, Debug)]
pub enum LoginError {
    #[error("Login stage error: Backwards")]
    LoginStageErrorBackwards,
    #[error("Login stage error: Didn't change")]
    LoginStageErrorDidntChange,
    #[error("Process authorization error: {0}")]
    ProcessAuthorizationError(#[from] ProcessAuthorizationError),
    #[error("Microsoft authorization error: {0}")]
    MsaAuthorizationError(#[from] MsaAuthorizationError),
    #[error("XboxLive authentication error: {0}")]
    XboxAuthenticateError(#[from] XboxAuthenticateError),
    #[error("Cancelled by user")]
    CancelledByUser,
}
