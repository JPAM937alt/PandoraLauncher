use std::{io::{BufRead, Read}, path::Path, sync::{atomic::Ordering, Arc}, time::{Duration, SystemTime}};

use auth::{credentials::AccountCredentials, secret::PlatformSecretStorage};
use bridge::{
    install::{ContentDownload, ContentInstall, ContentInstallFile, ContentType, InstallTarget}, instance::{ContentUpdateStatus, InstanceStatus, ModSummary}, message::{LogFiles, MessageToBackend, MessageToFrontend}, meta::MetadataResult, modal_action::{ModalActionVisitUrl, ProgressTracker}, serial::AtomicOptionSerial
};
use futures::{FutureExt, TryFutureExt};
use schema::{content::ContentSource, modrinth::{ModrinthFile, ModrinthLoader}, version::{LaunchArgument, LaunchArgumentValue}};
use serde::Deserialize;
use tokio::{io::AsyncBufReadExt, sync::Semaphore};

use crate::{
    account::{BackendAccount, MinecraftLoginInfo}, arcfactory::ArcStrFactory, instance::InstanceInfo, launch::{ArgumentExpansionKey, LaunchError}, log_reader, metadata::{items::{AssetsIndexMetadataItem, MinecraftVersionManifestMetadataItem, MinecraftVersionMetadataItem, ModrinthProjectVersionsMetadataItem, ModrinthSearchMetadataItem, ModrinthVersionUpdateMetadataItem, MojangJavaRuntimeComponentMetadataItem, MojangJavaRuntimesMetadataItem, VersionUpdateParameters}, manager::MetaLoadError}, mod_metadata::ModUpdateAction, BackendState, LoginError, WatchTarget
};

impl BackendState {
    pub async fn handle_message(&mut self, message: MessageToBackend) {
        match message {
            MessageToBackend::RequestMetadata { request, force_reload } => {
                let meta = self.meta.clone();
                let send = self.send.clone();
                tokio::task::spawn(async move {
                    let (result, keep_alive_handle) = match request {
                        bridge::meta::MetadataRequest::MinecraftVersionManifest => {
                            let (result, handle) = meta.fetch_with_keepalive(&MinecraftVersionManifestMetadataItem, force_reload).await;
                            (result.map(MetadataResult::MinecraftVersionManifest), handle)
                        },
                        bridge::meta::MetadataRequest::ModrinthSearch(ref search) => {
                            let (result, handle) = meta.fetch_with_keepalive(&ModrinthSearchMetadataItem(search), force_reload).await;
                            (result.map(MetadataResult::ModrinthSearchResult), handle)
                        },
                        bridge::meta::MetadataRequest::ModrinthProjectVersions(ref project_versions) => {
                            let (result, handle) = meta.fetch_with_keepalive(&ModrinthProjectVersionsMetadataItem(project_versions), force_reload).await;
                            (result.map(MetadataResult::ModrinthProjectVersionsResult), handle)
                        },
                    };
                    let result = result.map_err(|err| format!("{}", err).into());
                    send.send(MessageToFrontend::MetadataResult {
                        request,
                        result,
                        keep_alive_handle
                    });
                });
            },
            MessageToBackend::RequestLoadWorlds { id } => {
                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    instance.start_load_worlds(self.self_handle.clone());

                    if !instance.watching_dot_minecraft {
                        instance.watching_dot_minecraft = true;
                        if self.watcher.watch(&instance.dot_minecraft_path, notify::RecursiveMode::NonRecursive).is_ok() {
                            self.watching.insert(instance.dot_minecraft_path.clone(), WatchTarget::InstanceDotMinecraftDir {
                                id: instance.id,
                            });
                        }
                    }
                    if !instance.watching_saves_dir {
                        instance.watching_saves_dir = true;
                        let saves = instance.saves_path.clone();
                        if self.watcher.watch(&saves, notify::RecursiveMode::NonRecursive).is_ok() {
                            self.watching.insert(saves.clone(), WatchTarget::InstanceSavesDir { id: instance.id });
                        }
                    }
                }
            },
            MessageToBackend::RequestLoadServers { id } => {
                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    instance.start_load_servers(self.self_handle.clone());

                    if !instance.watching_dot_minecraft {
                        instance.watching_dot_minecraft = true;
                        if self.watcher.watch(&instance.dot_minecraft_path, notify::RecursiveMode::NonRecursive).is_ok() {
                            self.watching.insert(instance.dot_minecraft_path.clone(), WatchTarget::InstanceDotMinecraftDir {
                                id: instance.id,
                            });
                        }
                    }
                    if !instance.watching_server_dat {
                        instance.watching_server_dat = true;
                        let server_dat = instance.server_dat_path.clone();
                        if self.watcher.watch(&server_dat, notify::RecursiveMode::NonRecursive).is_ok() {
                            self.watching.insert(server_dat.clone(), WatchTarget::ServersDat { id: instance.id });
                        }
                    }
                }
            },
            MessageToBackend::RequestLoadMods { id } => {
                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    instance.start_load_mods(self.self_handle.clone(), &self.mod_metadata_manager);

                    if !instance.watching_dot_minecraft {
                        instance.watching_dot_minecraft = true;
                        if self.watcher.watch(&instance.dot_minecraft_path, notify::RecursiveMode::NonRecursive).is_ok() {
                            self.watching.insert(instance.dot_minecraft_path.clone(), WatchTarget::InstanceDotMinecraftDir {
                                id: instance.id,
                            });
                        }
                    }
                    if !instance.watching_mods_dir {
                        instance.watching_mods_dir = true;
                        let mods_path = instance.mods_path.clone();
                        if self.watcher.watch(&mods_path, notify::RecursiveMode::NonRecursive).is_ok() {
                            self.watching.insert(mods_path.clone(), WatchTarget::InstanceModsDir { id: instance.id });
                        }
                    }
                }
            },
            MessageToBackend::CreateInstance { name, version, loader } => {
                if !crate::is_single_component_path(&name) {
                    self.send.send_warning(format!("Unable to create instance, name must not be a path: {}", name));
                    return;
                }
                if !sanitize_filename::is_sanitized_with_options(&*name, sanitize_filename::OptionsForCheck { windows: true, ..Default::default() }) {
                    self.send.send_warning(format!("Unable to create instance, name is invalid: {}", name));
                    return;
                }
                if self.instances.iter().any(|(_, i)| i.name == name) {
                    self.send.send_warning("Unable to create instance, name is already used".to_string());
                    return;
                }

                let instance_dir = self.directories.instances_dir.join(name.as_str());

                let _ = tokio::fs::create_dir_all(&instance_dir).await;

                self.watch_filesystem(&self.directories.instances_dir.clone(), WatchTarget::InstancesDir).await;

                let instance_info = InstanceInfo {
                    minecraft_version: version,
                    loader,
                };

                let info_path = instance_dir.join("info_v1.json");
                tokio::fs::write(info_path, serde_json::to_string_pretty(&instance_info).unwrap()).await.unwrap();
            },
            MessageToBackend::KillInstance { id } => {
                if let Some(instance) = self.instances.get_mut(id.index)
                    && instance.id == id
                {
                    if let Some(mut child) = instance.child.take() {
                        let result = child.kill();
                        if result.is_err() {
                            self.send.send_error("Failed to kill instance");
                            eprintln!("Failed to kill instance: {:?}", result.unwrap_err());
                        }

                        self.send.send(instance.create_modify_message());
                    } else {
                        self.send.send_error("Can't kill instance, instance wasn't running");
                    }
                    return;
                }

                self.send.send_error("Can't kill instance, unknown id");
            },
            MessageToBackend::StartInstance {
                id,
                quick_play,
                modal_action,
            } => {
                let secret_storage = self.secret_storage.get_or_init(PlatformSecretStorage::new).await;

                let mut credentials = if let Some(selected_account) = self.account_info.selected_account {
                    secret_storage.read_credentials(selected_account).await.ok().flatten().unwrap_or_default()
                } else {
                    AccountCredentials::default()
                };

                let login_tracker = ProgressTracker::new(Arc::from("Logging in"), self.send.clone());
                modal_action.trackers.push(login_tracker.clone());

                let login_result = self.login(&mut credentials, &login_tracker, &modal_action).await;

                if matches!(login_result, Err(LoginError::CancelledByUser)) {
                    self.send.send(MessageToFrontend::CloseModal);
                    return;
                }

                let secret_storage = self.secret_storage.get_or_init(PlatformSecretStorage::new).await;

                let (profile, access_token) = match login_result {
                    Ok(login_result) => {
                        login_tracker.set_finished(false);
                        login_tracker.notify();
                        login_result
                    },
                    Err(ref err) => {
                        if let Some(selected_account) = self.account_info.selected_account {
                            let _ = secret_storage.delete_credentials(selected_account).await;
                        }

                        modal_action.set_error_message(format!("Error logging in: {}", &err).into());
                        login_tracker.set_finished(true);
                        login_tracker.notify();
                        modal_action.set_finished();
                        return;
                    },
                };

                if let Some(selected_account) = self.account_info.selected_account
                    && profile.id != selected_account
                {
                    let _ = secret_storage.delete_credentials(selected_account).await;
                }

                let mut update_account_json = false;

                if let Some(account) = self.account_info.accounts.get_mut(&profile.id) {
                    if !account.downloaded_head {
                        account.downloaded_head = true;
                        self.update_profile_head(&profile);
                    }
                } else {
                    let mut account = BackendAccount::new_from_profile(&profile);
                    account.downloaded_head = true;
                    self.account_info.accounts.insert(profile.id, account);
                    self.update_profile_head(&profile);
                    update_account_json = true;
                }

                if self.account_info.selected_account != Some(profile.id) {
                    self.account_info.selected_account = Some(profile.id);
                    update_account_json = true;
                }

                if secret_storage.write_credentials(profile.id, &credentials).await.is_err() {
                    self.send.send_warning("Unable to write credentials to keychain. You might need to fully log in again next time");
                }

                if update_account_json {
                    self.write_account_info().await;
                    self.send.send(self.account_info.create_update_message());
                }

                let login_info = MinecraftLoginInfo {
                    uuid: profile.id,
                    username: profile.name.clone(),
                    access_token,
                };

                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    if instance.child.is_some() {
                        self.send.send_warning("Can't launch instance, already running");
                        modal_action.set_error_message("Can't launch instance, already running".into());
                        modal_action.set_finished();
                        return;
                    }

                    if modal_action.has_requested_cancel() {
                        self.send.send(MessageToFrontend::CloseModal);
                        return;
                    }

                    self.send.send(MessageToFrontend::MoveInstanceToTop {
                        id
                    });
                    self.send.send(instance.create_modify_message_with_status(InstanceStatus::Launching));

                    let launch_tracker = ProgressTracker::new(Arc::from("Launching"), self.send.clone());
                    modal_action.trackers.push(launch_tracker.clone());

                    let result = self.launcher.launch(&self.http_client, instance, quick_play, login_info, &launch_tracker, &modal_action).await;

                    if matches!(result, Err(LaunchError::CancelledByUser)) {
                        self.send.send(MessageToFrontend::CloseModal);
                        self.send.send(instance.create_modify_message());
                        return;
                    }

                    let is_err = result.is_err();
                    match result {
                        Ok(mut child) => {
                            if let Some(stdout) = child.stdout.take() {
                                log_reader::start_game_output(stdout, child.stderr.take(), self.send.clone());
                            }
                            instance.child = Some(child);
                        },
                        Err(ref err) => {
                            modal_action.set_error_message(format!("{}", &err).into());
                        },
                    }

                    launch_tracker.set_finished(is_err);
                    launch_tracker.notify();
                    modal_action.set_finished();

                    self.send.send(instance.create_modify_message());

                    return;
                }

                self.send.send_error("Can't launch instance, unknown id");
                modal_action.set_error_message("Can't launch instance, unknown id".into());
                modal_action.set_finished();
            },
            MessageToBackend::SetModEnabled { id, mod_id, enabled } => {
                if let Some(instance) = self.instances.get_mut(id.index)
                    && instance.id == id
                    && let Some(instance_mod) = instance.try_get_mod(mod_id)
                {
                    if instance_mod.enabled == enabled {
                        return;
                    }

                    let mut new_path = instance_mod.path.to_path_buf();
                    if instance_mod.enabled {
                        new_path.add_extension("disabled");
                    } else {
                        new_path.set_extension("");
                    };

                    self.reload_mods_immediately.insert(id);
                    let _ = std::fs::rename(&instance_mod.path, new_path);
                }
            },
            MessageToBackend::UpdateAccountHeadPng {
                uuid,
                head_png,
                head_png_32x,
            } => {
                if let Some(account) = self.account_info.accounts.get_mut(&uuid) {
                    let head_png = Some(head_png);
                    if account.head != head_png {
                        account.head = head_png;
                        account.head_32x = Some(head_png_32x);
                        self.send.send(self.account_info.create_update_message());
                        self.write_account_info().await;
                    }
                }
            },
            MessageToBackend::DownloadAllMetadata => {
                self.download_all_metadata().await;
            },
            MessageToBackend::InstallContent { content, modal_action } => {
                self.install_content(content, modal_action).await;
            },
            MessageToBackend::DeleteMod { id, mod_id } => {
                if let Some(instance) = self.instances.get_mut(id.index)
                    && instance.id == id
                    && let Some(instance_mod) = instance.try_get_mod(mod_id)
                {
                    self.reload_mods_immediately.insert(id);
                    let _ = std::fs::remove_file(&instance_mod.path);
                }
            },
            MessageToBackend::UpdateCheck { instance: id, modal_action } => {
                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    if let Some(serial) = instance.start_load_mods(self.self_handle.clone(), &self.mod_metadata_manager) {
                        Self::instance_finished_loading_mods(instance, &self.send, id, serial).await;
                    }
                    let Some(mods) = &instance.mods else {
                        modal_action.set_finished();
                        return;
                    };

                    let modrinth_loader = instance.loader.as_modrinth_loader();
                    if modrinth_loader == ModrinthLoader::Unknown {
                        modal_action.set_error_message("Unable to update instance, unsupported loader".into());
                        modal_action.set_finished();
                        return;
                    }

                    let tracker = ProgressTracker::new("Checking mods".into(), self.send.clone());
                    tracker.set_total(mods.len());
                    modal_action.trackers.push(tracker.clone());

                    let semaphore = Semaphore::new(8);

                    let params = VersionUpdateParameters {
                        loaders: [modrinth_loader].into(),
                        game_versions: [instance.version].into(),
                    };

                    let meta = self.meta.clone();

                    let mut futures = Vec::new();

                    struct UpdateResult {
                        mod_summary: Arc<ModSummary>,
                        action: ModUpdateAction,
                    }

                    { // Scope is needed so await doesn't complain about the non-send RwLockReadGuard
                        let sources = self.mod_metadata_manager.read_content_sources();
                        for summary in mods.iter() {
                            let source = sources.get(&summary.mod_summary.hash).copied().unwrap_or(ContentSource::Manual);
                            let semaphore = &semaphore;
                            let meta = &meta;
                            let params = &params;
                            let tracker = &tracker;
                            futures.push(async move {
                                match source {
                                    ContentSource::Manual => {
                                        tracker.add_count(1);
                                        tracker.notify();
                                        Ok(ModUpdateAction::ManualInstall)
                                    },
                                    ContentSource::Modrinth => {
                                        let permit = semaphore.acquire().await.unwrap();
                                        let result = meta.fetch(&ModrinthVersionUpdateMetadataItem {
                                            sha1: hex::encode(summary.mod_summary.hash).into(),
                                            params: params.clone()
                                        }).await;
                                        drop(permit);

                                        tracker.add_count(1);
                                        tracker.notify();

                                        if let Err(MetaLoadError::NonOK(404)) = result {
                                            return Ok(ModUpdateAction::ErrorNotFound);
                                        }

                                        let result = result?;

                                        let install_file = result
                                            .0
                                            .files
                                            .iter()
                                            .find(|file| file.primary)
                                            .unwrap_or(result.0.files.first().unwrap());

                                        let mut latest_hash = [0u8; 20];
                                        let Ok(_) = hex::decode_to_slice(&*install_file.hashes.sha1, &mut latest_hash) else {
                                            return Ok(ModUpdateAction::ErrorInvalidHash);
                                        };

                                        if latest_hash == summary.mod_summary.hash {
                                            Ok(ModUpdateAction::AlreadyUpToDate)
                                        } else {
                                            Ok(ModUpdateAction::Modrinth(install_file.clone()))
                                        }
                                    },
                                }
                            }.map_ok(|action| UpdateResult {
                                mod_summary: summary.mod_summary.clone(),
                                action,
                            }));
                        }
                        drop(sources);
                    }

                    let results: Result<Vec<UpdateResult>, MetaLoadError> = futures::future::try_join_all(futures).await;

                    match results {
                        Ok(updates) => {
                            let mut meta_updates = self.mod_metadata_manager.updates.write().unwrap();

                            for update in updates {
                                update.mod_summary.update_status.store(update.action.to_status(), Ordering::Relaxed);
                                meta_updates.insert(update.mod_summary.hash, update.action);
                            }
                        },
                        Err(error) => {
                            tracker.set_finished(true);
                            modal_action.set_error_message(format!("Error checking for updates: {}", error).into());
                            modal_action.set_finished();
                            return;
                        },
                    }

                    tracker.set_finished(false);
                    modal_action.set_finished();

                    return;
                }

                self.send.send_error("Can't update instance, unknown id");
                modal_action.set_error_message("Can't update instance, unknown id".into());
                modal_action.set_finished();
            },
            MessageToBackend::FinishedLoadingWorlds { instance, serial } => {
                self.finished_loading_worlds(instance, serial).await;
            },
            MessageToBackend::FinishedLoadingServers { instance, serial } => {
                self.finished_loading_servers(instance, serial).await;
            },
            MessageToBackend::FinishedLoadingMods { instance, serial } => {
                self.finished_loading_mods(instance, serial).await;
            },
            MessageToBackend::UpdateMod { instance: id, mod_id, modal_action } => {
                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    let Some(mod_summary) = instance.try_get_mod(mod_id) else {
                        self.send.send_error("Can't update mod in instance, unknown mod id");
                        modal_action.set_finished();
                        return;
                    };

                    let Some(update_info) = self.mod_metadata_manager.updates.read().unwrap().get(&mod_summary.mod_summary.hash).cloned() else {
                        self.send.send_error("Can't update mod in instance, missing update action");
                        modal_action.set_finished();
                        return;
                    };

                    match update_info {
                        ModUpdateAction::ErrorNotFound => {
                            self.send.send_error("Can't update mod in instance, 404 not found");
                            modal_action.set_finished();
                        },
                        ModUpdateAction::ErrorInvalidHash => {
                            self.send.send_error("Can't update mod in instance, returned invalid hash");
                            modal_action.set_finished();
                        },
                        ModUpdateAction::AlreadyUpToDate => {
                            self.send.send_error("Can't update mod in instance, already up-to-date");
                            modal_action.set_finished();
                        },
                        ModUpdateAction::ManualInstall => {
                            self.send.send_error("Can't update mod in instance, mod was manually installed");
                            modal_action.set_finished();
                        },
                        ModUpdateAction::Modrinth(modrinth_file) => {
                            let content = ContentInstall {
                                target: InstallTarget::Instance(id),
                                files: [ContentInstallFile {
                                    replace: Some(mod_summary.path.clone()),
                                    download: ContentDownload::Url {
                                        url: modrinth_file.url.clone(),
                                        filename: if mod_summary.enabled {
                                            modrinth_file.filename
                                        } else {
                                            format!("{}.disabled", modrinth_file.filename).into()
                                        },
                                        sha1: modrinth_file.hashes.sha1.clone(),
                                        size: modrinth_file.size,
                                    },
                                    content_type: ContentType::Mod,
                                    content_source: ContentSource::Modrinth,
                                }].into(),
                            };
                            self.install_content(content, modal_action).await;
                        },
                    }

                    return;
                }

                self.send.send_error("Can't update mod in instance, unknown instance id");
                modal_action.set_finished();
            },
            MessageToBackend::Sleep5s => {
                tokio::time::sleep(Duration::from_secs(5)).await;
            },
            MessageToBackend::ReadLog { path, send } => {
                let frontend = self.send.clone();
                let serial = AtomicOptionSerial::default();

                let file = match std::fs::File::open(path) {
                    Ok(file) => file,
                    Err(e) => {
                        let error = format!("Unable to read file: {e}");
                        for line in error.split('\n') {
                            let replaced = log_reader::replace(line.trim_ascii_end());
                            if send.send(replaced.into()).await.is_err() {
                                return;
                            }
                        }
                        frontend.send_with_serial(MessageToFrontend::Refresh, &serial);
                        return;
                    },
                };

                let mut reader = std::io::BufReader::new(file);
                let Ok(buffer) = reader.fill_buf() else {
                    return;
                };
                if buffer.len() >= 2 && buffer[0] == 0x1F && buffer[1] == 0x8B {
                    let gz_decoder = flate2::bufread::GzDecoder::new(reader);
                    let mut buf_reader = std::io::BufReader::new(gz_decoder);
                    tokio::task::spawn_blocking(move || {
                        let mut line = String::new();
                        let mut factory = ArcStrFactory::default();
                        loop {
                            match buf_reader.read_line(&mut line) {
                                Ok(0) => return,
                                Ok(_) => {
                                    let replaced = log_reader::replace(line.trim_ascii_end());
                                    if send.blocking_send(factory.create(&replaced)).is_err() {
                                        return;
                                    }
                                    line.clear();
                                    frontend.send_with_serial(MessageToFrontend::Refresh, &serial);
                                },
                                Err(e) => {
                                    let error = format!("Error while reading file: {e}");
                                    for line in error.split('\n') {
                                        let replaced = log_reader::replace(line.trim_ascii_end());
                                        if send.blocking_send(factory.create(&replaced)).is_err() {
                                            return;
                                        }
                                    }
                                    frontend.send_with_serial(MessageToFrontend::Refresh, &serial);
                                    return;
                                },
                            }
                        }
                    });
                    return;
                }

                let mut line: Vec<u8> = buffer.into();
                let file = reader.into_inner();
                let mut reader = tokio::io::BufReader::new(tokio::fs::File::from_std(file));

                tokio::task::spawn(async move {
                    let mut first = true;
                    let mut factory = ArcStrFactory::default();
                    loop {
                        tokio::select! {
                            _ = send.closed() => {
                                return;
                            },
                            read = reader.read_until('\n' as u8, &mut line) => match read {
                                Ok(0) => {
                                    // EOF reached. If this file is being actively written to (e.g. latest.log),
                                    // then there could be more data
                                    tokio::time::sleep(Duration::from_millis(250)).await;
                                },
                                Ok(_) => {
                                    match str::from_utf8(&*line) {
                                        Ok(utf8) => {
                                            if first {
                                                first = false;
                                                for line in utf8.split('\n') {
                                                    let replaced = log_reader::replace(line.trim_ascii_end());
                                                    if send.send(factory.create(&replaced)).await.is_err() {
                                                        return;
                                                    }
                                                }
                                            } else {
                                                let replaced = log_reader::replace(utf8.trim_ascii_end());
                                                if send.send(factory.create(&replaced)).await.is_err() {
                                                    return;
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            let error = format!("Invalid UTF8: {e}");
                                            for line in error.split('\n') {
                                                let replaced = log_reader::replace(line.trim_ascii_end());
                                                if send.blocking_send(factory.create(&replaced)).is_err() {
                                                    return;
                                                }
                                            }
                                        },
                                    }
                                    frontend.send_with_serial(MessageToFrontend::Refresh, &serial);
                                    line.clear();
                                },
                                Err(e) => {
                                    let error = format!("Error while reading file: {e}");
                                    for line in error.split('\n') {
                                        let replaced = log_reader::replace(line.trim_ascii_end());
                                        if send.blocking_send(factory.create(&replaced)).is_err() {
                                            return;
                                        }
                                    }
                                    frontend.send_with_serial(MessageToFrontend::Refresh, &serial);
                                    return;
                                },
                            }
                        }
                    }
                });
            },
            MessageToBackend::GetLogFiles { instance: id, channel } => {
                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    let logs = instance.dot_minecraft_path.join("logs");

                    if let Ok(read_dir) = std::fs::read_dir(logs) {
                        let mut paths_with_time = Vec::new();
                        let mut total_gzipped_size = 0;

                        for file in read_dir {
                            let Ok(entry) = file else {
                                continue;
                            };
                            let Ok(metadata) = entry.metadata() else {
                                continue;
                            };
                            let filename = entry.file_name();
                            let Some(filename) = filename.to_str() else {
                                continue;
                            };

                            if filename.ends_with(".log.gz") {
                                total_gzipped_size += metadata.len();
                            } else if !filename.ends_with(".log") {
                                continue;
                            }

                            let created = metadata.created().unwrap_or(SystemTime::UNIX_EPOCH);
                            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

                            paths_with_time.push((Arc::from(entry.path()), created.max(modified)));
                        }

                        paths_with_time.sort_by_key(|(_, t)| *t);
                        let paths = paths_with_time.into_iter().map(|(p, _)| p).rev().collect();

                        let _ = channel.send(LogFiles { paths, total_gzipped_size: total_gzipped_size.min(usize::MAX as u64) as usize });
                    }
                }
            },
            MessageToBackend::CleanupOldLogFiles { instance: id } => {
                let mut deleted = 0;

                if let Some(instance) = self.instances.get_mut(id.index) && instance.id == id {
                    let logs = instance.dot_minecraft_path.join("logs");

                    if let Ok(read_dir) = std::fs::read_dir(logs) {
                        for file in read_dir {
                            let Ok(entry) = file else {
                                continue;
                            };

                            let filename = entry.file_name();
                            let Some(filename) = filename.to_str() else {
                                continue;
                            };

                            if filename.ends_with(".log.gz") {
                                if std::fs::remove_file(entry.path()).is_ok() {
                                    deleted += 1;
                                }
                            }
                        }
                    }
                }

                self.send.send_success(format!("Deleted {} files", deleted));
            },
            MessageToBackend::UploadLogFile { path, modal_action } => {
                let file = match std::fs::File::open(path) {
                    Ok(file) => file,
                    Err(e) => {
                        let error = format!("Unable to read file: {e}");
                        modal_action.set_error_message(log_reader::replace(&error).into());
                        modal_action.set_finished();
                        return;
                    },
                };

                let tracker = ProgressTracker::new("Reading log file".into(), self.send.clone());
                tracker.set_total(4);
                tracker.notify();
                modal_action.trackers.push(tracker.clone());

                let mut reader = std::io::BufReader::new(file);
                let Ok(buffer) = reader.fill_buf() else {
                    tracker.set_finished(true);
                    tracker.notify();
                    return;
                };

                let mut content = String::new();

                if buffer.len() >= 2 && buffer[0] == 0x1F && buffer[1] == 0x8B {
                    let mut gz_decoder = flate2::bufread::GzDecoder::new(reader);
                    if let Err(e) = gz_decoder.read_to_string(&mut content) {
                        let error = format!("Error while reading file: {e}");
                        modal_action.set_error_message(log_reader::replace(&error).into());
                        modal_action.set_finished();
                        return;
                    }
                } else {
                    if let Err(e) = reader.read_to_string(&mut content) {
                        let error = format!("Error while reading file: {e}");
                        modal_action.set_error_message(log_reader::replace(&error).into());
                        modal_action.set_finished();
                        return;
                    }
                }

                tracker.set_title("Redacting sensitive information".into());
                tracker.set_count(1);
                tracker.notify();

                let replaced = log_reader::replace(&*content);

                tracker.set_title("Uploading to mclo.gs".into());
                tracker.set_count(2);
                tracker.notify();

                if replaced.trim_ascii().is_empty() {
                    modal_action.set_error_message("Log file was empty, didn't upload".into());
                    modal_action.set_finished();
                    return;
                }

                let result = self.http_client.post("https://api.mclo.gs/1/log").form(&[("content", &*replaced)]).send().await;

                let resp = match result {
                    Ok(resp) => resp,
                    Err(e) => {
                        let error = format!("Error while uploading log: {e:?}");
                        modal_action.set_error_message(error.into());
                        modal_action.set_finished();
                        return;
                    },
                };

                tracker.set_count(3);
                tracker.notify();

                let bytes = match resp.bytes().await {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        let error = format!("Error while reading mclo.gs response: {e:?}");
                        modal_action.set_error_message(error.into());
                        modal_action.set_finished();
                        return;
                    },
                };

                #[derive(Deserialize)]
                struct McLogsResponse {
                    success: bool,
                    url: Option<String>,
                    error: Option<String>,
                }

                let response: McLogsResponse = match serde_json::from_slice(&bytes) {
                    Ok(response) => response,
                    Err(e) => {
                        let error = format!("Error while deserializing mclo.gs response: {e:?}");
                        modal_action.set_error_message(error.into());
                        modal_action.set_finished();
                        return;
                    },
                };

                if response.success {
                    if let Some(url) = response.url {
                        modal_action.set_visit_url(ModalActionVisitUrl {
                            message: format!("Open {}", url).into(),
                            url: url.into(),
                            prevent_auto_finish: true,
                        });
                        modal_action.set_finished();
                    } else {
                        modal_action.set_error_message("Success returned, but missing url".into());
                        modal_action.set_finished();
                    }
                } else {
                    if let Some(e) = response.error {
                        let error = format!("mclo.gs rejected upload: {e}");
                        modal_action.set_error_message(error.into());
                        modal_action.set_finished();
                    } else {
                        modal_action.set_error_message("Failure returned, but missing error".into());
                        modal_action.set_finished();
                    }
                }

                tracker.set_count(4);
                tracker.set_finished(false);
                tracker.notify();
            },
        }
    }

    pub async fn download_all_metadata(&self) {
        let Ok(versions) = self.meta.fetch(&MinecraftVersionManifestMetadataItem).await else {
            panic!("Unable to get Minecraft version manifest");
        };

        for link in &versions.versions {
            let Ok(version_info) = self.meta.fetch(&MinecraftVersionMetadataItem(link)).await else {
                panic!("Unable to get load version: {:?}", link.id);
            };

            let asset_index = format!("{}", version_info.assets);

            let Ok(_) = self.meta.fetch(&AssetsIndexMetadataItem {
                url: version_info.asset_index.url,
                cache: self.directories.assets_index_dir.join(format!("{}.json", &asset_index)).into(),
                hash: version_info.asset_index.sha1,
            }).await else {
                panic!("Can't get assets index {:?}", version_info.asset_index.url);
            };

            if let Some(arguments) = &version_info.arguments {
                for argument in arguments.game.iter() {
                    let value = match argument {
                        LaunchArgument::Single(launch_argument_value) => launch_argument_value,
                        LaunchArgument::Ruled(launch_argument_ruled) => &launch_argument_ruled.value,
                    };
                    match value {
                        LaunchArgumentValue::Single(shared_string) => {
                            check_argument_expansions(shared_string.as_str());
                        },
                        LaunchArgumentValue::Multiple(shared_strings) => {
                            for shared_string in shared_strings.iter() {
                                check_argument_expansions(shared_string.as_str());
                            }
                        },
                    }
                }
            } else if let Some(legacy_arguments) = &version_info.minecraft_arguments {
                for argument in legacy_arguments.split_ascii_whitespace() {
                    check_argument_expansions(argument);
                }
            }
        }

        let Ok(runtimes) = self.meta.fetch(&MojangJavaRuntimesMetadataItem).await else {
            panic!("Unable to get java runtimes manifest");
        };

        for (platform_name, platform) in &runtimes.platforms {
            for (jre_component, components) in &platform.components {
                if components.is_empty() {
                    continue;
                }

                let runtime_component_dir = self.directories.runtime_base_dir.join(jre_component).join(platform_name.as_str());
                let _ = std::fs::create_dir_all(&runtime_component_dir);
                let Ok(runtime_component_dir) = runtime_component_dir.canonicalize() else {
                    panic!("Unable to create runtime component dir");
                };

                for runtime_component in components {
                    let Ok(manifest) = self.meta.fetch(&MojangJavaRuntimeComponentMetadataItem {
                        url: runtime_component.manifest.url,
                        cache: runtime_component_dir.join("manifest.json").into(),
                        hash: runtime_component.manifest.sha1,
                    }).await else {
                        panic!("Unable to get java runtime component manifest");
                    };

                    let keys: &[Arc<std::path::Path>] = &[
                        std::path::Path::new("bin/java").into(),
                        std::path::Path::new("bin/javaw.exe").into(),
                        std::path::Path::new("jre.bundle/Contents/Home/bin/java").into(),
                        std::path::Path::new("MinecraftJava.exe").into(),
                    ];

                    let mut known_executable_path = false;
                    for key in keys {
                        if manifest.files.contains_key(key) {
                            known_executable_path = true;
                            break;
                        }
                    }

                    if !known_executable_path {
                        eprintln!("Warning: {}/{} doesn't contain known java executable", jre_component, platform_name);
                    }
                }
            }
        }

        println!("Done downloading all metadata");
    }
}

fn check_argument_expansions(argument: &str) {
    let mut dollar_last = false;
    for (i, character) in argument.char_indices() {
        if character == '$' {
            dollar_last = true;
        } else if dollar_last && character == '{' {
            let remaining = &argument[i..];
            if let Some(end) = remaining.find('}') {
                let to_expand = &argument[i+1..i+end];
                if ArgumentExpansionKey::from_str(to_expand).is_none() {
                    eprintln!("Unsupported argument: {:?}", to_expand);
                }
            }
        } else {
            dollar_last = false;
        }
    }
}
