use std::{path::Path, sync::{Arc, RwLock}};

use bridge::{
    handle::BackendHandle,
    install::ContentInstall,
    instance::{InstanceID, InstanceModID},
    message::{MessageToBackend, QuickPlayLaunch},
    modal_action::ModalAction,
};
use gpui::{prelude::*, *};
use gpui_component::{breadcrumb::Breadcrumb, v_flex};

use crate::{entity::DataEntities, modals, ui::{LauncherUI, PageType}};

pub struct LauncherRootGlobal {
    pub root: Entity<LauncherRoot>,
}

impl Global for LauncherRootGlobal {}

pub struct LauncherRoot {
    pub ui: Entity<LauncherUI>,
    pub panic_message: Arc<RwLock<Option<String>>>,
    pub backend_handle: BackendHandle,
}

impl LauncherRoot {
    pub fn new(
        data: &DataEntities,
        panic_message: Arc<RwLock<Option<String>>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let launcher_ui = cx.new(|cx| LauncherUI::new(data, window, cx));

        Self {
            ui: launcher_ui,
            panic_message,
            backend_handle: data.backend_handle.clone(),
        }
    }
}

impl Render for LauncherRoot {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(message) = &*self.panic_message.read().unwrap() {
            return v_flex().size_full().bg(gpui::blue()).child(message.clone());
        }
        if self.backend_handle.is_closed() {
            return v_flex().size_full().bg(gpui::red()).child("Backend has abruptly shutdown");
        }

        div()
            .size_full()
            .font_family("Inter 24pt")
            .child(self.ui.clone())
    }
}

pub fn start_instance(
    id: InstanceID,
    name: SharedString,
    quick_play: Option<QuickPlayLaunch>,
    backend_handle: &BackendHandle,
    window: &mut Window,
    cx: &mut App,
) {
    let modal_action = ModalAction::default();

    backend_handle.send(MessageToBackend::StartInstance {
        id,
        quick_play,
        modal_action: modal_action.clone(),
    });

    let title: SharedString = format!("Launching {}", name).into();
    modals::generic::show_modal(window, cx, title, "Error starting instance".into(), modal_action);
}

pub fn start_install(
    content_install: ContentInstall,
    backend_handle: &BackendHandle,
    window: &mut Window,
    cx: &mut App,
) {
    let modal_action = ModalAction::default();

    backend_handle.send(MessageToBackend::InstallContent {
        content: content_install.clone(),
        modal_action: modal_action.clone(),
    });

    modals::generic::show_notification(window, cx, "Error installing content".into(), modal_action);
}

pub fn start_update_check(
    instance: InstanceID,
    backend_handle: &BackendHandle,
    window: &mut Window,
    cx: &mut App,
) {
    let modal_action = ModalAction::default();

    backend_handle.send(MessageToBackend::UpdateCheck {
        instance,
        modal_action: modal_action.clone(),
    });

    let title: SharedString = "Checking for updates".into();
    modals::generic::show_modal(window, cx, title, "Error checking for updates".into(), modal_action);
}

pub fn update_single_mod(
    instance: InstanceID,
    mod_id: InstanceModID,
    backend_handle: &BackendHandle,
    window: &mut Window,
    cx: &mut App,
) {
    let modal_action = ModalAction::default();

    backend_handle.send(MessageToBackend::UpdateMod {
        instance,
        mod_id,
        modal_action: modal_action.clone(),
    });

    modals::generic::show_notification(window, cx, "Error downloading update".into(), modal_action);
}

pub fn upload_log_file(
    path: Arc<Path>,
    backend_handle: &BackendHandle,
    window: &mut Window,
    cx: &mut App,
) {
    let modal_action = ModalAction::default();

    backend_handle.send(MessageToBackend::UploadLogFile {
        path,
        modal_action: modal_action.clone(),
    });

    let title: SharedString = "Uploading log file".into();
    modals::generic::show_modal(window, cx, title, "Error uploading log file".into(), modal_action);
}

pub fn switch_page(
    page: PageType,
    breadcrumb: Option<Box<dyn Fn() -> Breadcrumb>>,
    window: &mut Window,
    cx: &mut App,
) {
    cx.update_global::<LauncherRootGlobal, ()>(|global, cx| {
        global.root.update(cx, |launcher_root, cx| {
            launcher_root.ui.update(cx, |ui, cx| {
                ui.switch_page(page, breadcrumb, window, cx);
            });
        });
    });
}
