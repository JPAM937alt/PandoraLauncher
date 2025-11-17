use std::sync::{
    atomic::{AtomicUsize, Ordering}, Arc, Mutex
};

use bridge::{
    handle::BackendHandle, install::{ContentDownload, ContentInstall, ContentInstallFile, ContentType, InstallTarget}, instance::{InstanceID, InstanceModSummary}, message::{AtomicBridgeDataLoadState, MessageToBackend}, serial::AtomicOptionSerial
};
use gpui::{prelude::*, *};
use gpui_component::{
    breadcrumb::{Breadcrumb, BreadcrumbItem}, button::{Button, ButtonVariants}, h_flex, list::{ListDelegate, ListItem, ListState}, notification::{Notification, NotificationType}, switch::Switch, v_flex, ActiveTheme as _, Icon, IconName, IndexPath, Sizable, WindowExt
};
use rustc_hash::FxHashSet;
use schema::content::ContentSource;

use crate::{entity::instance::InstanceEntry, png_render_cache, root};

use super::instance_page::InstanceSubpageType;

pub struct InstanceModsSubpage {
    instance: InstanceID,
    instance_title: SharedString,
    backend_handle: BackendHandle,
    mods_state: Arc<AtomicBridgeDataLoadState>,
    mod_list: Entity<ListState<ModsListDelegate>>,
    mods_serial: AtomicOptionSerial,
    _add_from_file_task: Option<Task<()>>,
}

impl InstanceModsSubpage {
    pub fn new(
        instance: &Entity<InstanceEntry>,
        backend_handle: BackendHandle,
        window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> Self {
        let instance = instance.read(cx);
        let instance_title = instance.title().into();
        let instance_id = instance.id;

        let mods_state = Arc::clone(&instance.mods_state);

        let mods_list_delegate = ModsListDelegate {
            id: instance_id,
            backend_handle: backend_handle.clone(),
            mods: instance.mods.read(cx).to_vec(),
            searched: instance.mods.read(cx).to_vec(),
            confirming_delete: Arc::new(AtomicUsize::new(0)),
            updating: Default::default(),
        };

        let mods = instance.mods.clone();

        let mod_list = cx.new(move |cx| {
            cx.observe(&mods, |list: &mut ListState<ModsListDelegate>, mods, cx| {
                let mods = mods.read(cx).to_vec();
                let delegate = list.delegate_mut();

                let mut updating = delegate.updating.lock().unwrap();
                if !updating.is_empty() {
                    let ids: FxHashSet<u64> = mods.iter().map(|summary| summary.filename_hash).collect();
                    updating.retain(|id| ids.contains(&id));
                }

                delegate.mods = mods.clone();
                delegate.searched = mods;
                delegate.confirming_delete.store(0, Ordering::Release);
                cx.notify();
            }).detach();

            ListState::new(mods_list_delegate, window, cx).selectable(false).searchable(true)
        });

        Self {
            instance: instance_id,
            instance_title,
            backend_handle,
            mods_state,
            mod_list,
            mods_serial: AtomicOptionSerial::default(),
            _add_from_file_task: None,
        }
    }
}

impl Render for InstanceModsSubpage {
    fn render(&mut self, _window: &mut gpui::Window, cx: &mut gpui::Context<Self>) -> impl gpui::IntoElement {
        let theme = cx.theme();

        let state = self.mods_state.load(Ordering::SeqCst);
        if state.should_send_load_request() {
            self.backend_handle.send_with_serial(MessageToBackend::RequestLoadMods { id: self.instance }, &self.mods_serial);
        }

        let header = h_flex()
            .gap_3()
            .mb_1()
            .ml_1()
            .child(div().text_lg().child("Mods"))
            .child(Button::new("update").label("Check for updates").success().compact().small().on_click({
                let backend_handle = self.backend_handle.clone();
                let instance_id = self.instance;
                move |_, window, cx| {
                    crate::root::start_update_check(instance_id, &backend_handle, window, cx);
                }
            }))
            .child(Button::new("addmr").label("Add from Modrinth").success().compact().small().on_click({
                let instance = self.instance;
                let instance_title = self.instance_title.clone();
                move |_, window, cx| {
                    let page = crate::ui::PageType::Modrinth { installing_for: Some(instance) };

                    let instance_title = instance_title.clone();
                    let breadcrumb = move || {
                        let instances_item = BreadcrumbItem::new("Instances").on_click(|_, window, cx| {
                            root::switch_page(crate::ui::PageType::Instances, None, window, cx);
                        });
                        let instance_item = BreadcrumbItem::new(instance_title.clone()).on_click(move |_, window, cx| {
                            root::switch_page(crate::ui::PageType::InstancePage(instance, InstanceSubpageType::Mods), None, window, cx);
                        });
                        Breadcrumb::new().text_xl().child(instances_item).child(instance_item)
                    };

                    root::switch_page(page, Some(Box::new(breadcrumb)), window, cx);

                }
            }))
            .child(Button::new("addfile").label("Add from file").success().compact().small().on_click({
                let backend_handle = self.backend_handle.clone();
                let instance = self.instance;
                cx.listener(move |this, _, window, cx| {
                    let receiver = cx.prompt_for_paths(PathPromptOptions {
                        files: true,
                        directories: false,
                        multiple: true,
                        prompt: Some("Select mods to install".into())
                    });

                    let backend_handle = backend_handle.clone();
                    let add_from_file_task = window.spawn(cx, async move |cx| {
                        let Ok(result) = receiver.await else {
                            return;
                        };
                        _ = cx.update(move |window, cx| {
                            match result {
                                Ok(Some(paths)) => {
                                    let content_install = ContentInstall {
                                        target: InstallTarget::Instance(instance),
                                        files: paths.into_iter().map(|path| {
                                            ContentInstallFile {
                                                replace: None,
                                                download: ContentDownload::File { path },
                                                content_type: ContentType::Mod,
                                                content_source: ContentSource::Manual,
                                            }
                                        }).collect(),
                                    };
                                    crate::root::start_install(content_install, &backend_handle, window, cx);
                                },
                                Ok(None) => {},
                                Err(error) => {
                                    let error = format!("{}", error);
                                    let notification = Notification::new()
                                        .autohide(false)
                                        .with_type(NotificationType::Error)
                                        .title(error);
                                    window.push_notification(notification, cx);
                                },
                            }
                        });
                    });
                    this._add_from_file_task = Some(add_from_file_task);
                })
            }));

        v_flex().p_4().size_full().child(header).child(
            div()
                .size_full()
                .border_1()
                .rounded(theme.radius)
                .border_color(theme.border)
                .child(self.mod_list.clone()),
        )
    }
}

pub struct ModsListDelegate {
    id: InstanceID,
    backend_handle: BackendHandle,
    mods: Vec<InstanceModSummary>,
    searched: Vec<InstanceModSummary>,
    confirming_delete: Arc<AtomicUsize>,
    updating: Arc<Mutex<FxHashSet<u64>>>,
}

impl ListDelegate for ModsListDelegate {
    type Item = ListItem;

    fn items_count(&self, _section: usize, _cx: &App) -> usize {
        self.searched.len()
    }

    fn render_item(&self, ix: IndexPath, _window: &mut Window, cx: &mut App) -> Option<Self::Item> {
        let summary = self.searched.get(ix.row)?;

        let icon = if let Some(png_icon) = summary.mod_summary.png_icon.as_ref() {
            png_render_cache::render(Arc::clone(png_icon), cx)
        } else {
            gpui::img(ImageSource::Resource(Resource::Embedded("images/default_mod.png".into())))
        };

        const GRAY: Hsla = Hsla { h: 0.0, s: 0.0, l: 0.5, a: 1.0};

        let description1 = v_flex()
            .w_1_5()
            .text_ellipsis()
            .child(SharedString::from(summary.mod_summary.name.clone()))
            .child(SharedString::from(summary.mod_summary.version_str.clone()));

        let description2 = v_flex()
            .text_color(GRAY)
            .child(SharedString::from(summary.mod_summary.authors.clone()))
            .child(SharedString::from(summary.filename.clone()));

        let id = self.id;
        let mod_id = summary.id;
        let element_id = summary.filename_hash;

        let delete_button = if self.confirming_delete.load(Ordering::Relaxed) == ix.row + 1 {
            Button::new(("delete", element_id)).danger().icon(IconName::Check).on_click({
                let backend_handle = self.backend_handle.clone();
                move |_, _, _| {
                    backend_handle.send(MessageToBackend::DeleteMod { id, mod_id });
                }
            })
        } else {
            let trash_icon = Icon::default().path("icons/trash-2.svg");
            let confirming_delete = self.confirming_delete.clone();
            let delete_ix = ix.row + 1;
            Button::new(("delete", element_id)).danger().icon(trash_icon).on_click(move |_, _, _| {
                confirming_delete.store(delete_ix, Ordering::Release);
            })
        };

        let update_button = match summary.mod_summary.update_status.load(Ordering::Relaxed) {
            bridge::instance::ContentUpdateStatus::Unknown => None,
            bridge::instance::ContentUpdateStatus::ManualInstall => Some(
                Button::new(("update", element_id)).warning().icon(Icon::default().path("icons/file-question-mark.svg"))
                    .tooltip("Mod was installed manually - cannot automatically update")
            ),
            bridge::instance::ContentUpdateStatus::ErrorNotFound => Some(
                Button::new(("update", element_id)).danger().icon(Icon::default().path("icons/triangle-alert.svg"))
                    .tooltip("Error while checking updates - 404 not found")
            ),
            bridge::instance::ContentUpdateStatus::ErrorInvalidHash => Some(
                Button::new(("update", element_id)).danger().icon(Icon::default().path("icons/triangle-alert.svg"))
                    .tooltip("Error while checking updates - returned invalid hash")
            ),
            bridge::instance::ContentUpdateStatus::AlreadyUpToDate => Some(
                Button::new(("update", element_id)).icon(Icon::default().path("icons/check.svg"))
                    .tooltip("Mod is already up-to-date")
            ),
            bridge::instance::ContentUpdateStatus::Modrinth => {
                let loading = self.updating.lock().unwrap().contains(&element_id);
                Some(
                    Button::new(("update", element_id)).success().loading(loading).icon(Icon::default().path("icons/download.svg"))
                        .tooltip("Download update from Modrinth").on_click({
                            let backend_handle = self.backend_handle.clone();
                            let updating = self.updating.clone();
                            move |_, window, cx| {
                                updating.lock().unwrap().insert(element_id);
                                crate::root::update_single_mod(id, mod_id, &backend_handle, window, cx);
                            }
                        })
                )
            },
        };

        let backend_handle = self.backend_handle.clone();

        let mut item_content = h_flex()
            .gap_1()
            .child(
                Switch::new(("toggle", element_id))
                    .checked(summary.enabled)
                    .on_click(move |checked, _, _| {
                        backend_handle.send(MessageToBackend::SetModEnabled {
                            id,
                            mod_id,
                            enabled: *checked,
                        });
                    })
                    .px_2(),
            )
            .child(icon.size_16().min_w_16().min_h_16().grayscale(!summary.enabled))
            .when(!summary.enabled, |this| this.line_through())
            .child(description1)
            .child(description2);

        if let Some(update_button) = update_button {
            item_content = item_content.child(h_flex().absolute().right_4().gap_2().child(update_button).child(delete_button))
        } else {
            item_content = item_content.child(delete_button.absolute().right_4())
        }

        let item = ListItem::new(("item", element_id)).p_1().child(item_content);

        Some(item)
    }

    fn set_selected_index(&mut self, _ix: Option<IndexPath>, _window: &mut Window, _cx: &mut Context<ListState<Self>>) {
    }

    fn perform_search(&mut self, query: &str, _window: &mut Window, _cx: &mut Context<ListState<Self>>) -> Task<()> {
        self.searched = self
            .mods
            .iter()
            .filter(|m| m.mod_summary.name.contains(query) || m.mod_summary.id.contains(query))
            .cloned()
            .collect();

        Task::ready(())
    }
}
