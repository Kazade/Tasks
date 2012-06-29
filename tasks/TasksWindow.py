# -*- Mode: Python; coding: utf-8; indent-tabs-mode: nil; tab-width: 4 -*-
### BEGIN LICENSE
# This file is in the public domain
### END LICENSE

import gettext
from gettext import gettext as _
gettext.textdomain('tasks')

from gi.repository import Gtk, Gdk, GdkPixbuf # pylint: disable=E0611
import logging
logger = logging.getLogger('tasks')

from tasks_lib import Window
from tasks.AboutTasksDialog import AboutTasksDialog
from tasks.PreferencesTasksDialog import PreferencesTasksDialog
from tasks.NewTaskDialog import NewTaskDialog

from tasks.store import Task, Store

from .image_toggle import ImageToggle
from .task_details_pane import TaskDetailsPane
from .task_list_entry import TaskListEntry

import os
import ConfigParser

UNCHECKED_IMAGE = "./data/media/unchecked.svg"
CHECKED_IMAGE = "./data/media/checked.svg"

from tasks.store import Store

class sync_checkbox_to_task_complete(object):
    def __init__(self, checkbox, task):
        self._checkbox = checkbox
        self._task = task

    def __call__(self, sender, **kwargs):
        instance = kwargs.pop("instance")
        if instance.pk == self._task.pk:
            self._checkbox.set_active(instance.complete)

# See tasks_lib.Window.py for more details about how this class works
class TasksWindow(Window):
    __gtype_name__ = "TasksWindow"
    
    def finish_initializing(self, builder): # pylint: disable=E1002
        """Set up the main window"""
        super(TasksWindow, self).finish_initializing(builder)

        self._last_filter = None
        self._selected_task_box = None
        self._task_details_pane = None
        self._task_entries = {}
        
        self.AboutDialog = AboutTasksDialog
        self.PreferencesDialog = PreferencesTasksDialog
        self.NewTaskDialog = NewTaskDialog
 
        self._store = Store()
        self._store.register_model(Task)
        
        self._store.connect("post-save", self.store_post_save)
 
        # Code for other initialization actions should be added here.
        self._locate_and_update_user_image()
        #FIXME: Find the user's first name (or fallback to the unix login)
        self._start_task_loading()
        #self._update_tags()
        self._show_task_details(None)
        
        self.ui.sorting_combo.set_active(0)
        self.maximize()

    def _locate_and_update_user_image(self):
        import getpass
        username = getpass.getuser()

        image_path = None        
        if os.path.exists(os.path.join("/var/lib/AccountsService/users/", username)):
            #TODO: Open the desktop file and read the icon path
            config = ConfigParser.ConfigParser()
            config.read(os.path.join("/var/lib/AccountsService/users/", username))            
            image_path = config.get("User", "Icon")
        elif os.path.exists(os.path.join(os.path.expanduser("~"), ".face")):
            image_path = os.path.join(os.path.expanduser("~"), ".face")

        assert image_path #FIXME: Fallback...
        
        pixbuf = GdkPixbuf.Pixbuf.new_from_file_at_size(image_path, 64, 64)        
        self.ui.user_image.set_from_pixbuf(pixbuf)
                
    def _start_task_loading(self):
        self.active_button_toggled_cb(self.ui.active_button)
        self._update_uncompleted_count()
        
    def _show_task_details(self, task):
        child = self.ui.task_details_alignment.get_child()
        if child:
            self.ui.task_details_alignment.remove(child)    
            
        self._task_details_pane = None
        if task is None:
            #Just show there is no task selected
            label = Gtk.Label()
            label.set_markup('<span foreground="grey"><b>No task selected</b></span>')
            self.ui.task_details_alignment.add(label)
        else:   
            task = self._store.get(Task, task.pk)
                      
            #Display the task details
            self._task_details_pane = TaskDetailsPane(task, self)
            self.ui.task_details_alignment.add(self._task_details_pane)
                    
        self.ui.task_details_alignment.show_all()
        if self._task_details_pane:
            self._task_details_pane.hide_summary_edit() #Ugly, we shouldn't need to do this here
                   
    def _show_tag_selection(self, task):
        assert False
            
    def _display_tasks(self, queryset, focused_task=None):
        logging.debug("Redisplaying tasks")
        self._show_task_details(None)
        
        child = self.ui.task_list_alignment.get_child()
        if child:
            self.ui.task_list_alignment.remove(child)
        
        #If there are no tasks at all, urge the user to create some
        if not Task.objects.exists():
            label = Gtk.Label()
            label.set_markup("<span foreground=\"grey\">You haven't added any tasks yet\n (Go add some!)</span>")
            
            label.set_justify(Gtk.Justification.CENTER)
            
            self.ui.task_list_alignment.add(label)
            self.ui.task_list_alignment.show_all()
            return
        
        #If this particular filter has no tasks, display a different message
        if not queryset:
            label = Gtk.Label()
            label.set_markup("<span foreground=\"grey\">No tasks found </span>")
            
            label.set_justify(Gtk.Justification.CENTER)
            
            self.ui.task_list_alignment.add(label)
            self.ui.task_list_alignment.show_all()
            return        
        
        self._last_filter = queryset.all()
        
        task_box = Gtk.VBox()   
        self._task_entries = {}             
        for task in queryset.all():
            entry = TaskListEntry(task)
            
            if task.pk == focused_task:
                self.task_summary_clicked_cb(None, None, task, entry)
            
            self._task_entries[task.pk] = entry
            entry.connect("entry-selected", self.task_summary_clicked_cb, task, entry)
            entry.connect("task-complete-toggled", self.task_completed_button_cb)
            task_box.pack_start(entry, 0, True, False)

        task_box.pack_end(Gtk.Alignment(), 0, True, True)            
        self.ui.task_list_alignment.add(task_box)
        self.ui.task_list_alignment.show_all()
                    
    def show_alert(self, text):
        pass
    
    def _update_uncompleted_count(self):
        count = Task.objects_uncompleted.count()
        self.ui.uncompleted_count.set_text("(%d)" % count)

    def _update_tags(self):        
        child = self.ui.tags_alignment.get_child()
        if child:
            self.ui.tags_alignment.remove(child)
            
        #If there are no tags, then hide stuff
        if not Tag.objects.exists():
            self.ui.tags_label.set_visible(False)
            self.ui.tags_alignment.set_visible(False)
        else:
            self.ui.tags_alignment.set_visible(True)
            self.ui.tags_alignment.set_visible(True)
            
            vbox = Gtk.VBox()
            first_button = None
            
            for tag in Tag.objects.order_by("value").all():
                button = Gtk.RadioButton()
                button.set_text(tag.value)
                if first_button:
                    button.set_group(first_button.get_group())
                else:
                    first_button = button
                vbox.pack_start(button, 0, True, False)
                
            self.ui.tags_alignment.add(vbox)
            self.ui.tags_alignment.show_all()
                
    #=============== Handlers
    def new_task_button_pressed_cb(self, obj):     
        summary = self.ui.new_task_box.get_text().strip().decode("utf-8")
        if not summary:
            show_alert("You must enter some text to add a task")
        else:
            new_task = self._store.create(
                Task,
                summary=summary
            )
            self.ui.new_task_box.set_text("")
            if self._last_filter:
                self._display_tasks(self._last_filter, focused_task=new_task.pk)
            else:
                self._display_tasks(Task.objects_uncompleted.all(), focused_task=new_task.pk)
            self._update_uncompleted_count()                            

    def new_task_box_key_press_event_cb(self, obj, event):
        keyname = Gdk.keyval_name(event.keyval)
        if keyname == "Return":
            self.new_task_button_pressed_cb(None)
        
    def all_tasks_button_toggled_cb(self, obj):
        if obj.get_active():
            self._display_tasks(Task.objects.all())
    
    def active_button_toggled_cb(self, obj):
        if obj.get_active():
            self._display_tasks(Task.objects_uncompleted.all())    

    def completed_button_toggled_cb(self, obj):
        if obj.get_active():
            self._display_tasks(Task.objects_completed.all())
            
    def important_button_toggled_cb(self, obj):
        if obj.get_active():
            self._display_tasks(Task.objects_important.all())

    def due_today_button_toggled_cb(self, obj):
        if obj.get_active():
            self._display_tasks(Task.objects_due_today.all())  
            
    def overdue_button_toggled_cb(self, obj):
        if obj.get_active():
            self._display_tasks(Task.objects_overdue.all())                                

    def task_summary_clicked_cb(self, obj, event, data, row_event_box):
        if self._selected_task_box:
            self._selected_task_box.override_background_color(Gtk.StateType.NORMAL, None)
    
        self._show_task_details(data)
        
        style_context = self.get_style_context()
        colour = style_context.lookup_color("selected_bg_color")[1]
        
        row_event_box.override_background_color(Gtk.StateType.NORMAL, colour)
        self._selected_task_box = row_event_box

    def store_post_save(self, store, instance):
        if instance.pk in self._task_entries:
            self._task_entries[instance.pk].refresh()
        self._update_uncompleted_count()
        
    def task_completed_button_cb(self, obj, task, active):
        #If the task is currently showing in the details pane, update it there
        if self._task_details_pane and self._task_details_pane._task == task:
            self._task_details_pane.get_checkmark().set_active(active)
        else:
            #Just save it here
            task = self._store.get(Task, task.pk) #Reload                
            task.complete = not task.complete
            self._store.save(task)
            

    def task_details_saved_cb(self, widget, task):
        print "Saving task: " + task.details
        self._store.save(task)
        self._update_uncompleted_count()
        
        #FIXME: self._refresh_task_in_list
        #self._display_tasks(self._last_filter, focused_task=task.pk)
        
        
