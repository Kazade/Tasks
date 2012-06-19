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
from tasks.models import Task, Tag

import os
import ConfigParser

# See tasks_lib.Window.py for more details about how this class works
class TasksWindow(Window):
    __gtype_name__ = "TasksWindow"
    
    def finish_initializing(self, builder): # pylint: disable=E1002
        """Set up the main window"""
        super(TasksWindow, self).finish_initializing(builder)

        self._last_filter = None
        self._selected_task_box = None

        self.AboutDialog = AboutTasksDialog
        self.PreferencesDialog = PreferencesTasksDialog
        self.NewTaskDialog = NewTaskDialog
 
        # Code for other initialization actions should be added here.
        self._initialize_django()
        self._locate_and_update_user_image()
        #FIXME: Find the user's first name (or fallback to the unix login)
        self._start_task_loading()
        self._update_tags()
        self._show_task_details(None)
        
        self.ui.sorting_combo.set_active(0)

    def _initialize_django(self):
        #FIXME: initialize tables
        from django.core.management import call_command
        call_command("syncdb")

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
        self.all_tasks_button_toggled_cb(self.ui.all_tasks_button)
        self._update_uncompleted_count()
        
    def _show_task_details(self, task):
        child = self.ui.task_details_alignment.get_child()
        if child:
            self.ui.task_details_alignment.remove(child)    
            
        if task is None:
            #Just show there is no task selected
            label = Gtk.Label()
            label.set_markup('<span foreground="grey"><b>No task selected</b></span>')
            self.ui.task_details_alignment.add(label)
        else:             
            #Display the task details
            vbox = Gtk.VBox()
            
        
        self.ui.task_details_alignment.show_all()
            
        
    def _display_tasks(self, queryset):
        logging.debug("Redisplaying tasks")
        
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
        if not queryset.exists():
            label = Gtk.Label()
            label.set_markup("<span foreground=\"grey\">No tasks found :) </span>")
            
            label.set_justify(Gtk.Justification.CENTER)
            
            self.ui.task_list_alignment.add(label)
            self.ui.task_list_alignment.show_all()
            return        
        
        self._last_filter = queryset.all()
        
        task_box = Gtk.VBox()                
        for task in queryset.all():
            event_box = Gtk.EventBox() 
            
            hbox = Gtk.HBox()
            label = Gtk.Label()
            label.set_markup("<b>" + task.summary + "</b>")            
            
            event_box.add_events(Gdk.EventMask.BUTTON_PRESS_MASK | Gdk.EventMask.POINTER_MOTION_MASK)
            event_box.connect("button-press-event", self.task_summary_clicked_cb, task)
            
            
            if task.complete:
                #FIXME: Bad relative paths
                check_image = "./data/media/checked.png"
            else:
                check_image = "./data/media/unchecked.png"
            
            try:
                pixbuf = GdkPixbuf.Pixbuf.new_from_file_at_size(check_image, 48, 48)
                image = Gtk.Image.new_from_pixbuf(pixbuf)
            except:
                #FIXME: need to deal with this properly
                image = Gtk.Image()
                image.set_size_request(48, 48)
                        
            hbox.pack_start(image, 0, True, False)
            hbox.pack_start(label, 0, True, False)            
            event_box.add(hbox)
            task_box.pack_start(event_box, 0, True, False)

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
        summary = self.ui.new_task_box.get_text().strip()
        if not summary:
            show_alert("You must enter some text to add a task")
        else:
            Task.objects.create(
                summary=summary
            )
            self.ui.new_task_box.set_text("")
            if self._last_filter:
                self._display_tasks(self._last_filter)
            else:
                self._display_tasks(Task.objects_uncompleted.all())
            self._update_uncompleted_count()                

    def new_task_box_key_press_event_cb(self, obj, event):
        keyname = Gdk.keyval_name(event.keyval)
        if keyname == "Return":
            self.new_task_button_pressed_cb(None)
        
    def all_tasks_button_toggled_cb(self, obj):
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

    def task_summary_clicked_cb(self, obj, event, data):
        if self._selected_task_box:
            self._selected_task_box.override_background_color(Gtk.StateType.NORMAL, None)
    
        self._show_task_details(data)
        
        style_context = self.get_style_context()
        colour = style_context.lookup_color("selected_bg_color")[1]
        
        obj.override_background_color(Gtk.StateType.NORMAL, colour)
        self._selected_task_box = obj
        
