import time
import datetime

from gi.repository import Gtk, Gdk, GObject

from .image_toggle import ImageToggle
from .store import Task

class TaskDetailsPane(Gtk.EventBox):
    __gtype_name__ = "TaskDetailsPane"
    
    __gsignals__ = {
        "save-requested" : (GObject.SIGNAL_RUN_FIRST, None, (object, ))
    }
    
    def __init__(self, task, window, *args, **kwargs):
        super(TaskDetailsPane, self).__init__(*args, **kwargs)

        self._task = task        
        self._time_checkbox = None
        self._hour_spinbutton = None
        self._minute_spinbutton = None
        self._initialize_widgets(task, window)                

    def hide_summary_edit(self):
        self._summary_edit.set_visible(False)
        self._summary_label_eb.set_visible(True)

    def notes_changed_callback(self, widget):    
        self._task.details = self._notes_box.get_buffer().get_text(
            self._notes_box.get_buffer().get_start_iter(),
            self._notes_box.get_buffer().get_end_iter(),
            True
        ).decode("utf-8")
        
        self._task.save()
 
    def get_checkmark(self):
        return self._checkmark
        
    def get_header(self):
        return self._header_eb
        
    def get_notes(self):
        return self._notes_box
    
    def summary_clicked_cb(self, obj, event):
        self._summary_label_eb.set_visible(False) #Hide the summary label
        self._summary_edit.set_visible(True) #Show the edit box
             
    def summary_edit_changed_cb(self, obj, event):
        keyname = Gdk.keyval_name(event.keyval)
        
        if keyname == "Return" or keyname == "Escape":
            if keyname == "Return":
                self._task.summary = self._summary_edit.get_text().decode("utf-8")
                self._task.save()
                self._summary_label.set_markup("<b>" + self._task.summary + "</b>")
                    
            self._summary_label_eb.set_visible(True)
            self._summary_edit.set_visible(False)
            
    def summary_edit_focus_out_cb(self, obj, event):
        #Just cancel the edit
        self._summary_edit.set_text(self._task.summary) #Reset the edit text
        
        self._summary_label_eb.set_visible(True)
        self._summary_edit.set_visible(False)    
                    
    def time_enabled_toggled_cb(self, obj):
        self._hour_spinbutton.set_sensitive(obj.get_active())
        self._minute_spinbutton.set_sensitive(obj.get_active())
        
        if obj.get_active():
            hours = int(self._hour_spinbutton.get_value())
            minutes = int(self._minute_spinbutton.get_value())
            self._task.due_time = datetime.datetime.strptime("%d:%d:00" % (hours, minutes), "%H:%M:%S")
        else:
            self._task.due_time = None
            
        self._task.save()

    def due_date_changed_cb(self, calendar):
        self._task.due_date = datetime.date(*calendar.get_date())
        self._task.save()
    
    def due_time_changed_cb(self, spinbutton):
        if self._hour_spinbutton and self._minute_spinbutton:
            hours = int(self._hour_spinbutton.get_value())
            minutes = int(self._minute_spinbutton.get_value())
            self._task.due_time = datetime.datetime.strptime("%d:%d:00" % (hours, minutes), "%H:%M:%S")
            self._task.save()
    
    def schedule_enabled_toggled_cb(self, obj):
        self._schedule_calendar.set_sensitive(obj.get_active())
        if self._time_checkbox:
            self._time_checkbox.set_sensitive(obj.get_active())
    
        if obj.get_active():
            self._task.due_date = datetime.date(*self._schedule_calendar.get_date())
            
            #If the time checkbox is active, then we must restore the time from there
            #so that unchecking and rechecking "set a deadline" works as you'd expect
            if self._time_checkbox and self._time_checkbox.get_active():
                hours = int(self._hour_spinbutton.get_value())
                minutes = int(self._minute_spinbutton.get_value())
                self._task.due_time = datetime.datetime.strptime("%d:%d:00" % (hours, minutes), "%H:%M:%S")
        else:
            self._task.due_date = None
            self._task.due_time = None
            
        self._task.save()
    
    def checkmark_toggled_cb(self, obj, event):
        self._task.complete = obj.get_active()
        self._task.save()
    
    def _initialize_widgets(self, task, window):
        from .TasksWindow import UNCHECKED_IMAGE, CHECKED_IMAGE

        #Display the task details
        vbox = Gtk.VBox()
        self.add(vbox)
        
        style_context = window.get_style_context()
        colour = style_context.lookup_color("selected_bg_color")[1]        
        vbox.override_background_color(Gtk.StateType.NORMAL, colour)
        vbox.set_margin_left(0)
                    
        self._header_eb = Gtk.EventBox()
        self._header_eb.override_background_color(Gtk.StateType.NORMAL, Gdk.RGBA(1.0, 1.0, 1.0, 1.0))
        self._header_eb.add_events(Gdk.EventMask.BUTTON_PRESS_MASK | Gdk.EventMask.POINTER_MOTION_MASK)
                                
        header_hbox = Gtk.HBox()
        self._checkmark = ImageToggle(UNCHECKED_IMAGE, CHECKED_IMAGE)
        self._checkmark.set_active(task.complete)
        self._checkmark.connect("toggled", self.checkmark_toggled_cb)
        header_hbox.pack_start(self._checkmark, 0, False, False)
        
        header_hbox.set_margin_top(10)
        header_hbox.set_margin_bottom(10)
        
        self._summary_label_eb = Gtk.EventBox()
        self._summary_label_eb.add_events(Gdk.EventMask.BUTTON_PRESS_MASK | Gdk.EventMask.POINTER_MOTION_MASK)
        self._summary_label_eb.connect("button-press-event", self.summary_clicked_cb)
        self._summary_label_eb.set_visible_window(False)
        
        self._summary_label = Gtk.Label()
        self._summary_label.set_markup("<b>" + task.summary + "</b>")
        self._summary_label.set_padding(0, 5)
        self._summary_label.set_line_wrap(True)                    
        self._summary_label_eb.add(self._summary_label)
        
        self._summary_edit = Gtk.Entry()
        self._summary_edit.set_size_request(400, -1)
        self._summary_edit.set_text(task.summary)
        self._summary_edit.connect("key-press-event", self.summary_edit_changed_cb)
        self._summary_edit.connect("focus-out-event", self.summary_edit_focus_out_cb)

        label_and_tags_box = Gtk.VBox()
        label_and_tags_box.pack_start(self._summary_edit, 0, True, False)
        label_and_tags_box.pack_start(self._summary_label_eb, 0, True, False)
        
        if False:
            #TODO: List tags
            pass
        else:
            tag_label = Gtk.Label()
            tag_label.set_markup('<span foreground="grey">Add tags</span>')
            tag_label.set_halign(Gtk.Align.START)
            tag_label.set_justify(Gtk.Justification.LEFT)
            label_and_tags_box.pack_start(tag_label, 0, True, False)
        
        header_hbox.pack_start(label_and_tags_box, 0, True, False)
        
#            archive_button = Gtk.Button("X")
#            archive_button.set_margin_right(5)
#            header_hbox.pack_end(archive_button, 0, True, False)            
        
        self._header_eb.add(header_hbox)
        vbox.pack_start(self._header_eb, 0, True, False)
    
        details_label = Gtk.Label()
        details_label.set_markup("<b>" + "Notes" + "</b>")
        details_label.set_justify(Gtk.Justification.LEFT)
        details_label.set_halign(Gtk.Align.START)
        details_label.set_margin_bottom(10)
        
        self._notes_box = Gtk.TextView()
        self._notes_box.get_buffer().set_text(task.details)
        self._notes_box.set_left_margin(2)
        self._notes_box.set_right_margin(2)
        self._notes_box.set_pixels_above_lines(2)
        self._notes_box.set_pixels_below_lines(2)
        self._notes_box.get_buffer().connect("changed", self.notes_changed_callback)
        self._notes_box.set_wrap_mode(Gtk.WrapMode.WORD)
        
        notes_scrolled_window = Gtk.ScrolledWindow()
        notes_scrolled_window.add_with_viewport(self._notes_box)
        notes_scrolled_window.set_size_request(-1, 50)
        
        details_vbox = Gtk.VBox()
        details_vbox.pack_start(details_label, 0, True, False)
        details_vbox.pack_start(notes_scrolled_window, 0, True, False)    

        details_vbox.set_margin_left(50)
        details_vbox.set_margin_top(10)
        details_vbox.set_margin_bottom(20)
        details_vbox.set_margin_right(50)
        
        vbox.pack_start(details_vbox, 0, True, False)
        vbox.pack_start(Gtk.Separator(), 0, True, True)

        schedule_label = Gtk.Label()
        schedule_label.set_markup("<b>" + "Deadline" + "</b>")
        schedule_label.set_justify(Gtk.Justification.LEFT)
        schedule_label.set_halign(Gtk.Align.START)
        schedule_label.set_margin_bottom(10)
        
        schedule_vbox = Gtk.VBox()
        schedule_vbox.pack_start(schedule_label, 0, True, False)
        
        schedule_enabled = Gtk.CheckButton()
        schedule_enabled.set_label("Set a deadline?")
        schedule_enabled.set_margin_bottom(20)
        schedule_enabled.connect("toggled", self.schedule_enabled_toggled_cb)
        
        self._schedule_calendar = Gtk.Calendar()
        if self._task.due_date:
            self._schedule_calendar.select_month(self._task.due_date.month, self._task.due_date.year)
            self._schedule_calendar.select_day(self._task.due_date.day)
        
        self._schedule_calendar.connect("day-selected", self.due_date_changed_cb)
        
        schedule_enabled.set_active(self._task.due_date is not None)
        schedule_enabled.emit("toggled")
        
        schedule_vbox.pack_start(schedule_enabled, 0, True, False)
        
        
        schedule_hbox = Gtk.HBox()
        schedule_hbox.pack_start(self._schedule_calendar, 0, False, False)

        time_selector = Gtk.HBox()
        time_selector.set_margin_left(50)
        time_selector.set_margin_top(10)

        def show_leading_zeros(spin_button):
            adjustment = spin_button.get_adjustment()
            spin_button.set_text('{:02d}'.format(int(adjustment.get_value())))
            return True
                                
        self._hour_spinbutton = Gtk.SpinButton()
        self._hour_spinbutton.set_range(0, 23)
        self._hour_spinbutton.connect("output", show_leading_zeros)
        self._hour_spinbutton.set_margin_left(2)
        self._hour_spinbutton.set_margin_right(10)
        self._hour_spinbutton.set_increments(1, 0)
        self._hour_spinbutton.set_wrap(True)
        self._hour_spinbutton.connect("value-changed", self.due_time_changed_cb)
    
        if self._task.due_time:
            self._hour_spinbutton.set_value(int(self._task.due_time.strftime("%H")))
                
        time_selector.pack_start(Gtk.Label("Hours:"), 5, True, False)
        time_selector.pack_start(self._hour_spinbutton, 5, True, False)
        
        self._minute_spinbutton = Gtk.SpinButton()
        self._minute_spinbutton.set_range(0, 59)
        self._minute_spinbutton.connect("output", show_leading_zeros)            
        self._minute_spinbutton.set_margin_left(2)
        self._minute_spinbutton.set_margin_right(10)
        self._minute_spinbutton.set_increments(1, 0)
        self._minute_spinbutton.set_wrap(True)        
        self._minute_spinbutton.connect("value-changed", self.due_time_changed_cb)

        if self._task.due_time:
            self._minute_spinbutton.set_value(int(self._task.due_time.strftime("%M")))
                                                
        time_selector.pack_start(Gtk.Label("Minutes:"), 5, True, False)
        time_selector.pack_start(self._minute_spinbutton, 5, True, False)            
                
        time_vbox = Gtk.VBox()
        self._time_checkbox = Gtk.CheckButton()
        self._time_checkbox.set_label("Set a time?")
        self._time_checkbox.set_margin_left(20)                        
        self._time_checkbox.set_active(self._task.due_time is not None)
        self._time_checkbox.set_sensitive(schedule_enabled.get_active())
        self._time_checkbox.connect("toggled", self.time_enabled_toggled_cb)                        
        
        #Make the hours and minutes sensitive to the time checkbox
        self._hour_spinbutton.set_sensitive(self._time_checkbox.get_active())
        self._minute_spinbutton.set_sensitive(self._time_checkbox.get_active())
        
        time_vbox.pack_start(self._time_checkbox, 0, True, False)
        time_vbox.pack_start(time_selector, 0, True, False)
        
        schedule_hbox.pack_start(time_vbox, 0, True, False)
        
        schedule_vbox.pack_start(schedule_hbox, 0, True, False)    

        schedule_vbox.set_margin_left(50)
        schedule_vbox.set_margin_top(10)
        schedule_vbox.set_margin_bottom(20)
        schedule_vbox.set_margin_right(5)
        
        vbox.pack_start(schedule_vbox, 0, True, False)
        vbox.pack_start(Gtk.Separator(), 0, True, True)    
        
    
