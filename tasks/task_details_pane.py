from gi.repository import Gtk, Gdk

from .image_toggle import ImageToggle

class TaskDetailsPane(Gtk.EventBox):
    def __init__(self, task, window, *args, **kwargs):
        super(TaskDetailsPane, self).__init__(*args, **kwargs)
        
        self._initialize_widgets(task, window)
        
        
    def get_checkmark(self):
        return self._checkmark
        
    def get_header(self):
        return self._header_eb
        
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
        header_hbox.pack_start(self._checkmark, 0, False, False)
        
        header_hbox.set_margin_top(10)
        header_hbox.set_margin_bottom(10)
        
        label = Gtk.Label()
        label.set_markup("<b>" + task.summary + "</b>")
        label.set_padding(0, 5)
        label.set_line_wrap(True)            
        
        label_and_tags_box = Gtk.VBox()
        label_and_tags_box.pack_start(label, 0, True, False)
        
        if task.tags.exists():
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
        details_box = Gtk.TextView()
        details_box.set_left_margin(2)
        details_box.set_right_margin(2)
        details_box.set_pixels_above_lines(2)
        details_box.set_pixels_below_lines(2)
        
        notes_scrolled_window = Gtk.ScrolledWindow()
        notes_scrolled_window.add_with_viewport(details_box)
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
        
        schedule_calendar = Gtk.Calendar()
        schedule_hbox = Gtk.HBox()
        schedule_hbox.pack_start(schedule_calendar, 0, False, False)
        
        time_vbox = Gtk.VBox()
        time_checkbox = Gtk.CheckButton()
        time_checkbox.set_label("Set a time?")
        time_checkbox.set_margin_left(20)                        
        time_vbox.pack_start(time_checkbox, 0, True, False)
        
        def show_leading_zeros(spin_button):
            adjustment = spin_button.get_adjustment()
            spin_button.set_text('{:02d}'.format(int(adjustment.get_value())))
            return True
            
        time_selector = Gtk.HBox()
        time_selector.set_margin_left(50)
        time_selector.set_margin_top(10)
        
        hour_spinbutton = Gtk.SpinButton()
        hour_spinbutton.set_range(0, 23)
        hour_spinbutton.connect("output", show_leading_zeros)
        hour_spinbutton.set_margin_left(2)
        hour_spinbutton.set_margin_right(10)
        hour_spinbutton.set_increments(1, 0)
        hour_spinbutton.set_wrap(True)
        
        time_selector.pack_start(Gtk.Label("Hours:"), 5, True, False)
        time_selector.pack_start(hour_spinbutton, 5, True, False)
        
        minute_spinbutton = Gtk.SpinButton()
        minute_spinbutton.set_range(0, 59)
        minute_spinbutton.connect("output", show_leading_zeros)            
        minute_spinbutton.set_margin_left(2)
        minute_spinbutton.set_margin_right(10)
        minute_spinbutton.set_increments(1, 0)
        minute_spinbutton.set_wrap(True)
                    
        time_selector.pack_start(Gtk.Label("Minutes:"), 5, True, False)
        time_selector.pack_start(minute_spinbutton, 5, True, False)            
        time_vbox.pack_start(time_selector, 0, True, False)
        
        schedule_hbox.pack_start(time_vbox, 0, True, False)
        
        schedule_vbox.pack_start(schedule_hbox, 0, True, False)    

        schedule_vbox.set_margin_left(50)
        schedule_vbox.set_margin_top(10)
        schedule_vbox.set_margin_bottom(20)
        schedule_vbox.set_margin_right(5)
        
        vbox.pack_start(schedule_vbox, 0, True, False)
        vbox.pack_start(Gtk.Separator(), 0, True, True)    
    
