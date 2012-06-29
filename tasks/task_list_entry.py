from gi.repository import Gtk, GObject, Gdk
from .image_toggle import ImageToggle

class TaskListEntry(Gtk.EventBox):
    __gtypename__ = "TaskListEntry"
    
    __gsignals__ = {
        "task-complete-toggled" : (GObject.SIGNAL_RUN_FIRST, None, (object, bool)),
        "entry-selected" : (GObject.SIGNAL_RUN_FIRST, None, (object, )),
    }
    
    def __init__(self, task, *args, **kwargs):
        super(TaskListEntry, self).__init__(*args, **kwargs)
        self._task = task
        self.initialize_widgets()
        
    def checkbox_toggled_cb(self, obj, event):
        self.emit("task-complete-toggled", self._task, obj.get_active())
    
    def summary_clicked_cb(self, obj, event):
        self.emit("entry-selected", self._task)
    
    def refresh(self):
        self._task = self._task.reload()
        self._label.set_markup("<b>" + self._task.summary + "</b>")
        self._checkbox.set_active(self._task.complete)
        
#        TODO: update the summary and checkbox state
#        trigger this from the post save signal in TasksWindow
                
    def initialize_widgets(self):
        from .TasksWindow import UNCHECKED_IMAGE, CHECKED_IMAGE
        
        child = self.get_child()
        if child:
            self.remove(child)
            
        self.set_border_width(1)
        
        hbox = Gtk.HBox()
        
        label_eb = Gtk.EventBox()
        
        self._label = Gtk.Label()
        self._label.set_markup("<b>" + self._task.summary + "</b>")          
        self._label.set_line_wrap(True)  
        self._label.set_justify(Gtk.Justification.LEFT)
        self._label.set_halign(0.0)
        label_eb.add(self._label)
        
        label_eb.add_events(Gdk.EventMask.BUTTON_PRESS_MASK | Gdk.EventMask.POINTER_MOTION_MASK)
        label_eb.connect("button-press-event", self.summary_clicked_cb)
        label_eb.set_visible_window(False)

        self._checkbox = ImageToggle(UNCHECKED_IMAGE, CHECKED_IMAGE)
        self._checkbox.set_active(self._task.complete)
        self._checkbox.connect("toggled", self.checkbox_toggled_cb)            
        self._checkbox.set_size_request(48, 48)
                            
        hbox.pack_start(self._checkbox, 0, False, False)
        hbox.pack_start(label_eb, 0, True, False)                        
        self.add(hbox)            
    
