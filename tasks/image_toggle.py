from gi.repository import Gtk, GdkPixbuf, GObject, Gdk

class ImageToggle(Gtk.EventBox):
    __gsignals__ = {
        "toggled" : (GObject.SIGNAL_RUN_FIRST, None, (Gtk.Widget, ))
    }

    def __init__(self, unchecked_image, checked_image, *args, **kwargs):
        super(ImageToggle, self).__init__(*args, **kwargs)
        
        pixbuf = GdkPixbuf.Pixbuf.new_from_file_at_size(checked_image, 22, 22)
        image = Gtk.Image.new_from_pixbuf(pixbuf)
        image.set_padding(10, 10)
        
        self._checked_image = image
        
        pixbuf = GdkPixbuf.Pixbuf.new_from_file_at_size(unchecked_image, 22, 22)
        image = Gtk.Image.new_from_pixbuf(pixbuf)
        image.set_padding(10, 10)        
        
        self._unchecked_image = image

        self.add_events(Gdk.EventMask.BUTTON_PRESS_MASK | Gdk.EventMask.POINTER_MOTION_MASK)
        self.connect("button-press-event", self.button_pressed_cb)
        
        self.set_active(False)
        
    def set_active(self, value=True):
        child = self.get_child()
        if child:
            self.remove(child)
            
        self._checked = value
        if value:
            self.add(self._checked_image)
        else:
            self.add(self._unchecked_image) 
        
        self.set_visible_window(False)        
        self.show_all()            

    def get_active(self):
        return self._checked

    def button_pressed_cb(self, obj, event):
        self._checked = not self._checked
        self.set_active(self._checked)
        self.emit("toggled", self)
        
        
