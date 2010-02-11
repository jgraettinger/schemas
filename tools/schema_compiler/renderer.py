
class CodeRenderer(object):
    def __init__(self, indent = 0, indent_txt = '    '):
        self.parts = []
        self.indtxt = indent_txt
        self.cur_ind = indent
        return
    
    def line(self, txt = ''):
        self.parts.append( '%s%s' % (
            self.indtxt * self.cur_ind,
            txt,
        ))
        return self
    
    def lines(self, txt, strip_len, **kwargs):
        if kwargs:
            txt = txt % kwargs
        
        for l in txt.split('\n')[1:]:
            self.line(l.lstrip().rjust(len(l) - strip_len))
        return self
    
    def line_append(self, txt):
        self.parts[-1] = self.parts[-1] + txt
        return self
    
    def unputc(self):
        self.parts[-1] = self.parts[-1][:-1]
        return self
    
    def indent(self):
        self.cur_ind += 1
        return self
    
    def deindent(self):
        self.cur_ind -= 1
        return self
    
    def render(self):
        return '\n'.join(self.parts)


