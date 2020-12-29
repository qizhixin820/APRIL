
class MyLogger:
    def __init__(self, path = '../data/log.txt'):
        self.f = open(path, 'w')
    def write(self, val_list):
        for val in val_list:
            self.f.write(str(val))
            self.f.flush()
    def close(self):
        self.f.close()