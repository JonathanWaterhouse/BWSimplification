import sys, time, random
from PyQt4.QtCore import QThread, QEvent
from PyQt4.QtGui import QTextEdit, QApplication
__author__ = 'U104675'

rand = random.Random()
class WorkerThread(QThread):
  def __init__(self, name, receiver):
    QThread.__init__(self)
    self.name = name
    self.receiver = receiver
    self.stopped = 0
  def run(self):
    while not self.stopped:
      time.sleep(rand.random() * 0.3)
      msg = rand.random()
      event = QEvent(10000)
      event.setData("%s: %f" % (self.name, msg))
      QThread.postEvent(self.receiver, event)

  def stop(self):
    self.stopped = 1

class ThreadExample(QTextEdit):
  def __init__(self, *args):
    QTextEdit.__init__(self, *args)
    self.setText("Threading Example")
    self.threads = []
    for name in ["t1", "t2", "t3"]:
      t = WorkerThread(name, self)
      t.start()
      self.threads.append(t)

  def customEvent(self,event):
    if event.type() == 10000:
      s = event.data()
      self.append(s)

  def __del__(self):
    for t in self.threads:
      running = t.running()
      t.stop()
      if not t.finished():
        t.wait()


app = QApplication(sys.argv)
threadExample = ThreadExample()
#app.setMainWidget(threadExample)
threadExample.show()

sys.exit(app.exec_())
