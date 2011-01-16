import sys
import sqlite3

from PyQt4 import QtGui


class Browser(object):
    def __init__(self):
        self.widget = QtGui.QWidget()
        layout = QtGui.QVBoxLayout()
        self.widget.setLayout(layout)
        self.widget.resize(1000, 600)
        self.widget.setWindowTitle('CV browser')
        
        self.textView = QtGui.QTextEdit()
        layout.addWidget(self.textView)
        
        buttonWidget = QtGui.QWidget()
        layout.addWidget(buttonWidget)
        
        buttonLayout = QtGui.QHBoxLayout()
        buttonWidget.setLayout(buttonLayout)
        
        
        nextButton = QtGui.QPushButton('Next')
        yesButton = QtGui.QPushButton( 'Is CV' )
        noButton = QtGui.QPushButton( 'Not CV' )
        buttonLayout.addWidget(nextButton)
        buttonLayout.addWidget(yesButton)
        buttonLayout.addWidget(noButton)
        
        dbfile = 'cvscrape.sqlite3'
        self.conn = sqlite3.connect( dbfile, detect_types=sqlite3.PARSE_DECLTYPES )
        self.textQuery = self.conn.execute( "SELECT t1.id, t1.contents FROM cvText AS t1 LEFT JOIN isCV AS t2 ON t1.id=t2.cvId WHERE t2.isCV IS NULL" )
        self.id, self.text = self.textQuery.fetchone()
        self.textView.setText( self.text )
        
        nextButton.clicked.connect( self.getNext )
        yesButton.clicked.connect( lambda : self.setStatus( True ) )
        noButton.clicked.connect( lambda : self.setStatus( False ) )
        self.widget.show()

    def getNext( self ):
        self.id, self.text = self.textQuery.fetchone()
        self.textView.setText( self.text )

    def setStatus( self, value ):
        self.conn.execute( "INSERT INTO isCV VALUES(NULL, ?, ?)", [self.id, 1 if value else 0] )
        self.conn.commit()
        self.getNext()

        
if __name__ == '__main__':
    app = QtGui.QApplication(sys.argv)
    b = Browser()
    sys.exit(app.exec_())

