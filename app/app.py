import tkMessageBox
import webbrowser
from Tkinter import *

from analysis import maxintensity
from etl.extractcsv import etl


class GUI(Frame):
    def __init__(self, master=None):

        Frame.__init__(self, master)
        self.master.title("BigUtrecht Tiny GUI")

        self.buttons = []

        self.ETLButton = Button(self, text="Download and Extract")
        self.ETLButton["command"] = self.runETL
        self.ETLButton.grid(row=0, column=0)

        self.buttons.append(self.ETLButton)

        self.map1Button = Button(self, text="Show Maximum Intensity Map")
        self.map1Button["command"] = self.showMap1
        self.map1Button.grid(row=1, column=0)

        self.map2Button = Button(self, text="Show Average Rush Hour Intensity Map")
        self.map2Button.grid(row=2, column=0)

        self.buttons.append(self.map1Button)
        self.buttons.append(self.map2Button)

        self.grid()

    def enableAll(self):
        for button in self.buttons:
            button["state"] = "normal"

    def disableAll(self):
        for button in self.buttons:
            button["state"] = "disabled"

    def runETL(self):
        self.disableAll()
        etl()
        tkMessageBox.showinfo("ETL Succeeded!")
        self.enableAll()

    def showMap1(self):
        self.disableAll()
        maxintensity.maxPointAnalysis()
        webbrowser.open("/tmp/map_max_intensity.html")
        self.enableAll()


if __name__ == "__main__":
    gui = GUI()
    gui.mainloop()
