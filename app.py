import tkMessageBox
import webbrowser
from Tkinter import *
from ttk import Separator

from analysis import maxintensity
from analysis.averagerush import avgRushAnalysis
from etl.extractcsv import etl
from visualisation.loading import getLocations
from visualisation.plots import showFlowPlot


class GUI(Frame):
    def __init__(self, master=None):
        self.locaties = getLocations()
        # self.locaties=['a', 'bbbb', 'c', 'ddddddddddddd', 'e', 'ff', 'ggggggggggggggggggggggggggg']
        Frame.__init__(self, master)
        self.master.title("BigUtrecht Tiny GUI")

        self.buttons = []

        self.ETLButton = Button(self, text="Download and Extract")
        self.ETLButton["command"] = self.runETL
        self.ETLButton.grid(row=0, column=0, sticky=W + E)

        self.buttons.append(self.ETLButton)

        self.map1Button = Button(self, text="Show Maximum Intensity Map")
        self.map1Button["command"] = self.showMap1
        self.map1Button.grid(row=1, column=0, sticky=W + E)

        self.map2Button = Button(self, text="Show Average Rush Hour Intensity Map")
        self.map2Button["command"] = self.showMap2
        self.map2Button.grid(row=2, column=0, sticky=W + E)

        Separator(self, orient=VERTICAL).grid(row=0, column=1, rowspan=3, sticky=N + S)

        self.locatieScrollbar = Scrollbar(self)
        self.locatieList = Listbox(self, yscrollcommand=self.locatieScrollbar.set, height=3)
        for locatie in self.locaties:
            self.locatieList.insert(END, locatie)
        self.locatieScrollbar['command'] = self.locatieList.yview
        self.locatieList.grid(row=0, column=2, rowspan=2, sticky=N + S + E + W)
        self.locatieScrollbar.grid(row=0, column=3, rowspan=2, sticky=N + S + E)

        self.flowButton = Button(self, text="Show Average Flow for Location")
        self.flowButton['command'] = self.showFlow
        self.flowButton.grid(row=2, column=2, columnspan=2, sticky=W + E)

        self.buttons.append(self.map1Button)
        self.buttons.append(self.map2Button)
        self.buttons.append(self.locatieList)
        self.buttons.append(self.flowButton)

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

    def showMap2(self):
        self.disableAll()
        avgRushAnalysis()
        webbrowser.open("/tmp/map_avg_rush.html")
        self.enableAll()

    def showFlow(self):
        location = self.locaties[self.locatieList.curselection()]
        showFlowPlot(location)


if __name__ == "__main__":
    gui = GUI()
    gui.mainloop()
