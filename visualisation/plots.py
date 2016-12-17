import matplotlib.pyplot as plt

from visualisation.loading import getFlowForLocation


def showFlowPlot(location):
    pdflow = getFlowForLocation(location)
    pdflow.plot(x="Tijd", y=["Flow", "Inflow", "Outflow"], kind='line', color=['green', 'red', 'blue'])
    plt.show()
