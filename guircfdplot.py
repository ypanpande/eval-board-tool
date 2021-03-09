
import tkinter as tk
from tkinter import ttk, filedialog
from readrcfd_V05_class import Read_rcfd
from tkinter import messagebox as msg
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
#import matplotlib.pyplot as plt
import pandas as pd
import os, ntpath
from collections import defaultdict
import matplotlib.dates as mdates

def main():

    root = tk.Tk()
    GuiRcfdPlot(root)
    root.mainloop()
    



class GuiRcfdPlot:
    def __init__(self, root):
        self.root = root
        self.root.title('Rcfd Convert and Plot Panel')	
        self.root.geometry("760x570+30+30") 	
        self.root.resizable(0,0)
        self.default_source_file = " "
        self.default_goal_path =" "	 
        
        self.default_csv_file = " "
        
        self.s = ttk.Style()
        self.s.configure('TLabelframe.Label', font = 'arial 12 bold')
        self.s.configure('TButton', font = 'Helvetica 10 bold')
        self.init_GuiRcfd()
        self.init_GuiPlot()
        
      
    
    def init_GuiRcfd(self):
        self.GuiRcfdFrame = ttk.LabelFrame(self.root, text = 'Rcfd to CSV Files:', width = 730, height = 300, relief = 'raised', borderwidth = 5)
        self.GuiRcfdFrame.place(x = 20, y = 10, width = 720, height = 290)
        
        bsource = ttk.Button(self.GuiRcfdFrame, text = "Rcfd Files", command = self.get_source_file)
        bsource.place(x = 10, y = 40, width = 100, height = 50)		
        
        self.lsource = ttk.Label(self.GuiRcfdFrame, text = self.default_source_file,  wraplength = 575, background = "yellow")
        self.lsource.place(x = 120, y = 40, width = 580, height = 50)		
        
        bgoal = ttk.Button(self.GuiRcfdFrame, text = "Result Folder", command = self.get_goal_path)
        bgoal.place(x = 10, y = 120, width = 100, height = 50)		
        
        self.lgoal = ttk.Label(self.GuiRcfdFrame, text = self.default_goal_path,  wraplength = 575, background = "yellow")
        self.lgoal.place(x = 120, y = 120, width = 580, height = 50)	
        
        self.bconvert = ttk.Button(self.GuiRcfdFrame, text = "Convert", command = self.file_convert)
        self.bconvert.place(x = 300, y = 200, width = 100, height = 50)
        
    def init_GuiPlot(self):
        self.GuiPlotFrame = ttk.LabelFrame(self.root, text = 'Plot CSV Files:', width = 730, height = 230, relief = 'raised', borderwidth = 5)
        self.GuiPlotFrame.place(x = 20, y = 330, width = 720, height = 220)
        
        bsourcecsv = ttk.Button(self.GuiPlotFrame, text = "CSV Files", command = self.get_csv_file)
        bsourcecsv.place(x = 10, y = 40, width = 100, height = 50)		
        
        self.lsourcecsv = ttk.Label(self.GuiPlotFrame, text = self.default_csv_file, wraplength = 575, background = "yellow")
        self.lsourcecsv.place(x = 120, y = 40, width = 580, height = 50)		
        
        self.bplot = ttk.Button(self.GuiPlotFrame, text = "Plot", command = self.csv_plots)
        self.bplot.place(x = 300, y = 120, width = 100, height = 50)

    def get_source_file(self):
        root = tk.Tk()
        root.withdraw()
        self.default_source_file = filedialog.askopenfilenames(initialdir = '/', title = 'Select rcfd files', filetypes = (('rcfd files', '*.rcfd'), ('All files', '*.*')))
        print('Rcfd files:', self.default_source_file)
        self.lsource['text'] = self.default_source_file
        
    def get_goal_path(self):
        root = tk.Tk()
        root.withdraw()
        self.default_goal_path = filedialog.askdirectory()
        self.lgoal['text'] = self.default_goal_path

    def file_convert(self):
        if len(self.default_source_file) == 1:
            filename = ntpath.basename(self.default_source_file[0])[0: -5]
        elif len(self.default_source_file) > 1:
            filename = ntpath.basename(self.default_source_file[0])[0: -5] + '_' + str(len(self.default_source_file)) + 'files'

        dict_df = defaultdict(pd.DataFrame)
        for file in self.default_source_file:
            rcfd = Read_rcfd(file)
            rcfd_dict = rcfd.get_all_data_OD2()
            for k in rcfd_dict.keys():
                dict_df[k] = dict_df[k].append(rcfd_dict[k])
        
        for key in dict_df.keys():
            dict_df[key].to_csv(os.path.join(self.default_goal_path, 'OD2_'+ key + '_' + filename + '.csv'), decimal = ',', sep = ';', index = False)
#        rcfd = Read_rcfd(self.default_source_file)
#        rcfd.OD2_write_to_csv(self.default_goal_path)
        msg.showinfo(title = 'file convert finished', message = 'Chosen Rcfd file has been converted to csv files!')
    
    def get_csv_file(self):
        root = tk.Tk()
        root.withdraw()
        self.default_csv_file = filedialog.askopenfilenames(initialdir = '/', title = 'Select csv file', filetypes = (('csv files', '*.csv'), ('All files', '*.*')))
        print('CSV files: ', self.default_csv_file)
        self.lsourcecsv['text'] = self.default_csv_file
        
    def csv_plots(self):
        for file in self.default_csv_file:
            self.csv_plot(file)
            
    def csv_plot(self, csv_file):
        path, filename = os.path.split(csv_file)
        df = pd.read_csv(csv_file, sep = ';', decimal = ',', header = 0)
        cols = list(df.columns)
        colnum = len(df.columns)
        df['time'] = pd.to_datetime(df[cols[0]], format = '%d.%m.%Y %H:%M:%S.%f')
        
        time_fmt = mdates.DateFormatter('%H:%M:%S')
        self.af = Figure(figsize=(16, 8.5), dpi=90)
        if colnum == 3:
            axes = self.af.subplots(2,1,sharex = True)
            axes[0].plot(df['time'], df[cols[1]], label = cols[1])
            axes[0].set_ylabel(cols[1] + '_value', fontsize = 14)
            axes[0].legend()
            axes[0].grid()
            axes[0].tick_params(labelsize = 12)
            axes[1].plot(df['time'], df[cols[2]], label = cols[2])
            axes[1].set_ylabel(cols[2] + '_value', fontsize = 14)
            axes[1].set_xlabel('Time', fontsize = 14)
            axes[1].xaxis.set_major_formatter(time_fmt)
            axes[1].legend()
            axes[1].grid()
            axes[1].tick_params(labelsize = 12)
        elif colnum == 4:
            axes = self.af.subplots(3,1,sharex = True)
            axes[0].plot(df['time'], df[cols[1]], label = cols[1])
            axes[0].set_ylabel(cols[1] + '_value', fontsize = 14)
            axes[0].legend()
            axes[0].grid()  
            axes[0].tick_params(labelsize = 14)
            axes[1].plot(df['time'], df[cols[2]], label = cols[2])
            axes[1].set_ylabel(cols[2] + '_value', fontsize = 14)
            axes[1].legend()
            axes[1].grid()
            axes[1].tick_params(labelsize = 12)
            axes[2].plot(df['time'], df[cols[3]], label = cols[3])
            axes[2].set_ylabel(cols[3] + '_value', fontsize = 14)
            axes[2].set_xlabel('Time', fontsize = 14)   
            axes[2].xaxis.set_major_formatter(time_fmt)
            axes[2].legend()
            axes[2].grid()
            axes[2].tick_params(labelsize = 12)
        self.af.autofmt_xdate()
        self.af.tight_layout(pad = 4)
        self.af.suptitle(filename, fontsize = 15)

        #aa = self.af.add_subplot(111)
        

        csvplot = tk.Toplevel(self.root)
        csvplot.title('Plot of csv file')
        csvplot.geometry('980x800+30+30')
    
        canvas_frame = ttk.Frame(csvplot)
        canvas_frame.pack(side=tk.TOP,fill=tk.BOTH, expand=1)
        canvas = FigureCanvasTkAgg(self.af, master=canvas_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        toolbar_frame = tk.Frame(csvplot)
    #        self.toolbar_frame.place(x = 5, y = 950 , width = 1540, height = 45)
        toolbar_frame.pack(fill=tk.BOTH, expand=1)
        toolbar = NavigationToolbar2Tk(canvas, toolbar_frame)
        toolbar.update()

    
    
if __name__ == '__main__': main()