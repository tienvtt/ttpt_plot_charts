from _0_Dependencies import *

# CODE_FOLDER = r'D:\HungNTG\0_TTPT\3_Master_Loop'
CODE_FOLDER = os.getcwd()

SHAREPOINT_FOLDER = r'C:\Users\hung.ntg\OneDrive - CONG TY CO PHAN CHUNG KHOAN RONG VIET\INFOSNAP CHANNEL - FA_Baocaotomtat'
SHAREPOINT_BEGIN_URL = r'https://rongvietcorp.sharepoint.com/sites/INFOSNAPCHANNEL/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FINFOSNAPCHANNEL%2FShared%20Documents%2FFA_Baocaotomtat%2F'
SHAREPOINT_END_URL = r'%2Epdf&viewid=49ba1302-326f-47c8-ac90-7707b2ed5ad5&parent=%2Fsites%2FINFOSNAPCHANNEL%2FShared%20Documents%2FFA_Baocaotomtat'

dll_folder_map = ['Lib', 'site-packages', 'clidriver', 'bin']

# DATABASE PARAMETERS
db2_database_dict = {
	'user': "tvcn",
	'password': 'd26cN9wB',
	'host': "192.168.3.99",
	'port': "50000",
	'db': "PRORAW",
	'schema': "RAWTVCN",
}

mysql_database_dict = {
	'user': "HUNG.NTG",
	'password': "GiaHung@56785678",
	'host': "192.168.9.72",
	'port': "3306",
	'db': 'TTPT',
	'schema': None,
}

data_level = ['world','country','macro','industry','company','stock', 'finance']

PRINT_OUT = False

COLOR_DICT = {
	'black': (40,40,40),
	'white': (255,255,255),
    'grey': (200,200,200),
    'red': (237, 52, 46),
    'green': (1, 165, 78), 
    'yellow': (220, 171, 39),
    'dark_yellow': (127, 90, 61),
    'light_yellow': (237, 229, 151),
}

# ///////////////////////////////////////////////////////////////////////////
# SOURCE: https://learn.microsoft.com/en-us/office/vba/api/excel.xlcharttype
vba_chart_num_dict = {
    'xl3DArea': -4098, #3D Area.
    'xl3DAreaStacked': 78, #3D Stacked Area.
    'xl3DAreaStacked100': 79, #100% Stacked Area.
    'xl3DBarClustered': 60, #3D Clustered Bar.
    'xl3DBarStacked': 61, #3D Stacked Bar.
    'xl3DBarStacked100': 62, #3D 100% Stacked Bar.
    'xl3DColumn': -4100, #3D Column.
    'xl3DColumnClustered': 54, #3D Clustered Column.
    'xl3DColumnStacked': 55, #3D Stacked Column.
    'xl3DColumnStacked100': 56, #3D 100% Stacked Column.
    'xl3DLine': -4101, #3D Line.
    'xl3DPie': -4102, #3D Pie.
    'xl3DPieExploded': 70, #Exploded 3D Pie.
    'xlArea': 1, #Area
    'xlAreaStacked': 76, #Stacked Area.
    'xlAreaStacked100': 77, #100% Stacked Area.
    'xlBarClustered': 57, #Clustered Bar.
    'xlBarOfPie': 71, #Bar of Pie.
    'xlBarStacked': 58, #Stacked Bar.
    'xlBarStacked100': 59, #100% Stacked Bar.
    'xlBubble': 15, #Bubble.
    'xlBubble3DEffect': 87, #Bubble with 3D effects.
    'xlColumnClustered': 51, #Clustered Column.
    'xlColumnStacked': 52, #Stacked Column.
    'xlColumnStacked100': 53, #100% Stacked Column.
    'xlConeBarClustered': 102, #Clustered Cone Bar.
    'xlConeBarStacked': 103, #Stacked Cone Bar.
    'xlConeBarStacked100': 104, #100% Stacked Cone Bar.
    'xlConeCol': 105, #3D Cone Column.
    'xlConeColClustered': 99, #Clustered Cone Column.
    'xlConeColStacked': 100, #Stacked Cone Column.
    'xlConeColStacked100': 101, #100% Stacked Cone Column.
    'xlCylinderBarClustered': 95, #Clustered Cylinder Bar.
    'xlCylinderBarStacked': 96, #Stacked Cylinder Bar.
    'xlCylinderBarStacked100': 97, #100% Stacked Cylinder Bar.
    'xlCylinderCol': 98, #3D Cylinder Column.
    'xlCylinderColClustered': 92, #Clustered Cone Column.
    'xlCylinderColStacked': 93, #Stacked Cone Column.
    'xlCylinderColStacked100': 94, #100% Stacked Cylinder Column.
    'xlDoughnut': -4120, #Doughnut.
    'xlDoughnutExploded': 80, #Exploded Doughnut.
    'xlLine': 4, #Line.
    'xlLineMarkers': 65, #Line with Markers.
    'xlLineMarkersStacked': 66, #Stacked Line with Markers.
    'xlLineMarkersStacked100': 67, #100% Stacked Line with Markers.
    'xlLineStacked': 63, #Stacked Line.
    'xlLineStacked100': 64, #100% Stacked Line.
    'xlPie': 5, #Pie.
    'xlPieExploded': 69, #Exploded Pie.
    'xlPieOfPie': 68, #Pie of Pie.
    'xlPyramidBarClustered': 109, #Clustered Pyramid Bar.
    'xlPyramidBarStacked': 110, #Stacked Pyramid Bar.
    'xlPyramidBarStacked100': 111, #100% Stacked Pyramid Bar.
    'xlPyramidCol': 112, #3D Pyramid Column.
    'xlPyramidColClustered': 106, #Clustered Pyramid Column.
    'xlPyramidColStacked': 107, #Stacked Pyramid Column.
    'xlPyramidColStacked100': 108, #100% Stacked Pyramid Column.
    'xlRadar': -4151, #Radar.
    'xlRadarFilled': 82, #Filled Radar.
    'xlRadarMarkers': 81, #Radar with Data Markers.
    'xlRegionMap': 140, #Map chart.
    'xlStockHLC': 88, #High-Low-Close.
    'xlStockOHLC': 89, #Open-High-Low-Close.
    'xlStockVHLC': 90, #Volume-High-Low-Close.
    'xlStockVOHLC': 91, #Volume-Open-High-Low-Close.
    'xlSurface': 83, #3D Surface.
    'xlSurfaceTopView': 85, #Surface (Top View).
    'xlSurfaceTopViewWireframe': 86, #Surface (Top View wireframe).
    'xlSurfaceWireframe': 84, #3D Surface (wireframe).
    'xlXYScatter': -4169, #Scatter.
    'xlXYScatterLines': 74, #Scatter with Lines.
    'xlXYScatterLinesNoMarkers': 75, #Scatter with Lines and No Data Markers.
    'xlXYScatterSmooth': 72, #Scatter with Smoothed Lines.
    'xlXYScatterSmoothNoMarkers': 73, #Scatter with Smoothed Lines and No Data Markers.
    }
   
# ///////////////////////////
# General Number Formats:
# "0": Integer
# "0.00": Two decimal places
# "#,##0": Thousands separator without decimal places
# "#,##0.00": Thousands separator with two decimal places

# Currency Formats:
# "$#,##0.00": Currency with two decimal places (e.g., $1,000.00)
# "€#,##0.00": Euro currency
# "£#,##0.00": Pound currency

# Percentage Formats:
# "0%": Whole percentage (e.g., 50%)
# "0.00%": Percentage with two decimal places (e.g., 50.00%)

# Date Formats:
# "mm/dd/yyyy": Standard U.S. date format
# "dd/mm/yyyy": European date format
# "yyyy-mm-dd": ISO date format

# Time Formats:
# "h:mm AM/PM": 12-hour time format (e.g., 1:30 PM)
# "hh:mm": 24-hour time format (e.g., 13:30)

# Text Format:
# "@": Treats content as text

# Scientific Notation:
# "0.00E+00": Scientific notation (e.g., 1.23E+05)
# ///////////////////////////