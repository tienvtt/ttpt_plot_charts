combinechart(
    "Y2022",
    "Y2024",
    timeframe="yearly",
    title="Biểu đồ Giá cổ phiếu VCB vs TCB từng năm",
).add_stock("VCB").add_stock("TCB").plot_bar(save_path="./combine_img")