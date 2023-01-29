import bokeh as bkh
import bokeh.plotting as bkhp


def create_2d_figure(title: str, label_abscissa: str = 'x', label_ordinate: str = 'y') -> bkhp.figure:
    return bkhp.figure(title=title, x_axis_label=label_abscissa, y_axis_label=label_ordinate, x_axis_type="datetime", y_axis_type=None)


def plot_2d_line(figure: bkhp.figure, x, y, label, width: int = 2):
    return figure.line(x, y, legend_label=label, line_width=width)


def plot_2d_lines(figure: bkhp.figure, x, y, label, width: int = 2):
    figure.multi_line()
    return figure.line(x, y, legend_label=label, line_width=width)


def show_2d_figure(figure: bkhp.figure):
    bkhp.show(figure)


# figure = dioplot.create_2d_figure(
#     title='Акции Tesla',
#     label_abscissa='Время', label_ordinate='Стоимость, $'
# )
# dioplot.plot_2d_line(
#     figure, x=dataset_target.index, y=dataset_target['open'], label='open'
# )
# dioplot.show_2d_figure(figure)