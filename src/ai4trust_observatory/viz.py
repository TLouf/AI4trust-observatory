
import polars as pl


def plot_from_d(data: pl.DataFrame, plot_d: dict, ax, **kwargs):
    if plot_d["chart_type"] == "bar":
        ax.bar(x=plot_d["x_data"], height=plot_d["y_data"], data=data, **kwargs)
    elif plot_d["chart_type"] == "line":
        ax.plot(x=plot_d["x_data"], y=plot_d["y_data"], data=data, **kwargs)
    elif plot_d["chart_type"] == "scatter":
        ax.scatter(x=plot_d["x_data"], y=plot_d["y_data"], data=data, **kwargs)
    elif plot_d["chart_type"] == "diff":
        plot_data = data.with_columns(
            pl.col(plot_d["x_data"]).alias("x_data"),
            pl.col(plot_d["pos_y_data"]).alias("pos_y_data"),
            -pl.col(plot_d["neg_y_data"]).alias("neg_y_data"),
        ).with_columns(diff=pl.col("pos_y_data") + pl.col("neg_y_data"))
        ax.bar(x="x_data", height="pos_y_data", data=plot_data, fc="seagreen", **kwargs)
        ax.bar(x="x_data", height="neg_y_data", data=plot_data, fc="indianred", **kwargs)
        ax.plot("x_data", "diff", data=plot_data, c="k", ls='--', **kwargs)
        yticks = ax.get_yticks()
        ax.set_yticks(yticks, [int(abs(tick)) for tick in yticks])
    ax.set_xlabel(plot_d.get("xlabel"))
    ax.set_ylabel(plot_d.get("ylabel"))
    ax.set_title(plot_d.get("title"))
    ax.set_xscale(plot_d.get("yscale", "linear"))
    ax.set_yscale(plot_d.get("xscale", "linear"))
    return ax