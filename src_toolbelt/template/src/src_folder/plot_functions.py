from typing import Literal

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from $PROJECT_NAME$ import get_docs_path


def colorful_barplot(
    dataset_plot: pd.DataFrame,
    col_categ: str,
    col_target: str,
    col_line: str = None,
    line_kwargs: dict = {},
    legend_loc: str = "best",
    y2axis_title: str = None,
    plot_title: str = None,
    xaxis_title: str = None,
    yaxis_title: str = None,
):

    categs = np.sort(dataset_plot.index)
    colors = list(sns.color_palette("husl", len(categs)).as_hex())

    fig, ax1 = plt.subplots(figsize=(12, 5))
    plt.bar(categs, dataset_plot[col_target], color=colors)
    for i, categ in enumerate(categs):
        ax1.text(
            i,
            dataset_plot[col_target].iloc[i],
            "{:0.0f}".format(dataset_plot[col_target].iloc[i]),
            fontsize=10,
            bbox=dict(facecolor=colors[i], alpha=1.0),
            ha="center",
            va="center",
            fontweight="normal",
            color="white",
        )

    if col_line:
        ax2 = ax1.twinx()
        ax2.plot(categs, dataset_plot[col_line], **line_kwargs)
        if legend_loc:
            plt.legend(loc=legend_loc, shadow=True)
        ax2.set_ylabel(y2axis_title)

    ax1.set_xticks(categs)
    ax1.set_xticklabels(dataset_plot[col_categ], rotation=45, ha="right")
    ax1.set_xlabel(xaxis_title)
    ax1.set_ylabel(yaxis_title)
    ax1.set_title(plot_title)

    plt.tight_layout()
    plt.show()


def colorful_boxplot(
    dataset_plot: pd.DataFrame,
    col_categ: str,
    col_target: str,
    boxplot_whis: tuple = None,
    remove_outliers: bool = True,
    ourliers_pct: float = 0.05,
    show_quantile_text: bool = True,
    quantile_text_loc: float = 0.75,
    quantile_text_sig: int = 1,
    show_agg_text: bool = True,
    agg_text_funct: Literal["mean", "median"] = "median",
    agg_text_sig: int = 1,
    plot_title: str = None,
    xaxis_title: str = None,
    xaxis_rotation: int = 0,
    xaxis_inverse=False,
    yaxis_title: str = None,
    show_mean_line: bool = True,
    show_pct_diff_plot: bool = True,
    pct_diff_agg_funct: Literal["mean", "median"] = "median",
    pct_diff_text_sig: int = 1,
    pct_diff_type: Literal["lowest", "highest"] = "lowest",
    save_file: str = None,
):

    categs = np.sort(dataset_plot[col_categ].unique())
    colors = list(sns.color_palette("husl", len(categs)).as_hex())

    if remove_outliers:
        dataset_plot = dataset_plot.loc[
            dataset_plot[col_target].between(
                dataset_plot[col_target].quantile(ourliers_pct),
                dataset_plot[col_target].quantile(1 - ourliers_pct),
            )
        ]

    fig, ax = plt.subplots(figsize=(12, 5))

    means = []
    for i, categ in enumerate(categs):
        aux = dataset_plot.loc[dataset_plot[col_categ] == categ, col_target]

        means.append(np.mean(aux))
        ax.boxplot(
            aux,
            positions=[i],
            widths=0.5,
            whis=boxplot_whis,
            patch_artist=True,
            boxprops=dict(facecolor=colors[i], color=colors[i]),
            medianprops=dict(color="black"),
            whiskerprops=dict(color=colors[i]),
            capprops=dict(color=colors[i]),
            showfliers=False,
            labels=[categ],
        )

        if show_quantile_text:
            ax.text(
                i,
                aux.quantile(quantile_text_loc),
                "{:.{sig}f}".format(
                    aux.quantile(quantile_text_loc), sig=quantile_text_sig
                ),
                fontsize=10,
                bbox=dict(facecolor=colors[i], alpha=1.0),
                ha="center",
                va="center",
                fontweight="normal",
                color="white",
            )

        if show_agg_text:
            if agg_text_funct == "mean":
                ax.text(
                    i,
                    np.mean(aux),
                    "{:.{sig}f}".format(np.mean(aux), sig=agg_text_sig),
                    fontsize=10,
                    bbox=dict(facecolor=colors[i], alpha=1.0),
                    ha="center",
                    va="center",
                    fontweight="normal",
                    color="white",
                )
            elif agg_text_funct == "median":
                ax.text(
                    i,
                    np.median(aux),
                    "{:.{sig}f}".format(np.median(aux), sig=agg_text_sig),
                    fontsize=10,
                    bbox=dict(facecolor=colors[i], alpha=1.0),
                    ha="center",
                    va="center",
                    fontweight="normal",
                    color="white",
                )

    if show_mean_line:
        ax.plot(
            np.arange(len(categs)),
            means,
            color="red",
            linestyle="--",
            linewidth=1.25,
        )

    ax.set_title(plot_title, fontsize=11)
    ax.set_xlabel(xaxis_title, fontsize=10)
    ax.set_ylabel(yaxis_title, fontsize=10)

    # ax.legend(fontsize=10, shadow=True)
    plt.xticks(rotation=xaxis_rotation, ha="center")
    ax.grid(linewidth=0.05)
    if xaxis_inverse:
        plt.gca().invert_xaxis()
    plt.tight_layout()
    if save_file is not None:
        plt.savefig(get_docs_path(save_file + ".png"), dpi=300)
        print("Saved file: " + get_docs_path(save_file + ".png"))
    plt.show()

    if show_pct_diff_plot:
        dataset_grouped = (
            dataset_plot.groupby(col_categ)
            .agg({col_target: pct_diff_agg_funct})
            .reset_index()
        )

        if pct_diff_type == "lowest":
            dataset_grouped["diff_pct"] = 100 * (
                (dataset_grouped[col_target] - (dataset_grouped[col_target]).min())
                / (dataset_grouped[col_target]).min()
            )

        elif pct_diff_type == "highest":
            dataset_grouped["diff_pct"] = 100 * (
                (dataset_grouped[col_target] - (dataset_grouped[col_target]).max())
                / (dataset_grouped[col_target]).max()
            )

        fig, ax = plt.subplots(figsize=(10, 5))
        plt.bar(categs, dataset_grouped["diff_pct"], color=colors)
        for i, categ in enumerate(categs):
            ax.text(
                i,
                dataset_grouped["diff_pct"].iloc[i],
                "{:.{sig}f}%".format(
                    dataset_grouped["diff_pct"].iloc[i], sig=pct_diff_text_sig
                ),
                fontsize=10,
                bbox=dict(facecolor=colors[i], alpha=1.0),
                ha="center",
                va="center",
                fontweight="normal",
                color="white",
            )
        plt.plot([-0.5, len(categs) - 0.5], [0, 0], color="black", linewidth=0.5)
        if pct_diff_type == "lowest":
            plt.ylim(
                0 - np.max(dataset_grouped["diff_pct"]) * 0.05,
                np.max(dataset_grouped["diff_pct"]) * 1.05,
            )
        elif pct_diff_type == "highest":
            plt.ylim(
                np.min(dataset_grouped["diff_pct"]) * 1.05,
                0 - np.min(dataset_grouped["diff_pct"]) * 0.05,
            )
        plt.title(
            "(% diff from {} {} value)".format(pct_diff_type, pct_diff_agg_funct),
            fontsize=11,
        )
        if xaxis_inverse:
            plt.gca().invert_xaxis()
        plt.xticks(rotation=xaxis_rotation, ha="center")
        plt.grid(linewidth=0.05)
        plt.tight_layout()
        if save_file is not None:
            plt.savefig(get_docs_path(save_file + "_diffplot.png"), dpi=300)
        plt.show()
