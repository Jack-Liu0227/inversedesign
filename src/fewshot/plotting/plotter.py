from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import matplotlib

from .plot_format import PLOT_FORMAT
from .plot_style import PLOT_STYLE

matplotlib.use("Agg")


def plot_diagonal(
    predictions_path: str | Path,
    output_dir: str | Path,
    target_cols: List[str],
    model_name: str | None = None,
    fmt: Dict[str, object] | None = None,
    style: Dict[str, object] | None = None,
) -> List[Path]:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting") from exc

    fmt = fmt or PLOT_FORMAT
    style = style or PLOT_STYLE

    predictions_path = Path(predictions_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(predictions_path)
    output_paths: List[Path] = []

    for target in target_cols:
        true_col = f"{target}_true"
        pred_col = f"{target}_predicted"
        if true_col not in df.columns or pred_col not in df.columns:
            continue
        subset = df[[true_col, pred_col]].dropna()
        if subset.empty:
            continue

        x = subset[true_col]
        y = subset[pred_col]
        errors = y - x
        mae = float(errors.abs().mean())
        rmse = float((errors**2).mean() ** 0.5)
        ss_res = float((errors**2).sum())
        ss_tot = float(((x - x.mean()) ** 2).sum())
        r2 = None if ss_tot == 0 else 1 - (ss_res / ss_tot)
        min_val = min(x.min(), y.min())
        max_val = max(x.max(), y.max())
        padding = (max_val - min_val) * float(style["limits"]["padding_ratio"])
        lower = min_val - padding
        upper = max_val + padding

        fig, ax = plt.subplots(
            figsize=tuple(fmt["figure"]["size"]),
            dpi=int(fmt["figure"]["dpi"]),
        )
        ax.scatter(
            x,
            y,
            s=float(style["scatter"]["size"]),
            alpha=float(style["scatter"]["alpha"]),
            color=style["scatter"]["color"],
            edgecolor=style["scatter"]["edgecolor"],
            linewidth=float(style["scatter"]["linewidth"]),
            marker=style["scatter"]["marker"],
            label=model_name or "Predicted",
        )
        ax.plot(
            [lower, upper],
            [lower, upper],
            color=style["diagonal"]["color"],
            linewidth=float(style["diagonal"]["linewidth"]),
            linestyle=style["diagonal"]["linestyle"],
            label="Ideal",
        )
        ax.set_xlim(lower, upper)
        ax.set_ylim(lower, upper)
        ax.set_xlabel(f"True {target}", fontsize=int(fmt["axes"]["label_size"]))
        ax.set_ylabel(f"Predicted {target}", fontsize=int(fmt["axes"]["label_size"]))
        ax.set_title(
            f"Predicted vs True: {target}", fontsize=int(fmt["title"]["size"])
        )
        ax.tick_params(axis="both", labelsize=int(fmt["axes"]["tick_size"]))
        if fmt["grid"]["enabled"]:
            ax.grid(
                True,
                alpha=float(fmt["grid"]["alpha"]),
                linestyle=fmt["grid"]["linestyle"],
                linewidth=float(fmt["grid"]["linewidth"]),
            )
        metrics_lines = [f"MAE: {mae:.4f}", f"RMSE: {rmse:.4f}"]
        if r2 is not None:
            metrics_lines.append(f"R2: {r2:.4f}")
        metrics_text = "\n".join(metrics_lines)
        ax.text(
            0.02,
            0.98,
            metrics_text,
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=int(fmt["metrics"]["size"]),
            bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.7},
        )
        ax.legend(fontsize=int(fmt["legend"]["size"]))
        fig.tight_layout()

        output_path = output_dir / f"diagonal_{target}.png"
        fig.savefig(output_path, bbox_inches="tight")
        plt.close(fig)
        output_paths.append(output_path)

    return output_paths


def _compute_metrics(x: pd.Series, y: pd.Series) -> Dict[str, float | None]:
    errors = y - x
    mae = float(errors.abs().mean())
    rmse = float((errors**2).mean() ** 0.5)
    ss_res = float((errors**2).sum())
    ss_tot = float(((x - x.mean()) ** 2).sum())
    r2 = None if ss_tot == 0 else 1 - (ss_res / ss_tot)
    return {"mae": mae, "rmse": rmse, "r2": r2}


def _split_groups(df: pd.DataFrame, conf_col: str) -> Dict[str, pd.DataFrame]:
    if conf_col not in df.columns:
        return {"all": df}
    grouped: Dict[str, pd.DataFrame] = {}
    for name, chunk in df.groupby(df[conf_col].astype(str).str.lower()):
        grouped[name] = chunk
    if not grouped:
        grouped = {"all": df}
    return grouped


def _format_group_label(name: str) -> str:
    return name.capitalize()


def _relative_error_bins(values: pd.Series, bins: List[float], labels: List[str]) -> pd.Series:
    return pd.cut(values, bins=bins, labels=labels, right=False, include_lowest=True)


def _plot_error_distribution(
    ax,
    data_by_group: Dict[str, pd.DataFrame],
    true_col: str,
    pred_col: str,
    fmt: Dict[str, object],
    style: Dict[str, object],
) -> None:
    import numpy as np

    bins = style["error_bins"]
    labels = style["error_bin_labels"]
    groups = [
        g for g in style["confidence_order"] if g in data_by_group
    ] + [g for g in data_by_group.keys() if g not in style["confidence_order"]]

    counts = {}
    total_counts = {}
    for group in groups:
        subset = data_by_group[group][[true_col, pred_col]].dropna()
        subset = subset[subset[true_col] != 0]
        if subset.empty:
            counts[group] = np.zeros(len(labels), dtype=int)
            total_counts[group] = 0
            continue
        rel_err = (subset[pred_col] - subset[true_col]).abs() / subset[true_col].abs() * 100
        binned = _relative_error_bins(rel_err, bins, labels)
        vc = binned.value_counts().reindex(labels, fill_value=0)
        counts[group] = vc.values
        total_counts[group] = int(vc.sum())

    x = np.arange(len(labels))
    total_width = 0.8
    bar_width = total_width / max(len(groups), 1)

    for idx, group in enumerate(groups):
        color = style["confidence_colors"].get(group, "#888888")
        offset = (idx - (len(groups) - 1) / 2) * bar_width
        ax.bar(
            x + offset,
            counts[group],
            bar_width,
            color=color,
            alpha=0.6,
            edgecolor="white",
            label=f"{_format_group_label(group)} (N={total_counts[group]})",
        )

    ax.set_ylabel("Count", fontsize=int(fmt["comparison"]["label_size"]))
    ax.set_xticks(x)
    ax.set_xticklabels(
        labels,
        rotation=45,
        ha="right",
        fontsize=int(fmt["comparison"]["tick_size"]),
    )
    ax.set_xlabel("Relative Error (%)", fontsize=int(fmt["comparison"]["label_size"]))
    ax.tick_params(axis="y", labelsize=int(fmt["comparison"]["tick_size"]))
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)

    ax2 = ax.twinx()
    for idx, group in enumerate(groups):
        vals = counts[group]
        total = total_counts[group]
        percents = (vals / total * 100) if total > 0 else np.zeros(len(vals))
        color = style["confidence_colors"].get(group, "#888888")
        marker = style["confidence_markers"].get(group, "o")
        offset = (idx - (len(groups) - 1) / 2) * bar_width
        ax2.plot(
            x + offset,
            percents,
            color=color,
            marker=marker,
            linewidth=2,
            label=f"{_format_group_label(group)} (%)",
        )
        for px, py in zip(x + offset, percents):
            if py > 1:
                ax2.text(
                    px,
                    py + 1.2,
                    f"{py:.0f}",
                    color=color,
                    ha="center",
                    va="bottom",
                    fontsize=int(fmt["comparison"]["metrics_size"]),
                )
    ax2.set_ylabel("Percentage (%)", fontsize=int(fmt["comparison"]["label_size"]))
    ax2.tick_params(axis="y", labelsize=int(fmt["comparison"]["tick_size"]))

    handles, labels_text = ax.get_legend_handles_labels()
    if handles:
        ax.legend(
            handles,
            labels_text,
            loc="upper right",
            fontsize=int(fmt["comparison"]["legend_size"]),
            framealpha=0.85,
        )


def plot_comparison(
    predictions_path: str | Path,
    output_dir: str | Path,
    target_cols: List[str],
    model_name: str | None = None,
    fmt: Dict[str, object] | None = None,
    style: Dict[str, object] | None = None,
) -> Path | None:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting") from exc

    fmt = fmt or PLOT_FORMAT
    style = style or PLOT_STYLE

    predictions_path = Path(predictions_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(predictions_path)
    if df.empty:
        return None

    n_targets = len(target_cols)
    if n_targets == 0:
        return None

    width = float(fmt["comparison"]["width_per_target"]) * n_targets
    height = float(fmt["comparison"]["height"])
    fig, axes = plt.subplots(
        2,
        n_targets,
        figsize=(width, height),
        dpi=int(fmt["figure"]["dpi"]),
    )
    if n_targets == 1:
        axes = axes.reshape(2, 1)

    short_model = model_name.split("/")[-1] if model_name else "LLM"
    for idx, target in enumerate(target_cols):
        true_col = f"{target}_true"
        pred_col = f"{target}_predicted"
        if true_col not in df.columns or pred_col not in df.columns:
            continue
        cols = [true_col, pred_col]
        if "confidence" in df.columns:
            cols.append("confidence")
        subset = df[cols].dropna()
        if subset.empty:
            continue

        grouped = _split_groups(subset, "confidence")
        ax = axes[0, idx]
        for group_name, group_df in grouped.items():
            color = style["confidence_colors"].get(group_name, "#888888")
            marker = style["confidence_markers"].get(group_name, "o")
            ax.scatter(
                group_df[true_col],
                group_df[pred_col],
                s=float(style["scatter"]["size"]),
                alpha=float(style["scatter"]["alpha"]),
                color=color,
                edgecolor=style["scatter"]["edgecolor"],
                linewidth=float(style["scatter"]["linewidth"]),
                marker=marker,
                label=_format_group_label(group_name),
            )

        x = subset[true_col]
        y = subset[pred_col]
        metrics = _compute_metrics(x, y)
        min_val = min(x.min(), y.min())
        max_val = max(x.max(), y.max())
        padding = (max_val - min_val) * float(style["limits"]["padding_ratio"])
        lower = min_val - padding
        upper = max_val + padding
        ax.plot(
            [lower, upper],
            [lower, upper],
            color=style["diagonal"]["color"],
            linewidth=float(style["diagonal"]["linewidth"]),
            linestyle=style["diagonal"]["linestyle"],
        )
        ax.set_xlim(lower, upper)
        ax.set_ylim(lower, upper)
        ax.set_xlabel(f"Experimental {target}", fontsize=int(fmt["comparison"]["label_size"]))
        ax.set_ylabel(f"Predicted {target}", fontsize=int(fmt["comparison"]["label_size"]))
        ax.set_title(
            f"{target} from {short_model}",
            fontsize=int(fmt["comparison"]["subplot_title_size"]),
            fontweight="bold",
            pad=int(fmt["comparison"]["subplot_title_pad"]),
        )
        ax.tick_params(axis="both", labelsize=int(fmt["comparison"]["tick_size"]))
        ax.grid(
            True,
            alpha=float(fmt["grid"]["alpha"]),
            linestyle=fmt["grid"]["linestyle"],
            linewidth=float(fmt["grid"]["linewidth"]),
        )
        ax.text(
            0.5,
            1.01,
            f"MAE = {metrics['mae']:.2f}, R2 = {metrics['r2']:.3f}"
            if metrics["r2"] is not None
            else f"MAE = {metrics['mae']:.2f}",
            transform=ax.transAxes,
            ha="center",
            va="bottom",
            fontsize=int(fmt["comparison"]["metrics_size"]),
            style="italic",
        )

        for group_name, group_df in grouped.items():
            group_metrics = _compute_metrics(group_df[true_col], group_df[pred_col])
            pos = style["group_box_positions"].get(group_name, (0.05, 0.85))
            text = (
                f"{_format_group_label(group_name)}\n"
                f"MAE = {group_metrics['mae']:.2f}\n"
                f"R2 = {group_metrics['r2']:.3f}"
                if group_metrics["r2"] is not None
                else f"{_format_group_label(group_name)}\nMAE = {group_metrics['mae']:.2f}"
            )
            ax.text(
                pos[0],
                pos[1],
                text,
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=int(fmt["comparison"]["group_box_size"]),
                bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.7},
            )

        if idx == 0:
            ax.legend(
                fontsize=int(fmt["comparison"]["legend_size"]),
                loc=fmt["comparison"]["scatter_legend_loc"],
                bbox_to_anchor=tuple(fmt["comparison"]["scatter_legend_bbox"]),
                framealpha=0.85,
            )

        ax_dist = axes[1, idx]
        _plot_error_distribution(
            ax_dist,
            grouped,
            true_col,
            pred_col,
            fmt,
            style,
        )

    fig.tight_layout()
    output_path = output_dir / "comparison_plot.png"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    return output_path
