PLOT_STYLE = {
    "scatter": {
        "marker": "o",
        "size": 30,
        "alpha": 0.75,
        "color": "#1f77b4",
        "edgecolor": "#ffffff",
        "linewidth": 0.6,
    },
    "diagonal": {"color": "#222222", "linewidth": 1.5, "linestyle": "--"},
    "limits": {"padding_ratio": 0.05},
    "confidence_order": ["high", "medium", "low"],
    "confidence_colors": {
        "high": "#1f77b4",
        "medium": "#ff7f0e",
        "low": "#2ca02c",
        "all": "#333333",
    },
    "confidence_markers": {"high": "o", "medium": "s", "low": "^"},
    "group_box_positions": {
        "high": (0.05, 0.92),
        "medium": (0.75, 0.18),
        "low": (0.05, 0.18),
    },
    "error_bins": [0, 5, 10, 15, 20, 30, 50, 10_000],
    "error_bin_labels": ["0-5%", "5-10%", "10-15%", "15-20%", "20-30%", "30-50%", ">50%"],
}
