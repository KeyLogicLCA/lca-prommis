import matplotlib.pyplot as plt
import numpy as np
from src.generate_contribution_tree import generate_contribution_tree
# Main Function
def plot_results(result):
    # generate contribution tree with all nodes and only 1 level
    df = generate_contribution_tree(result, 1, -1, False)
    # get impact categories
    impact_categories = df["Impact_Category"].drop_duplicates()
    # determine number of impact cateogories to get number of figures
    n = len(impact_categories)
    # determine number of columns and rows based on the number of figures we need to plot
    cols = 3
    rows = int(np.ceil(n / cols))
    # Scale height a bit with number of rows
    fig, axs = plt.subplots(rows, cols, figsize=(5*cols, 3.5*rows))
    axs = np.array(axs).reshape(-1)  # flatten safely even if rows/cols == 1
    # initialize shared handles and labels - this is added to have one legend for all figures
    shared_handles = shared_labels = None
    # plot each impact category
    for i, impact_category in enumerate(impact_categories):
        df_ic = df[df["Impact_Category"] == impact_category]
        # Capture legend only from the FIRST subplot
        capture = (shared_handles is None)
        ax, handles, labels = plot_results_contribution_tree(
            df_ic, ax=axs[i], capture_legend=capture
        )
        ax.set_title(impact_category)
        if capture and handles:
            shared_handles, shared_labels = handles, labels
    # Remove any unused axes
    for j in range(n, len(axs)):
        fig.delaxes(axs[j])
    # Ensure there are no legends remaining on the figures
    for ax in fig.axes:
        leg = ax.get_legend()
        if leg:
            leg.remove()
    # Add one legend for all figures
    if shared_handles:
        fig.legend(
            shared_handles, shared_labels,
            title="Legend",
            loc="upper center",
            bbox_to_anchor=(0.5, 1.02),
            ncol=min(3, len(shared_labels))
        )
        # Give the top legend room
        fig.tight_layout(rect=[0, 0, 1, 0.97])
    else:
        fig.tight_layout()
    plt.show()

# Helper Function
###########################################################
def plot_results_contribution_tree(df, ax=None, capture_legend=False):
    """
    Plot on the provided ax. If capture_legend=True, briefly create a legend
    to read handles/labels, then remove it and return them.
    """
    # Split first row (direct contribution) from the rest
    direct = df.iloc[0]["Direct_Contribution"]
    others = df.iloc[1:].set_index("Provider")["Result"]
    # Combine into one DataFrame for stacked plotting
    # first row: parameters and second row: values
    plot_df = others.to_frame().T
    plot_df["Direct Contribution"] = direct
    if ax is None:
        fig, ax = plt.subplots()
    # Only the first call (capture_legend=True) will temporarily create a legend
    ax = plot_df.plot(kind="bar", stacked=True, ax=ax, legend=capture_legend)
    ax.set_ylabel("Value")
    # TODO: modify generate_contirbution_tree to include unit in the output df
    # TODO: add unit to the y-axis label
    handles = labels = None
    if capture_legend:
        handles, labels = ax.get_legend_handles_labels()
        # remove per-axis legend so we can add a single figure-level legend
        leg = ax.get_legend()
        if leg:
            leg.remove()
    return ax, handles, labels

